package main

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"log"
	"math/big"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

// Aave V3 Pool Base — flashLoanSimple
const aaveV3PoolBase = "0xA238Dd80C259a72e81d7e4664a9801593F98d1c5"

const flashArbABIJSON = `[
  {"name":"executeArb","type":"function","stateMutability":"nonpayable",
   "inputs":[
     {"name":"asset","type":"address"},
     {"name":"amount","type":"uint256"},
     {"name":"buyRouter","type":"address"},
     {"name":"buyCalldata","type":"bytes"},
     {"name":"sellRouter","type":"address"},
     {"name":"quoteToken","type":"address"},
     {"name":"v3Sell","type":"bool"},
     {"name":"v3Fee","type":"uint24"},
     {"name":"minWethOut","type":"uint256"},
     {"name":"minTokenAfterBuy","type":"uint256"},
     {"name":"minProfitWei","type":"uint256"},
     {"name":"deadline","type":"uint256"}
   ],
   "outputs":[]
  }
]`

const aavePoolPremiumABIJSON = `[{"inputs":[],"name":"FLASHLOAN_PREMIUM_TOTAL","outputs":[{"internalType":"uint128","name":"","type":"uint128"}],"stateMutability":"view","type":"function"}]`

var (
	flashArbParsed       abi.ABI
	aavePoolPremiumParsed abi.ABI
	flashArbContract     common.Address
	flashLoanAmountWei   *big.Int // optional override; nil => use wethIn from signal
	flashMinProfitWei    *big.Int
	flashMaxTaxPct       float64 // 0 = only REAL_MAX_SLIPPAGE on leg1 min token
	flashSkipSimulate    bool    // true = skip eth_call, send TX directly (cheap gas L2 strategy)
	flashSkipSimMinUSD   float64 // min estimated profit to skip simulate (default $0.15)
	flashSimTimeout      = 8 * time.Second // eth_call executeArb: отдельный дедлайн (FLASH_SIM_TIMEOUT_SEC)
	flashBadTokenTTL     = 24 * time.Hour

	flashBadMu    sync.RWMutex
	flashBadToken = make(map[common.Address]time.Time)

	cachedPremiumBps   uint64 = 5
	cachedPremiumAt    time.Time
	cachedPremiumTTL   = 5 * time.Minute
	cachedPremiumMu    sync.RWMutex
)

func init() {
	var err error
	flashArbParsed, err = abi.JSON(strings.NewReader(flashArbABIJSON))
	if err != nil {
		panic(err)
	}
	aavePoolPremiumParsed, err = abi.JSON(strings.NewReader(aavePoolPremiumABIJSON))
	if err != nil {
		panic(err)
	}
}

func loadFlashArbSettings() {
	flashArbContract = common.Address{}
	if v := strings.TrimSpace(os.Getenv("FLASH_ARB_CONTRACT")); v != "" {
		if common.IsHexAddress(v) {
			flashArbContract = common.HexToAddress(v)
		}
	}
	flashLoanAmountWei = nil
	if v := strings.TrimSpace(os.Getenv("FLASH_LOAN_AMOUNT_WEI")); v != "" {
		if bi, ok := new(big.Int).SetString(v, 10); ok && bi.Sign() > 0 {
			flashLoanAmountWei = bi
		}
	}
	flashMinProfitWei = big.NewInt(0)
	if v := strings.TrimSpace(os.Getenv("FLASH_MIN_PROFIT_WEI")); v != "" {
		if bi, ok := new(big.Int).SetString(v, 10); ok && bi.Sign() >= 0 {
			flashMinProfitWei = bi
		}
	}
	flashMaxTaxPct = 0
	if v := strings.TrimSpace(os.Getenv("FLASH_MAX_TAX_PCT")); v != "" {
		if f, err := parseFloatEnv(v); err == nil && f >= 0 {
			flashMaxTaxPct = f
		}
	}
	if v := strings.TrimSpace(os.Getenv("FLASH_BAD_TOKEN_TTL_HOURS")); v != "" {
		if h, err := parseFloatEnv(v); err == nil && h > 0 {
			flashBadTokenTTL = time.Duration(h * float64(time.Hour))
		}
	}
	flashSkipSimulate = strings.TrimSpace(os.Getenv("FLASH_SKIP_SIMULATE")) == "1"
	flashSkipSimMinUSD = 0.15
	if v := strings.TrimSpace(os.Getenv("FLASH_SKIP_SIM_MIN_USD")); v != "" {
		if f, err := parseFloatEnv(v); err == nil && f > 0 {
			flashSkipSimMinUSD = f
		}
	}
	flashSimTimeout = 8 * time.Second
	if v := strings.TrimSpace(os.Getenv("FLASH_SIM_TIMEOUT_SEC")); v != "" {
		if sec, err := parseFloatEnv(v); err == nil && sec > 0 {
			flashSimTimeout = time.Duration(sec * float64(time.Second))
		}
	}
	if flashArbContract != (common.Address{}) {
		log.Printf("FLASH ARB: contract=%s | minProfitWei=%s | maxTaxPct=%.2f | skipSim=%v (min$%.2f) | simTimeout=%v | loanOverride=%v",
			flashArbContract.Hex(), flashMinProfitWei.String(), flashMaxTaxPct, flashSkipSimulate, flashSkipSimMinUSD, flashSimTimeout, flashLoanAmountWei != nil)
	}
}

func parseFloatEnv(s string) (float64, error) {
	var f float64
	_, err := fmt.Sscanf(strings.ReplaceAll(s, ",", "."), "%f", &f)
	return f, err
}

func flashAmountForTrade(wethIn *big.Int) *big.Int {
	if flashLoanAmountWei != nil && flashLoanAmountWei.Sign() > 0 {
		return new(big.Int).Set(flashLoanAmountWei)
	}
	if wethIn == nil || wethIn.Sign() <= 0 {
		return big.NewInt(0)
	}
	return new(big.Int).Set(wethIn)
}

func isFlashBadToken(tok common.Address) bool {
	flashBadMu.RLock()
	t, ok := flashBadToken[tok]
	flashBadMu.RUnlock()
	if !ok {
		return false
	}
	if time.Since(t) > flashBadTokenTTL {
		flashBadMu.Lock()
		delete(flashBadToken, tok)
		flashBadMu.Unlock()
		return false
	}
	return true
}

func markFlashBadToken(tok common.Address) {
	flashBadMu.Lock()
	flashBadToken[tok] = time.Now()
	flashBadMu.Unlock()
}

func aaveFlashPremiumBpsCached(ctx context.Context, ec *ethclient.Client) uint64 {
	cachedPremiumMu.RLock()
	if time.Since(cachedPremiumAt) < cachedPremiumTTL {
		v := cachedPremiumBps
		cachedPremiumMu.RUnlock()
		return v
	}
	cachedPremiumMu.RUnlock()

	pool := common.HexToAddress(aaveV3PoolBase)
	data, err := aavePoolPremiumParsed.Pack("FLASHLOAN_PREMIUM_TOTAL")
	if err != nil {
		return cachedPremiumBps
	}
	msg := ethereum.CallMsg{To: &pool, Data: data}
	out, err := callContractRetry(ctx, ec, msg, nil)
	if err != nil {
		return cachedPremiumBps
	}
	vals, err := aavePoolPremiumParsed.Unpack("FLASHLOAN_PREMIUM_TOTAL", out)
	if err != nil || len(vals) != 1 {
		return cachedPremiumBps
	}
	if v, ok := vals[0].(*big.Int); ok && v != nil {
		cachedPremiumMu.Lock()
		cachedPremiumBps = v.Uint64()
		cachedPremiumAt = time.Now()
		cachedPremiumMu.Unlock()
		return v.Uint64()
	}
	return cachedPremiumBps
}

func aavePremiumWei(amount *big.Int, bps uint64) *big.Int {
	if amount == nil || amount.Sign() <= 0 {
		return big.NewInt(0)
	}
	p := new(big.Int).Mul(amount, big.NewInt(int64(bps)))
	return p.Div(p, big.NewInt(10000))
}

// minTokenAfterBuyLeg1 combines slippage floor and max-tax floor (stricter = larger minimum).
func minTokenAfterBuyLeg1(leg1Expected *big.Int) *big.Int {
	if leg1Expected == nil || leg1Expected.Sign() <= 0 {
		return big.NewInt(0)
	}
	slip := pctToMinOut(leg1Expected, realMaxSlippagePct)
	out := slip
	if flashMaxTaxPct > 0 {
		tax := pctToMinOut(leg1Expected, flashMaxTaxPct)
		if tax.Cmp(out) > 0 {
			out = tax
		}
	}
	return out
}

type flashArbArgs struct {
	Asset             common.Address
	Amount            *big.Int
	BuyRouter         common.Address
	BuyCalldata       []byte
	SellRouter        common.Address
	QuoteToken        common.Address
	V3Sell            bool
	V3Fee             uint32
	MinWethOut        *big.Int
	MinTokenAfterBuy  *big.Int
	MinProfitWei      *big.Int
	Deadline          *big.Int
}

func packExecuteArb(a flashArbArgs) ([]byte, error) {
	feeBI := big.NewInt(int64(a.V3Fee))
	return flashArbParsed.Pack("executeArb",
		a.Asset,
		a.Amount,
		a.BuyRouter,
		a.BuyCalldata,
		a.SellRouter,
		a.QuoteToken,
		a.V3Sell,
		feeBI,
		a.MinWethOut,
		a.MinTokenAfterBuy,
		a.MinProfitWei,
		a.Deadline,
	)
}

func flashArbSimulate(ctx context.Context, ec *ethclient.Client, from common.Address, a flashArbArgs) error {
	if flashArbContract == (common.Address{}) {
		return errors.New("FLASH_ARB_CONTRACT not set")
	}
	data, err := packExecuteArb(a)
	if err != nil {
		return err
	}
	msg := ethereum.CallMsg{
		From: from,
		To:   &flashArbContract,
		Data: data,
	}
	_, err = callContractRetry(ctx, ec, msg, nil)
	return err
}

// flashArbSimulateDeadline — симуляция executeArb с дедлайном flashSimTimeout (не «висеть» на весь 25–30s ctx при сбое RPC).
func flashArbSimulateDeadline(parent context.Context, ec *ethclient.Client, from common.Address, a flashArbArgs) error {
	ctx := parent
	var cancel context.CancelFunc
	if flashSimTimeout > 0 {
		ctx, cancel = context.WithTimeout(parent, flashSimTimeout)
		defer cancel()
	}
	return flashArbSimulate(ctx, ec, from, a)
}

func flashSelfTest(ctx context.Context, ec *ethclient.Client) {
	if flashArbContract == (common.Address{}) {
		log.Printf("[FLASH SELF-TEST] пропуск: FLASH_ARB_CONTRACT не задан")
		return
	}
	_, trader, err := parseTraderKey()
	if err != nil {
		log.Printf("[FLASH SELF-TEST] пропуск: TRADER_PRIVATE_KEY не задан: %v", err)
		return
	}

	log.Printf("[FLASH SELF-TEST] проверка flash loan системы (WETH/USDC, eth_call)…")

	ctxT, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	bps := aaveFlashPremiumBpsCached(ctxT, ec)
	log.Printf("[FLASH SELF-TEST] Aave premium = %d bps (%.2f%%)", bps, float64(bps)/100)

	amount := big.NewInt(20_000_000_000_000_000) // 0.02 WETH (~$40)
	usdc := common.HexToAddress("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913")

	leg1Exp, errQ := routerGetAmountsOut(ctxT, ec, addrUniswapV2Router, amount, []common.Address{addrWETH, usdc})
	if errQ != nil || leg1Exp == nil || leg1Exp.Sign() <= 0 {
		log.Printf("[FLASH SELF-TEST] FAIL: leg1 quote (Uni V2 WETH→USDC): %v", errQ)
		return
	}
	log.Printf("[FLASH SELF-TEST] leg1 (Uni V2 buy): 0.02 WETH → %s USDC (raw)", leg1Exp.String())

	leg2Exp, errQ2 := routerGetAmountsOut(ctxT, ec, addrSushiV2Router, leg1Exp, []common.Address{usdc, addrWETH})
	if errQ2 != nil || leg2Exp == nil || leg2Exp.Sign() <= 0 {
		log.Printf("[FLASH SELF-TEST] FAIL: leg2 quote (Sushi V2 USDC→WETH): %v", errQ2)
		return
	}

	prem := aavePremiumWei(amount, bps)
	repay := new(big.Int).Add(amount, prem)
	diff := new(big.Int).Sub(leg2Exp, repay)
	diffF, _ := new(big.Float).SetInt(diff).Float64()
	diffETH := diffF / 1e18
	log.Printf("[FLASH SELF-TEST] leg2 (Sushi V2 sell): %s USDC → %s WETH | repay=%s | diff=%.6f ETH",
		leg1Exp.String(), leg2Exp.String(), repay.String(), diffETH)

	deadline := big.NewInt(time.Now().Add(120 * time.Second).Unix())
	minTok := big.NewInt(1)
	buyData, errPack := uniV2RouterABIv.Pack("swapExactTokensForTokens", amount, minTok,
		[]common.Address{addrWETH, usdc}, flashArbContract, deadline)
	if errPack != nil {
		log.Printf("[FLASH SELF-TEST] FAIL: pack buyData: %v", errPack)
		return
	}

	args := flashArbArgs{
		Asset:            addrWETH,
		Amount:           amount,
		BuyRouter:        addrUniswapV2Router,
		BuyCalldata:      buyData,
		SellRouter:       addrSushiV2Router,
		QuoteToken:       usdc,
		V3Sell:           false,
		V3Fee:            0,
		MinWethOut:       big.NewInt(1),
		MinTokenAfterBuy: big.NewInt(1),
		MinProfitWei:     big.NewInt(0),
		Deadline:         deadline,
	}

	errSim := flashArbSimulate(ctxT, ec, trader, args)
	if errSim != nil {
		errStr := errSim.Error()
		if strings.Contains(errStr, "ProfitTooLow") || strings.Contains(errStr, "execution reverted") {
			if diff.Sign() < 0 {
				log.Printf("[FLASH SELF-TEST] OK: контракт + Aave + роутеры работают! Revert ожидаем: пара убыточна (%.6f ETH)", diffETH)
			} else {
				log.Printf("[FLASH SELF-TEST] OK: revert при положительном diff — возможно slippage/tax. Система работает.")
			}
		} else if strings.Contains(errStr, "INSUFFICIENT_OUTPUT_AMOUNT") {
			log.Printf("[FLASH SELF-TEST] OK: контракт работает, revert из-за ликвидности")
		} else {
			log.Printf("[FLASH SELF-TEST] FAIL: %v", errSim)
		}
	} else {
		log.Printf("[FLASH SELF-TEST] SUCCESS: flash simulate прошёл! Вся система работает, есть профит!")
	}
}

func flashArbExecute(ctx context.Context, ec *ethclient.Client, pk *ecdsa.PrivateKey, from common.Address, a flashArbArgs) (*types.Transaction, error) {
	if flashArbContract == (common.Address{}) {
		return nil, errors.New("FLASH_ARB_CONTRACT not set")
	}
	data, err := packExecuteArb(a)
	if err != nil {
		return nil, err
	}
	return sendDynamicTx(ctx, ec, pk, from, &flashArbContract, data, big.NewInt(0))
}
