package main

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"log"
	"math"
	"math/big"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
)

// Base mainnet routers (UniswapV2Router02 ABI-compatible).
var (
	addrUniswapV2Router = common.HexToAddress("0x4752ba5dbc23f44d87826276bf6fd6b1c372ad24")
	addrSushiV2Router   = common.HexToAddress("0x6BDED42c6DA8FBf0d2bA55B2fa120C5e0c8D7891")
	addrUniswapV3Router = common.HexToAddress("0x2626664c2603336E57B271c5C0b26F421741e481") // SwapRouter02
)

var (
	realTradingEnabled bool = false // SIMULATION=0 => true
	realMaxSlippagePct       = 0.3   // %
	realLossLimitUSD         = 10.0  // hard stop
	realTradeLogPath         = "real_trading.log"

	realStartBalanceUSD float64
	realStartBalanceSet bool

	realMu sync.Mutex

	activeTradeWg sync.WaitGroup
)

const erc20ABIJSON = `[{"constant":true,"inputs":[{"name":"owner","type":"address"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"type":"function"},{"constant":true,"inputs":[{"name":"owner","type":"address"},{"name":"spender","type":"address"}],"name":"allowance","outputs":[{"name":"","type":"uint256"}],"type":"function"},{"constant":false,"inputs":[{"name":"spender","type":"address"},{"name":"amount","type":"uint256"}],"name":"approve","outputs":[{"name":"","type":"bool"}],"type":"function"}]`

const uniV2RouterABIJSON = `[
  {"name":"getAmountsOut","type":"function","stateMutability":"view",
   "inputs":[
     {"name":"amountIn","type":"uint256"},
     {"name":"path","type":"address[]"}
   ],
   "outputs":[{"name":"amounts","type":"uint256[]"}]
  },
  {"name":"swapExactTokensForTokens","type":"function","stateMutability":"nonpayable",
   "inputs":[
     {"name":"amountIn","type":"uint256"},
     {"name":"amountOutMin","type":"uint256"},
     {"name":"path","type":"address[]"},
     {"name":"to","type":"address"},
     {"name":"deadline","type":"uint256"}
   ],
   "outputs":[{"name":"amounts","type":"uint256[]"}]
  }
]`

const uniV3RouterABIJSON = `[
  {"name":"exactInputSingle","type":"function","stateMutability":"payable",
   "inputs":[{"name":"params","type":"tuple","components":[
     {"name":"tokenIn","type":"address"},
     {"name":"tokenOut","type":"address"},
     {"name":"fee","type":"uint24"},
     {"name":"recipient","type":"address"},
     {"name":"deadline","type":"uint256"},
     {"name":"amountIn","type":"uint256"},
     {"name":"amountOutMinimum","type":"uint256"},
     {"name":"sqrtPriceLimitX96","type":"uint160"}
   ]}],
   "outputs":[{"name":"amountOut","type":"uint256"}]
  }
]`

var (
	erc20Parsed     abi.ABI
	uniV2RouterABIv abi.ABI
	uniV3RouterABIv abi.ABI
)

func init() {
	var err error
	erc20Parsed, err = abi.JSON(strings.NewReader(erc20ABIJSON))
	if err != nil {
		panic(err)
	}
	uniV2RouterABIv, err = abi.JSON(strings.NewReader(uniV2RouterABIJSON))
	if err != nil {
		panic(err)
	}
	uniV3RouterABIv, err = abi.JSON(strings.NewReader(uniV3RouterABIJSON))
	if err != nil {
		panic(err)
	}
}

func loadRealTradingSettings() {
	sim := strings.TrimSpace(strings.ToLower(os.Getenv("SIMULATION")))
	realTradingEnabled = sim == "0" || sim == "false" || sim == "off"
	if v := strings.TrimSpace(os.Getenv("REAL_MAX_SLIPPAGE_PCT")); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f >= 0 {
			realMaxSlippagePct = f
		}
	}
	if v := strings.TrimSpace(os.Getenv("LOSS_LIMIT_USD")); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			realLossLimitUSD = f
		}
	}
	if v := strings.TrimSpace(os.Getenv("REAL_TRADE_LOG_FILE")); v != "" {
		realTradeLogPath = v
	}
	if realTradingEnabled {
		log.Printf("REAL MODE: SIMULATION=0 | notional=$%.0f | slippage≤%.2f%% | hard-stop=$%.2f | log=%s",
			notionalUSDForDisplay, realMaxSlippagePct, realLossLimitUSD, realTradeLogPath)
		appendRealTradeLog(fmt.Sprintf("%s | SESSION_START | pid=%d", time.Now().Format(time.RFC3339), os.Getpid()))
	} else {
		log.Printf("SIMULATION=1 — без отправки транзакций (paper trading)")
	}
}

func appendRealTradeLog(line string) {
	if strings.TrimSpace(realTradeLogPath) == "" {
		return
	}
	realMu.Lock()
	defer realMu.Unlock()
	f, err := os.OpenFile(realTradeLogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Printf("[REAL] log open: %v", err)
		return
	}
	defer f.Close()
	_, _ = f.WriteString(line + "\n")
}

func parseTraderKey() (*ecdsa.PrivateKey, common.Address, error) {
	raw := strings.TrimSpace(os.Getenv("TRADER_PRIVATE_KEY"))
	if raw == "" {
		return nil, common.Address{}, errors.New("TRADER_PRIVATE_KEY is empty")
	}
	raw = strings.TrimPrefix(raw, "0x")
	pk, err := crypto.HexToECDSA(raw)
	if err != nil {
		return nil, common.Address{}, err
	}
	addr := crypto.PubkeyToAddress(pk.PublicKey)
	return pk, addr, nil
}

func erc20Balance(ctx context.Context, ec *ethclient.Client, token, owner common.Address) (*big.Int, error) {
	data, err := erc20Parsed.Pack("balanceOf", owner)
	if err != nil {
		return nil, err
	}
	msg := ethereum.CallMsg{To: &token, Data: data}
	out, err := callContractRetry(ctx, ec, msg, nil)
	if err != nil {
		return nil, err
	}
	vals, err := erc20Parsed.Unpack("balanceOf", out)
	if err != nil || len(vals) != 1 {
		return nil, err
	}
	bal, ok := vals[0].(*big.Int)
	if !ok || bal == nil {
		return nil, errors.New("balanceOf: bad type")
	}
	return bal, nil
}

func erc20Allowance(ctx context.Context, ec *ethclient.Client, token, owner, spender common.Address) (*big.Int, error) {
	data, err := erc20Parsed.Pack("allowance", owner, spender)
	if err != nil {
		return nil, err
	}
	msg := ethereum.CallMsg{To: &token, Data: data}
	out, err := callContractRetry(ctx, ec, msg, nil)
	if err != nil {
		return nil, err
	}
	vals, err := erc20Parsed.Unpack("allowance", out)
	if err != nil || len(vals) != 1 {
		return nil, err
	}
	al, ok := vals[0].(*big.Int)
	if !ok || al == nil {
		return nil, errors.New("allowance: bad type")
	}
	return al, nil
}

func ensureApprove(ctx context.Context, ec *ethclient.Client, pk *ecdsa.PrivateKey, from common.Address, token, spender common.Address, need *big.Int) (*types.Receipt, error) {
	if need == nil || need.Sign() <= 0 {
		return nil, nil
	}
	al, err := erc20Allowance(ctx, ec, token, from, spender)
	if err == nil && al != nil && al.Cmp(need) >= 0 {
		return nil, nil
	}
	max := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
	data, err := erc20Parsed.Pack("approve", spender, max)
	if err != nil {
		return nil, err
	}
	tx, err := sendDynamicTx(ctx, ec, pk, from, &token, data, big.NewInt(0))
	if err != nil {
		return nil, err
	}
	rcpt, werr := waitReceipt(ctx, ec, tx.Hash(), 90*time.Second)
	return rcpt, werr
}

func sendDynamicTx(ctx context.Context, ec *ethclient.Client, pk *ecdsa.PrivateKey, from common.Address, to *common.Address, data []byte, value *big.Int) (*types.Transaction, error) {
	chainID, err := ec.ChainID(ctx)
	if err != nil {
		return nil, err
	}
	nonce, err := ec.PendingNonceAt(ctx, from)
	if err != nil {
		return nil, err
	}
	tip, err := ec.SuggestGasTipCap(ctx)
	if err != nil || tip == nil || tip.Sign() <= 0 {
		tip = big.NewInt(1_500_000) // ~0.0015 gwei
	}
	h, err := ec.HeaderByNumber(ctx, nil)
	if err != nil {
		return nil, err
	}
	baseFee := h.BaseFee
	if baseFee == nil {
		baseFee = big.NewInt(0)
	}
	feeCap := new(big.Int).Add(new(big.Int).Mul(baseFee, big.NewInt(2)), tip)

	msg := ethereum.CallMsg{From: from, To: to, Value: value, Data: data}
	gas, err := ec.EstimateGas(ctx, msg)
	if err != nil || gas == 0 {
		gas = 350000
	} else {
		gas = uint64(math.Ceil(float64(gas) * 1.20))
	}

	tx := types.NewTx(&types.DynamicFeeTx{
		ChainID:   chainID,
		Nonce:     nonce,
		GasTipCap: tip,
		GasFeeCap: feeCap,
		Gas:       gas,
		To:        to,
		Value:     value,
		Data:      data,
	})
	signed, err := types.SignTx(tx, types.LatestSignerForChainID(chainID), pk)
	if err != nil {
		return nil, err
	}
	if err := ec.SendTransaction(ctx, signed); err != nil {
		return nil, err
	}
	return signed, nil
}

func waitReceipt(ctx context.Context, ec *ethclient.Client, h common.Hash, timeout time.Duration) (*types.Receipt, error) {
	dl := time.Now().Add(timeout)
	for time.Now().Before(dl) {
		rcpt, err := ec.TransactionReceipt(ctx, h)
		if err == nil && rcpt != nil {
			return rcpt, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(1200 * time.Millisecond):
		}
	}
	return nil, errors.New("receipt timeout")
}

func wethWeiToUSD(w *big.Int) float64 {
	if w == nil {
		return 0
	}
	r := new(big.Rat).SetFrac(w, tenPowU8(18))
	r.Mul(r, ethUsdHint)
	f, _ := r.Float64()
	return f
}

func hardStopIfLossExceeded(ctx context.Context, ec *ethclient.Client, trader common.Address) {
	if !realTradingEnabled {
		return
	}
	bal, err := erc20Balance(ctx, ec, addrWETH, trader)
	if err != nil || bal == nil {
		return
	}
	curUSD := wethWeiToUSD(bal)
	realMu.Lock()
	if !realStartBalanceSet {
		realStartBalanceUSD = curUSD
		realStartBalanceSet = true
		realMu.Unlock()
		log.Printf("[REAL] стартовый баланс: ~$%.2f (WETH)", curUSD)
		return
	}
	start := realStartBalanceUSD
	realMu.Unlock()
	if start-curUSD >= realLossLimitUSD {
		log.Fatalf("FATAL: Loss limit reached. Stopping bot.")
	}
}

func pctToMinOut(expected *big.Int, slippagePct float64) *big.Int {
	if expected == nil || expected.Sign() <= 0 {
		return big.NewInt(0)
	}
	if slippagePct <= 0 {
		return new(big.Int).Set(expected)
	}
	m := 1.0 - (slippagePct / 100.0)
	if m <= 0 {
		return big.NewInt(0)
	}
	r := new(big.Rat).SetInt(expected)
	r.Mul(r, new(big.Rat).SetFloat64(m))
	return new(big.Int).Div(r.Num(), r.Denom())
}

func routerGetAmountsOut(ctx context.Context, ec *ethclient.Client, router common.Address, amountIn *big.Int, path []common.Address) (*big.Int, error) {
	if amountIn == nil || amountIn.Sign() <= 0 || len(path) < 2 {
		return nil, errors.New("bad getAmountsOut args")
	}
	data, err := uniV2RouterABIv.Pack("getAmountsOut", amountIn, path)
	if err != nil {
		return nil, err
	}
	msg := ethereum.CallMsg{To: &router, Data: data}
	out, err := callContractRetry(ctx, ec, msg, nil)
	if err != nil {
		return nil, err
	}
	vals, err := uniV2RouterABIv.Unpack("getAmountsOut", out)
	if err != nil || len(vals) < 1 {
		return nil, err
	}
	arr, ok := vals[0].([]*big.Int)
	if !ok || len(arr) < 2 || arr[len(arr)-1] == nil {
		return nil, errors.New("getAmountsOut: bad decode")
	}
	return new(big.Int).Set(arr[len(arr)-1]), nil
}

func parseV3FeeFromTag(tag string) (uint32, bool) {
	// tag like: UniV3(0.3%)
	if !strings.Contains(tag, "UniV3(") {
		return 0, false
	}
	switch {
	case strings.Contains(tag, "0.01%"):
		return 100, true
	case strings.Contains(tag, "0.05%"):
		return 500, true
	case strings.Contains(tag, "0.3%"):
		return 3000, true
	case strings.Contains(tag, "1%"):
		return 10000, true
	default:
		return 0, false
	}
}

func v3ExactInputSingleTxData(tokenIn, tokenOut common.Address, fee uint32, recipient common.Address, amountIn, amountOutMin *big.Int, deadline *big.Int) ([]byte, error) {
	type params struct {
		TokenIn           common.Address `abi:"tokenIn"`
		TokenOut          common.Address `abi:"tokenOut"`
		Fee               *big.Int       `abi:"fee"`
		Recipient         common.Address `abi:"recipient"`
		Deadline          *big.Int       `abi:"deadline"`
		AmountIn          *big.Int       `abi:"amountIn"`
		AmountOutMinimum  *big.Int       `abi:"amountOutMinimum"`
		SqrtPriceLimitX96 *big.Int       `abi:"sqrtPriceLimitX96"`
	}
	p := params{
		TokenIn:           tokenIn,
		TokenOut:          tokenOut,
		Fee:               big.NewInt(int64(fee)),
		Recipient:         recipient,
		Deadline:          deadline,
		AmountIn:          amountIn,
		AmountOutMinimum:  amountOutMin,
		SqrtPriceLimitX96: big.NewInt(0),
	}
	return uniV3RouterABIv.Pack("exactInputSingle", p)
}

func executeRealV2V2RoundTrip(ctx context.Context, ec *ethclient.Client, pairLabel string, buyName, sellName string, quote common.Address, wethIn *big.Int, estProfitUSD float64) {
	if !realTradingEnabled {
		return
	}
	activeTradeWg.Add(1)
	defer activeTradeWg.Done()

	pk, trader, err := parseTraderKey()
	if err != nil {
		log.Printf("[REAL] TRADER_PRIVATE_KEY не задан — пропуск реального исполнения (%s)", pairLabel)
		return
	}
	hardStopIfLossExceeded(ctx, ec, trader)

	if strings.Contains(buyName, "UniV3") || strings.Contains(sellName, "UniV3") || strings.Contains(buyName, "Aero") || strings.Contains(sellName, "Aero") {
		appendRealTradeLog(fmt.Sprintf("%s | %s | SKIP route=%s→%s | estProfit=$%.2f", time.Now().Format(time.RFC3339), pairLabel, buyName, sellName, estProfitUSD))
		return
	}
	isV2 := func(s string) bool { return s == "UniswapV2" || s == "SushiSwap" }
	if !isV2(buyName) || !isV2(sellName) {
		appendRealTradeLog(fmt.Sprintf("%s | %s | SKIP unsupported=%s→%s | estProfit=$%.2f", time.Now().Format(time.RFC3339), pairLabel, buyName, sellName, estProfitUSD))
		return
	}

	if flashArbContract == (common.Address{}) {
		appendRealTradeLog(fmt.Sprintf("%s | %s | SKIP FLASH_ARB_CONTRACT unset | estProfit=$%.2f", time.Now().Format(time.RFC3339), pairLabel, estProfitUSD))
		return
	}
	if isFlashBadToken(quote) {
		appendRealTradeLog(fmt.Sprintf("%s | %s | SKIP flash bad-token cache | quote=%s", time.Now().Format(time.RFC3339), pairLabel, quote.Hex()))
		return
	}

	var buyRouter, sellRouter common.Address
	if buyName == "UniswapV2" {
		buyRouter = addrUniswapV2Router
	} else {
		buyRouter = addrSushiV2Router
	}
	if sellName == "UniswapV2" {
		sellRouter = addrUniswapV2Router
	} else {
		sellRouter = addrSushiV2Router
	}

	amount := flashAmountForTrade(wethIn)
	if amount.Sign() <= 0 {
		return
	}

	ctxA, cancelA := context.WithTimeout(ctx, 25*time.Second)
	defer cancelA()

	bps, errPrem := aaveFlashPremiumBps(ctxA, ec)
	if errPrem != nil {
		bps = 5 // 0.05% по умолчанию (Aave V3 Base)
	}
	prem := aavePremiumWei(amount, bps)
	repay := new(big.Int).Add(amount, prem)

	leg1Exp, err := routerGetAmountsOut(ctxA, ec, buyRouter, amount, []common.Address{addrWETH, quote})
	if err != nil || leg1Exp == nil || leg1Exp.Sign() <= 0 {
		appendRealTradeLog(fmt.Sprintf("%s | %s | FLASH_ABORT leg1_quote | err=%v", time.Now().Format(time.RFC3339), pairLabel, err))
		return
	}
	minTok := minTokenAfterBuyLeg1(leg1Exp)

	leg2Exp, err := routerGetAmountsOut(ctxA, ec, sellRouter, minTok, []common.Address{quote, addrWETH})
	if err != nil || leg2Exp == nil || leg2Exp.Sign() <= 0 {
		appendRealTradeLog(fmt.Sprintf("%s | %s | FLASH_ABORT leg2_quote | err=%v", time.Now().Format(time.RFC3339), pairLabel, err))
		return
	}
	leg2Min := pctToMinOut(leg2Exp, realMaxSlippagePct)
	needWeth := new(big.Int).Add(repay, flashMinProfitWei)
	if leg2Min.Cmp(needWeth) < 0 {
		appendRealTradeLog(fmt.Sprintf("%s | %s | SKIP flash leg2Min<repay+minProfit | estProfit=$%.2f", time.Now().Format(time.RFC3339), pairLabel, estProfitUSD))
		return
	}

	deadline := big.NewInt(time.Now().Add(60 * time.Second).Unix())
	buyData, err := uniV2RouterABIv.Pack("swapExactTokensForTokens", amount, minTok, []common.Address{addrWETH, quote}, flashArbContract, deadline)
	if err != nil {
		return
	}

	args := flashArbArgs{
		Asset:            addrWETH,
		Amount:           amount,
		BuyRouter:        buyRouter,
		BuyCalldata:      buyData,
		SellRouter:       sellRouter,
		QuoteToken:       quote,
		V3Sell:           false,
		V3Fee:            0,
		MinWethOut:       leg2Min,
		MinTokenAfterBuy: minTok,
		MinProfitWei:     new(big.Int).Set(flashMinProfitWei),
		Deadline:         deadline,
	}

	if err := flashArbSimulate(ctxA, ec, trader, args); err != nil {
		appendRealTradeLog(fmt.Sprintf("%s | %s | FLASH_SIM_FAIL | err=%v | estProfit=$%.2f", time.Now().Format(time.RFC3339), pairLabel, err, estProfitUSD))
		return
	}

	tx, err := flashArbExecute(ctxA, ec, pk, trader, args)
	if err != nil {
		appendRealTradeLog(fmt.Sprintf("%s | %s | FLASH_TX_SUBMIT_FAIL | err=%v", time.Now().Format(time.RFC3339), pairLabel, err))
		return
	}
	rc, err := waitReceipt(ctxA, ec, tx.Hash(), 120*time.Second)
	status := "SUCCESS"
	if err != nil || rc == nil || rc.Status != 1 {
		status = "FAILED"
		markFlashBadToken(quote)
	}
	hardStopIfLossExceeded(ctxA, ec, trader)
	appendRealTradeLog(fmt.Sprintf("%s | %s | %s FLASH | estProfit=$%.2f | tx=%s",
		time.Now().Format(time.RFC3339), pairLabel, status, estProfitUSD, tx.Hash().Hex()))
}

// executeRealHybridV3V2RoundTrip — один tx: Aave flash loan + UniV3/V2 buy + V2/V3 sell через FlashArb.
func executeRealHybridV3V2RoundTrip(ctx context.Context, ec *ethclient.Client, pairLabel string, buyName, sellName string, quote common.Address, wethIn *big.Int, estProfitUSD float64) {
	if !realTradingEnabled {
		return
	}
	activeTradeWg.Add(1)
	defer activeTradeWg.Done()

	pk, trader, err := parseTraderKey()
	if err != nil {
		log.Printf("[REAL] key: %v", err)
		return
	}
	hardStopIfLossExceeded(ctx, ec, trader)

	if strings.Contains(buyName, "Aero") || strings.Contains(sellName, "Aero") {
		appendRealTradeLog(fmt.Sprintf("%s | %s | SKIP Aero flash | route=%s→%s", time.Now().Format(time.RFC3339), pairLabel, buyName, sellName))
		return
	}
	if flashArbContract == (common.Address{}) {
		appendRealTradeLog(fmt.Sprintf("%s | %s | SKIP FLASH_ARB_CONTRACT unset | estProfit=$%.2f", time.Now().Format(time.RFC3339), pairLabel, estProfitUSD))
		return
	}
	if isFlashBadToken(quote) {
		appendRealTradeLog(fmt.Sprintf("%s | %s | SKIP flash bad-token cache | quote=%s", time.Now().Format(time.RFC3339), pairLabel, quote.Hex()))
		return
	}

	fee, ok := parseV3FeeFromTag(buyName)
	v3First := true
	if !ok {
		fee, ok = parseV3FeeFromTag(sellName)
		v3First = false
	}
	if !ok {
		appendRealTradeLog(fmt.Sprintf("%s | %s | SKIP bad_v3_tag=%s→%s", time.Now().Format(time.RFC3339), pairLabel, buyName, sellName))
		return
	}

	v2Name := buyName
	if strings.Contains(v2Name, "UniV3") {
		v2Name = sellName
	}
	var v2Router common.Address
	switch v2Name {
	case "UniswapV2", "SushiSwapV2", "SushiSwap":
		if v2Name == "UniswapV2" {
			v2Router = addrUniswapV2Router
		} else {
			v2Router = addrSushiV2Router
		}
	default:
		appendRealTradeLog(fmt.Sprintf("%s | %s | SKIP unsupported_v2=%s", time.Now().Format(time.RFC3339), pairLabel, v2Name))
		return
	}

	amount := flashAmountForTrade(wethIn)
	if amount.Sign() <= 0 {
		return
	}

	ctxA, cancelA := context.WithTimeout(ctx, 30*time.Second)
	defer cancelA()

	bps, errPrem := aaveFlashPremiumBps(ctxA, ec)
	if errPrem != nil {
		bps = 5
	}
	prem := aavePremiumWei(amount, bps)
	repay := new(big.Int).Add(amount, prem)
	needWeth := new(big.Int).Add(repay, flashMinProfitWei)

	deadline := big.NewInt(time.Now().Add(60 * time.Second).Unix())

	var leg1OutExp *big.Int
	if v3First {
		leg1OutExp, err = quoteV3ExactInputSingle(ctxA, ec, addrWETH, quote, fee, amount)
	} else {
		leg1OutExp, err = routerGetAmountsOut(ctxA, ec, v2Router, amount, []common.Address{addrWETH, quote})
	}
	if err != nil || leg1OutExp == nil || leg1OutExp.Sign() <= 0 {
		appendRealTradeLog(fmt.Sprintf("%s | %s | FLASH_ABORT leg1_quote | err=%v", time.Now().Format(time.RFC3339), pairLabel, err))
		return
	}
	minTok := minTokenAfterBuyLeg1(leg1OutExp)

	var buyRouter common.Address
	var buyData []byte
	if v3First {
		buyRouter = addrUniswapV3Router
		buyData, err = v3ExactInputSingleTxData(addrWETH, quote, fee, flashArbContract, amount, minTok, deadline)
	} else {
		buyRouter = v2Router
		buyData, err = uniV2RouterABIv.Pack("swapExactTokensForTokens", amount, minTok, []common.Address{addrWETH, quote}, flashArbContract, deadline)
	}
	if err != nil {
		appendRealTradeLog(fmt.Sprintf("%s | %s | FLASH_ABORT pack_buy | err=%v", time.Now().Format(time.RFC3339), pairLabel, err))
		return
	}

	var leg2OutExp *big.Int
	var v3Sell bool
	var sellRouter common.Address
	var v3FeeU32 uint32
	if v3First {
		leg2OutExp, err = routerGetAmountsOut(ctxA, ec, v2Router, minTok, []common.Address{quote, addrWETH})
		v3Sell = false
		sellRouter = v2Router
		v3FeeU32 = 0
	} else {
		leg2OutExp, err = quoteV3ExactInputSingle(ctxA, ec, quote, addrWETH, fee, minTok)
		v3Sell = true
		sellRouter = addrUniswapV3Router
		v3FeeU32 = fee
	}
	if err != nil || leg2OutExp == nil || leg2OutExp.Sign() <= 0 {
		appendRealTradeLog(fmt.Sprintf("%s | %s | FLASH_ABORT leg2_quote | err=%v", time.Now().Format(time.RFC3339), pairLabel, err))
		return
	}
	leg2Min := pctToMinOut(leg2OutExp, realMaxSlippagePct)
	if leg2Min.Cmp(needWeth) < 0 {
		appendRealTradeLog(fmt.Sprintf("%s | %s | SKIP flash leg2Min<repay+minProfit | estProfit=$%.2f", time.Now().Format(time.RFC3339), pairLabel, estProfitUSD))
		return
	}

	args := flashArbArgs{
		Asset:            addrWETH,
		Amount:           amount,
		BuyRouter:        buyRouter,
		BuyCalldata:      buyData,
		SellRouter:       sellRouter,
		QuoteToken:       quote,
		V3Sell:           v3Sell,
		V3Fee:            v3FeeU32,
		MinWethOut:       leg2Min,
		MinTokenAfterBuy: minTok,
		MinProfitWei:     new(big.Int).Set(flashMinProfitWei),
		Deadline:         deadline,
	}

	if err := flashArbSimulate(ctxA, ec, trader, args); err != nil {
		appendRealTradeLog(fmt.Sprintf("%s | %s | FLASH_SIM_FAIL | err=%v | route=%s→%s", time.Now().Format(time.RFC3339), pairLabel, err, buyName, sellName))
		return
	}

	tx, err := flashArbExecute(ctxA, ec, pk, trader, args)
	if err != nil {
		appendRealTradeLog(fmt.Sprintf("%s | %s | FLASH_TX_SUBMIT_FAIL | err=%v", time.Now().Format(time.RFC3339), pairLabel, err))
		return
	}
	rc, err := waitReceipt(ctxA, ec, tx.Hash(), 120*time.Second)
	status := "SUCCESS"
	if err != nil || rc == nil || rc.Status != 1 {
		status = "FAILED"
		markFlashBadToken(quote)
	}
	hardStopIfLossExceeded(ctxA, ec, trader)
	appendRealTradeLog(fmt.Sprintf("%s | %s | %s FLASH hybrid | route=%s→%s | estProfit=$%.2f | tx=%s",
		time.Now().Format(time.RFC3339), pairLabel, status, buyName, sellName, estProfitUSD, tx.Hash().Hex()))
}

