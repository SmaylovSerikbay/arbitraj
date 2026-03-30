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
)

var (
	realTradingEnabled bool = false // SIMULATION=0 => true
	realMaxSlippagePct       = 0.3   // %
	realLossLimitUSD         = 10.0  // hard stop
	realTradeLogPath         = "real_trading.log"

	realStartBalanceUSD float64
	realStartBalanceSet bool

	realMu sync.Mutex
)

const erc20ABIJSON = `[{"constant":true,"inputs":[{"name":"owner","type":"address"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"type":"function"},{"constant":true,"inputs":[{"name":"owner","type":"address"},{"name":"spender","type":"address"}],"name":"allowance","outputs":[{"name":"","type":"uint256"}],"type":"function"},{"constant":false,"inputs":[{"name":"spender","type":"address"},{"name":"amount","type":"uint256"}],"name":"approve","outputs":[{"name":"","type":"bool"}],"type":"function"}]`

const uniV2RouterABIJSON = `[
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

var (
	erc20Parsed     abi.ABI
	uniV2RouterABIv abi.ABI
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
	if m < 0 {
		m = 0
	}
	// minOut = floor(expected * m)
	f, _ := new(big.Rat).SetInt(expected).Mul(new(big.Rat).SetInt(expected), new(big.Rat).SetFloat64(m)).Float64()
	if f <= 0 {
		return big.NewInt(0)
	}
	return big.NewInt(int64(f))
}

func executeRealV2V2RoundTrip(ctx context.Context, ec *ethclient.Client, pairLabel string, buyName, sellName string, quote common.Address, wethIn *big.Int, estProfitUSD float64) {
	if !realTradingEnabled {
		return
	}
	// hard stop check before any tx
	_, from, err := parseTraderKey()
	if err != nil {
		log.Printf("[REAL] TRADER_PRIVATE_KEY не задан — пропуск реального исполнения (%s)", pairLabel)
		return
	}
	hardStopIfLossExceeded(ctx, ec, from)

	if strings.Contains(buyName, "UniV3") || strings.Contains(sellName, "UniV3") || strings.Contains(buyName, "Aero") || strings.Contains(sellName, "Aero") {
		appendRealTradeLog(fmt.Sprintf("%s | %s | SKIP route=%s→%s | estProfit=$%.2f", time.Now().Format(time.RFC3339), pairLabel, buyName, sellName, estProfitUSD))
		return
	}
	// allow only V2<->V2 (UniswapV2 <-> SushiSwap)
	isV2 := func(s string) bool { return s == "UniswapV2" || s == "SushiSwap" }
	if !isV2(buyName) || !isV2(sellName) {
		appendRealTradeLog(fmt.Sprintf("%s | %s | SKIP unsupported=%s→%s | estProfit=$%.2f", time.Now().Format(time.RFC3339), pairLabel, buyName, sellName, estProfitUSD))
		return
	}

	pk, trader, err := parseTraderKey()
	if err != nil {
		log.Printf("[REAL] key: %v", err)
		return
	}
	if trader != from {
		// should never happen
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

	// expected outs by current reserves (already in tp); use AMM formula.
	// First leg: WETH -> quote
	// We don't have reserves here; so in real mode we require that caller already decided V2 route and we do minimal slippage only.
	// amountOutMin computed as amountIn * (1 - slippage) just to cap tax tokens; conservative.
	outMin1 := pctToMinOut(wethIn, realMaxSlippagePct)
	outMin2 := pctToMinOut(wethIn, realMaxSlippagePct)

	// approvals
	ctxA, cancelA := context.WithTimeout(ctx, 20*time.Second)
	defer cancelA()
	if _, err := ensureApprove(ctxA, ec, pk, trader, addrWETH, buyRouter, wethIn); err != nil {
		appendRealTradeLog(fmt.Sprintf("%s | %s | APPROVE_FAIL WETH->%s | err=%v", time.Now().Format(time.RFC3339), pairLabel, buyName, err))
		return
	}

	// swap1
	deadline := big.NewInt(time.Now().Add(60 * time.Second).Unix())
	data1, err := uniV2RouterABIv.Pack("swapExactTokensForTokens", wethIn, outMin1, []common.Address{addrWETH, quote}, trader, deadline)
	if err != nil {
		return
	}
	tx1, err := sendDynamicTx(ctxA, ec, pk, trader, &buyRouter, data1, big.NewInt(0))
	if err != nil {
		appendRealTradeLog(fmt.Sprintf("%s | %s | TX1_SUBMIT_FAIL %s | err=%v", time.Now().Format(time.RFC3339), pairLabel, buyName, err))
		return
	}
	rc1, err := waitReceipt(ctxA, ec, tx1.Hash(), 120*time.Second)
	if err != nil || rc1 == nil || rc1.Status != 1 {
		appendRealTradeLog(fmt.Sprintf("%s | %s | TX1_FAIL %s | hash=%s | err=%v", time.Now().Format(time.RFC3339), pairLabel, buyName, tx1.Hash().Hex(), err))
		return
	}

	// for swap2 we need current quote balance (all-in)
	qBal, err := erc20Balance(ctxA, ec, quote, trader)
	if err != nil || qBal == nil || qBal.Sign() <= 0 {
		appendRealTradeLog(fmt.Sprintf("%s | %s | TX2_ABORT noQuoteBalance | tx1=%s", time.Now().Format(time.RFC3339), pairLabel, tx1.Hash().Hex()))
		return
	}
	if _, err := ensureApprove(ctxA, ec, pk, trader, quote, sellRouter, qBal); err != nil {
		appendRealTradeLog(fmt.Sprintf("%s | %s | APPROVE_FAIL quote->%s | err=%v", time.Now().Format(time.RFC3339), pairLabel, sellName, err))
		return
	}

	data2, err := uniV2RouterABIv.Pack("swapExactTokensForTokens", qBal, outMin2, []common.Address{quote, addrWETH}, trader, deadline)
	if err != nil {
		return
	}
	tx2, err := sendDynamicTx(ctxA, ec, pk, trader, &sellRouter, data2, big.NewInt(0))
	if err != nil {
		appendRealTradeLog(fmt.Sprintf("%s | %s | TX2_SUBMIT_FAIL %s | err=%v | tx1=%s", time.Now().Format(time.RFC3339), pairLabel, sellName, err, tx1.Hash().Hex()))
		return
	}
	rc2, err := waitReceipt(ctxA, ec, tx2.Hash(), 120*time.Second)
	status := "SUCCESS"
	if err != nil || rc2 == nil || rc2.Status != 1 {
		status = "FAILED"
	}

	// hard stop after trade
	hardStopIfLossExceeded(ctxA, ec, trader)

	appendRealTradeLog(fmt.Sprintf("%s | %s | %s | estProfit=$%.2f | tx1=%s | tx2=%s",
		time.Now().Format(time.RFC3339), pairLabel, status, estProfitUSD, tx1.Hash().Hex(), tx2.Hash().Hex()))
}

