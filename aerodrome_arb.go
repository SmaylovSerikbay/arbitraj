package main

import (
	"context"
	"log"
	"math/big"
	"os"
	"strings"
	"sync/atomic"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

// Aerodrome (Solidly-style) на Base — основная ликвидность наряду с UniV3.
// Pool factory: https://basescan.org/address/0x420dd381b31aef6683db6b902084cb0ffece40da
var (
	addrAerodromeFactory = common.HexToAddress("0x420DD381b31aEf6683db6B902084cB0FFECe40Da")
)

const aeroFactoryABIJSON = `[{"inputs":[{"internalType":"address","name":"tokenA","type":"address"},{"internalType":"address","name":"tokenB","type":"address"},{"internalType":"bool","name":"stable","type":"bool"}],"name":"getPool","outputs":[{"internalType":"address","name":"pool","type":"address"}],"stateMutability":"view","type":"function"}]`

const aeroPoolABIJSON = `[{"inputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"address","name":"tokenIn","type":"address"}],"name":"getAmountOut","outputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"}],"stateMutability":"view","type":"function"}]`

var (
	aeroFactoryParsed abi.ABI
	aeroPoolParsed    abi.ABI
	enableAeroArb     = true
)

// V3-only route telemetry (atomic).
var (
	v3OnlyEvalCalls  uint64
	v3OnlyV3V3Wins   uint64
	v3OnlyV3AeroWins uint64
	aeroBestWethUp   uint64
	aeroNetPositive  uint64
)

func init() {
	var err error
	aeroFactoryParsed, err = abi.JSON(strings.NewReader(aeroFactoryABIJSON))
	if err != nil {
		panic(err)
	}
	aeroPoolParsed, err = abi.JSON(strings.NewReader(aeroPoolABIJSON))
	if err != nil {
		panic(err)
	}
}

func loadAeroSettings() {
	if strings.TrimSpace(os.Getenv("ENABLE_AERO_ARB")) == "0" {
		enableAeroArb = false
		log.Printf("ENABLE_AERO_ARB=0 — без маршрутов через Aerodrome")
		return
	}
	enableAeroArb = true
	log.Printf("Aerodrome: включено (getPool + getAmountOut на пуле)")
}

func aeroStableLabel(stable bool) string {
	if stable {
		return "stable"
	}
	return "volatile"
}

func getAerodromePool(ctx context.Context, ec *ethclient.Client, tokenA, tokenB common.Address, stable bool) (common.Address, error) {
	t0, t1 := sortTokens(tokenA, tokenB)
	data, err := aeroFactoryParsed.Pack("getPool", t0, t1, stable)
	if err != nil {
		return common.Address{}, err
	}
	msg := ethereum.CallMsg{To: &addrAerodromeFactory, Data: data}
	out, err := callContractRetry(ctx, ec, msg, nil)
	if err != nil {
		return common.Address{}, err
	}
	var pool common.Address
	if err := aeroFactoryParsed.UnpackIntoInterface(&pool, "getPool", out); err != nil {
		return common.Address{}, err
	}
	return pool, nil
}

func quoteAeroGetAmountOut(ctx context.Context, ec *ethclient.Client, pool common.Address, amountIn *big.Int, tokenIn common.Address) (*big.Int, error) {
	if pool == (common.Address{}) || amountIn == nil || amountIn.Sign() <= 0 {
		return nil, nil
	}
	data, err := aeroPoolParsed.Pack("getAmountOut", amountIn, tokenIn)
	if err != nil {
		return nil, err
	}
	msg := ethereum.CallMsg{To: &pool, Data: data}
	out, err := callContractRetry(ctx, ec, msg, nil)
	if err != nil {
		return nil, err
	}
	vals, err := aeroPoolParsed.Unpack("getAmountOut", out)
	if err != nil || len(vals) < 1 {
		return nil, err
	}
	amt, ok := vals[0].(*big.Int)
	if !ok || amt == nil {
		return nil, err
	}
	return amt, nil
}

// v3AeroHybridBestProfit — лучший чистый USD после WETH→…→WETH (один UniV3 + один Aerodrome своп).
func v3AeroHybridBestProfit(
	ctx context.Context,
	ec *ethclient.Client,
	quote common.Address,
	wethIn *big.Int,
	baseGasUSD *big.Rat,
) (potNet *big.Rat, buyName, sellName string, ok bool) {
	if !enableAeroArb || ec == nil || wethIn == nil || wethIn.Sign() <= 0 {
		return nil, "", "", false
	}
	gasHybrid := new(big.Rat).Mul(baseGasUSD, hybridGasMult)

	fees := []uint32{3000, 500, 10000, 100}
	stables := []bool{false, true}

	var bestWei *big.Int
	var bBuy, bSell string

	try := func(tagBuy, tagSell string, wBack *big.Int) {
		if wBack == nil || wBack.Sign() <= 0 {
			return
		}
		if bestWei == nil || wBack.Cmp(bestWei) > 0 {
			bestWei = new(big.Int).Set(wBack)
			bBuy, bSell = tagBuy, tagSell
		}
	}

	for _, stable := range stables {
		aPool, err := getAerodromePool(ctx, ec, addrWETH, quote, stable)
		if err != nil || aPool == (common.Address{}) {
			continue
		}
		al := "Aero(" + aeroStableLabel(stable) + ")"

		// V3 WETH→token, Aerodrome token→WETH
		for _, fee := range fees {
			if p, err := getV3Pool(ctx, ec, addrWETH, quote, fee); err != nil || p == (common.Address{}) {
				continue
			}
			tokOut, err := quoteV3ExactInputSingle(ctx, ec, addrWETH, quote, fee, wethIn)
			if err != nil || tokOut == nil || tokOut.Sign() <= 0 {
				continue
			}
			wBack, err := quoteAeroGetAmountOut(ctx, ec, aPool, tokOut, quote)
			if err != nil || wBack == nil || wBack.Sign() <= 0 {
				continue
			}
			try("UniV3("+feeTag(fee)+")", al, wBack)
		}

		// Aerodrome WETH→token, V3 token→WETH
		tokFromAero, err := quoteAeroGetAmountOut(ctx, ec, aPool, wethIn, addrWETH)
		if err != nil || tokFromAero == nil || tokFromAero.Sign() <= 0 {
			continue
		}
		for _, fee := range fees {
			if p, err := getV3Pool(ctx, ec, addrWETH, quote, fee); err != nil || p == (common.Address{}) {
				continue
			}
			wBack, err := quoteV3ExactInputSingle(ctx, ec, quote, addrWETH, fee, tokFromAero)
			if err != nil || wBack == nil || wBack.Sign() <= 0 {
				continue
			}
			try(al, "UniV3("+feeTag(fee)+")", wBack)
		}
	}

	if bestWei == nil || bestWei.Sign() <= 0 || bBuy == "" {
		return nil, "", "", false
	}
	if bestWei.Cmp(wethIn) > 0 {
		atomic.AddUint64(&aeroBestWethUp, 1)
	}
	profitWei := new(big.Int).Sub(bestWei, wethIn)
	pUSD := new(big.Rat).Mul(new(big.Rat).SetFrac(profitWei, tenPowU8(18)), ethUsdHint)
	pot := new(big.Rat).Sub(pUSD, gasHybrid)
	if pot.Sign() > 0 {
		atomic.AddUint64(&aeroNetPositive, 1)
	}
	return pot, bBuy, bSell, true
}

func bestV3OnlyProfit(
	ctx context.Context,
	ec *ethclient.Client,
	quote common.Address,
	wethIn *big.Int,
	baseGasUSD *big.Rat,
) (potNet *big.Rat, buyName, sellName, route string, ok bool) {
	atomic.AddUint64(&v3OnlyEvalCalls, 1)
	var best *big.Rat
	var bb, ss string
	var bestRoute string

	if p, b, s, ok := v3V3BestProfit(ctx, ec, quote, wethIn, baseGasUSD); ok && p != nil && p.Sign() > 0 {
		best = p
		bb, ss = b, s
		bestRoute = "v3v3"
	}
	if enableAeroArb {
		if p, b, s, ok := v3AeroHybridBestProfit(ctx, ec, quote, wethIn, baseGasUSD); ok && p != nil && p.Sign() > 0 {
			if best == nil || p.Cmp(best) > 0 {
				best = p
				bb, ss = b, s
				bestRoute = "v3aero"
			}
		}
	}
	if best == nil || best.Sign() <= 0 {
		return nil, "", "", "", false
	}
	if bestRoute == "v3v3" {
		atomic.AddUint64(&v3OnlyV3V3Wins, 1)
	} else if bestRoute == "v3aero" {
		atomic.AddUint64(&v3OnlyV3AeroWins, 1)
	}
	return best, bb, ss, bestRoute, true
}

func v3OnlyTelemetrySnapshot() (evals, v3v3, v3aero uint64) {
	return atomic.LoadUint64(&v3OnlyEvalCalls),
		atomic.LoadUint64(&v3OnlyV3V3Wins),
		atomic.LoadUint64(&v3OnlyV3AeroWins)
}

func aeroHybridOppTelemetrySnapshot() (bestUp, netPos uint64) {
	return atomic.LoadUint64(&aeroBestWethUp), atomic.LoadUint64(&aeroNetPositive)
}
