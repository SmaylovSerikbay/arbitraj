package main

import (
	"context"
	"log"
	"math/big"
	"os"
	"strings"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

// Uniswap V3 на Base (официальные деплои).
var (
	addrUniswapV3Factory = common.HexToAddress("0x33128a8fC17869897dcE68Ed026d694621f6FDfD")
	addrQuoterV2Base     = common.HexToAddress("0x3d4e44Eb1374240CE5F1B871ab261CD16335B76a")
)

const v3FactoryABIJSON = `[{"inputs":[{"internalType":"address","name":"tokenA","type":"address"},{"internalType":"address","name":"tokenB","type":"address"},{"internalType":"uint24","name":"fee","type":"uint24"}],"name":"getPool","outputs":[{"internalType":"address","name":"pool","type":"address"}],"stateMutability":"view","type":"function"}]`

// QuoterV2.quoteExactInputSingle(QuoteExactInputSingleParams params)
const quoterV2ABIJSON = `[{"inputs":[{"components":[{"internalType":"address","name":"tokenIn","type":"address"},{"internalType":"address","name":"tokenOut","type":"address"},{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint24","name":"fee","type":"uint24"},{"internalType":"uint160","name":"sqrtPriceLimitX96","type":"uint160"}],"internalType":"struct QuoteExactInputSingleParams","name":"params","type":"tuple"}],"name":"quoteExactInputSingle","outputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"},{"internalType":"uint160","name":"sqrtPriceX96After","type":"uint160"},{"internalType":"uint32","name":"initializedTicksCrossed","type":"uint32"},{"internalType":"uint256","name":"gasEstimate","type":"uint256"}],"stateMutability":"nonpayable","type":"function"}]`

var (
	v3FactoryParsed abi.ABI
	quoterV2Parsed  abi.ABI
	enableV3Arb     = true
	hybridGasMult   = big.NewRat(135, 100) // V3+2×V2 свопы тяжелее; подстройка через HYBRID_GAS_MULT
)

func init() {
	var err error
	v3FactoryParsed, err = abi.JSON(strings.NewReader(v3FactoryABIJSON))
	if err != nil {
		panic(err)
	}
	quoterV2Parsed, err = abi.JSON(strings.NewReader(quoterV2ABIJSON))
	if err != nil {
		panic(err)
	}
}

func loadV3ArbSettings() {
	if strings.TrimSpace(os.Getenv("ENABLE_V3_ARB")) == "0" {
		enableV3Arb = false
		log.Printf("ENABLE_V3_ARB=0 — только UniswapV2 ↔ SushiSwap V2")
		return
	}
	enableV3Arb = true
	s := strings.TrimSpace(os.Getenv("HYBRID_GAS_MULT"))
	if s != "" {
		r := new(big.Rat)
		if _, ok := r.SetString(s); ok && r.Sign() > 0 {
			hybridGasMult = r
		}
	}
	log.Printf("V3↔V2: включено (QuoterV2), газ гибрида ×%s от базового", hybridGasMult.FloatString(2))
}

func getV3Pool(ctx context.Context, ec *ethclient.Client, tokenA, tokenB common.Address, fee uint32) (common.Address, error) {
	t0, t1 := sortTokens(tokenA, tokenB)
	data, err := v3FactoryParsed.Pack("getPool", t0, t1, big.NewInt(int64(fee)))
	if err != nil {
		return common.Address{}, err
	}
	msg := ethereum.CallMsg{To: &addrUniswapV3Factory, Data: data}
	out, err := ec.CallContract(ctx, msg, nil)
	if err != nil {
		return common.Address{}, err
	}
	var pool common.Address
	if err := v3FactoryParsed.UnpackIntoInterface(&pool, "getPool", out); err != nil {
		return common.Address{}, err
	}
	return pool, nil
}

func quoteV3ExactInputSingle(ctx context.Context, ec *ethclient.Client, tokenIn, tokenOut common.Address, fee uint32, amountIn *big.Int) (*big.Int, error) {
	type qparams struct {
		TokenIn           common.Address `abi:"tokenIn"`
		TokenOut          common.Address `abi:"tokenOut"`
		AmountIn          *big.Int       `abi:"amountIn"`
		Fee               *big.Int       `abi:"fee"`
		SqrtPriceLimitX96 *big.Int       `abi:"sqrtPriceLimitX96"`
	}
	p := qparams{
		TokenIn:           tokenIn,
		TokenOut:          tokenOut,
		AmountIn:          new(big.Int).Set(amountIn),
		Fee:               big.NewInt(int64(fee)),
		SqrtPriceLimitX96: big.NewInt(0),
	}
	data, err := quoterV2Parsed.Pack("quoteExactInputSingle", p)
	if err != nil {
		return nil, err
	}
	msg := ethereum.CallMsg{To: &addrQuoterV2Base, Data: data}
	out, err := ec.CallContract(ctx, msg, nil)
	if err != nil {
		return nil, err
	}
	vals, err := quoterV2Parsed.Unpack("quoteExactInputSingle", out)
	if err != nil || len(vals) < 1 {
		return nil, err
	}
	amt, ok := vals[0].(*big.Int)
	if !ok || amt == nil {
		return nil, err
	}
	return amt, nil
}

// hybridV3V2BestProfit — лучший чистый USD после WETH→…→WETH (один V3 + один V2 своп).
func hybridV3V2BestProfit(
	ctx context.Context,
	ec *ethclient.Client,
	quote common.Address,
	wethIn *big.Int,
	wethIsT0 bool,
	u0, u1, s0, s1 *big.Int,
	baseGasUSD *big.Rat,
) (potNet *big.Rat, buyName, sellName string, ok bool) {
	if ec == nil || wethIn == nil || wethIn.Sign() <= 0 {
		return nil, "", "", false
	}
	gasHybrid := new(big.Rat).Mul(baseGasUSD, hybridGasMult)

	fees := []uint32{3000, 500, 10000, 100}
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

	for _, fee := range fees {
		pool, err := getV3Pool(ctx, ec, addrWETH, quote, fee)
		if err != nil || pool == (common.Address{}) {
			continue
		}
		// V3 buy WETH→token
		tokOut, err := quoteV3ExactInputSingle(ctx, ec, addrWETH, quote, fee, wethIn)
		if err == nil && tokOut.Sign() > 0 {
			var wback *big.Int
			if wethIsT0 {
				wback = getAmountOut(tokOut, u1, u0)
			} else {
				wback = getAmountOut(tokOut, u0, u1)
			}
			try("UniV3("+feeTag(fee)+")", "UniswapV2", wback)
			var wbackS *big.Int
			if wethIsT0 {
				wbackS = getAmountOut(tokOut, s1, s0)
			} else {
				wbackS = getAmountOut(tokOut, s0, s1)
			}
			try("UniV3("+feeTag(fee)+")", "SushiSwapV2", wbackS)
		}
		// Купить на Uni V2, продать через V3
		var xFromUni *big.Int
		if wethIsT0 {
			xFromUni = getAmountOut(wethIn, u0, u1)
		} else {
			xFromUni = getAmountOut(wethIn, u1, u0)
		}
		if xFromUni.Sign() > 0 {
			wb, err3 := quoteV3ExactInputSingle(ctx, ec, quote, addrWETH, fee, xFromUni)
			if err3 == nil && wb != nil && wb.Sign() > 0 {
				try("UniswapV2", "UniV3("+feeTag(fee)+")", wb)
			}
		}
		var xFromSu *big.Int
		if wethIsT0 {
			xFromSu = getAmountOut(wethIn, s0, s1)
		} else {
			xFromSu = getAmountOut(wethIn, s1, s0)
		}
		if xFromSu.Sign() > 0 {
			wb, err4 := quoteV3ExactInputSingle(ctx, ec, quote, addrWETH, fee, xFromSu)
			if err4 == nil && wb != nil && wb.Sign() > 0 {
				try("SushiSwapV2", "UniV3("+feeTag(fee)+")", wb)
			}
		}
	}

	if bestWei == nil || bestWei.Sign() <= 0 || bBuy == "" {
		return nil, "", "", false
	}
	profitWei := new(big.Int).Sub(bestWei, wethIn)
	pUSD := new(big.Rat).Mul(new(big.Rat).SetFrac(profitWei, tenPowU8(18)), ethUsdHint)
	pot := new(big.Rat).Sub(pUSD, gasHybrid)
	return pot, bBuy, bSell, true
}

func feeTag(fee uint32) string {
	switch fee {
	case 100:
		return "0.01%"
	case 500:
		return "0.05%"
	case 3000:
		return "0.3%"
	case 10000:
		return "1%"
	default:
		return "fee"
	}
}

func ethClientForV3() *ethclient.Client {
	gasOracleMu.Lock()
	defer gasOracleMu.Unlock()
	return gasOracleClient
}
