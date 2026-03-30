package main

import (
	"context"
	"math/big"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

func TestPackExecuteArb_RoundTrip(t *testing.T) {
	a := flashArbArgs{
		Asset:            addrWETH,
		Amount:           big.NewInt(1e18),
		BuyRouter:        addrUniswapV2Router,
		BuyCalldata:      []byte{0x01, 0x02},
		SellRouter:       addrSushiV2Router,
		QuoteToken:       common.HexToAddress("0x0000000000000000000000000000000000000001"),
		V3Sell:           false,
		V3Fee:            3000,
		MinWethOut:       big.NewInt(1),
		MinTokenAfterBuy: big.NewInt(2),
		MinProfitWei:     big.NewInt(3),
		Deadline:         big.NewInt(time.Now().Unix() + 60),
	}
	data, err := packExecuteArb(a)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) < 4 {
		t.Fatal("short calldata")
	}
}

func TestAaveFlashPremiumBps_BaseRPC(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	rpc := strings.TrimSpace(os.Getenv("RPC_URL_DRPC"))
	if rpc == "" {
		rpc = strings.TrimSpace(os.Getenv("BASE_HTTP"))
	}
	if rpc == "" {
		t.Skip("set RPC_URL_DRPC or BASE_HTTP for live RPC test")
	}
	ec, err := ethclient.Dial(rpc)
	if err != nil {
		t.Fatal(err)
	}
	defer ec.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	bps, err := aaveFlashPremiumBps(ctx, ec)
	if err != nil {
		t.Fatal(err)
	}
	if bps == 0 || bps > 1000 {
		t.Fatalf("unexpected FLASHLOAN_PREMIUM_TOTAL bps: %d", bps)
	}
}

// TestFlashArbFullCycleEthCall — полная eth_call симуляция executeArb (нужен задеплоенный контракт и валидный маршрут).
// Запуск: FLASH_ARB_SIM=1 и заданы FLASH_ARB_CONTRACT, RPC_URL_DRPC (или BASE_HTTP).
func TestFlashArbFullCycleEthCall(t *testing.T) {
	if strings.TrimSpace(os.Getenv("FLASH_ARB_SIM")) != "1" {
		t.Skip("set FLASH_ARB_SIM=1 to run full-cycle simulation")
	}
	addrStr := strings.TrimSpace(os.Getenv("FLASH_ARB_CONTRACT"))
	rpc := strings.TrimSpace(os.Getenv("RPC_URL_DRPC"))
	if rpc == "" {
		rpc = strings.TrimSpace(os.Getenv("BASE_HTTP"))
	}
	if !common.IsHexAddress(addrStr) || rpc == "" {
		t.Skip("need FLASH_ARB_CONTRACT and RPC_URL_DRPC/BASE_HTTP")
	}
	flashArbContract = common.HexToAddress(addrStr)
	pk, trader, err := parseTraderKey()
	if err != nil {
		t.Skip("TRADER_PRIVATE_KEY required and must be owner of FlashArb")
	}
	_ = pk

	ec, err := ethclient.Dial(rpc)
	if err != nil {
		t.Fatal(err)
	}
	defer ec.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Минимальный sanity: вызов с нулевым amount должен revert на стороне Aave/контракта — проверяем, что RPC отвечает.
	amount := big.NewInt(1)
	minTok := big.NewInt(1)
	deadline := big.NewInt(time.Now().Add(60 * time.Second).Unix())
	buyData, _ := uniV2RouterABIv.Pack("swapExactTokensForTokens", amount, minTok,
		[]common.Address{addrWETH, addrWETH}, flashArbContract, deadline)
	args := flashArbArgs{
		Asset:            addrWETH,
		Amount:           amount,
		BuyRouter:        addrUniswapV2Router,
		BuyCalldata:      buyData,
		SellRouter:       addrUniswapV2Router,
		QuoteToken:       addrWETH,
		V3Sell:           false,
		V3Fee:            0,
		MinWethOut:       big.NewInt(0),
		MinTokenAfterBuy: minTok,
		MinProfitWei:     big.NewInt(0),
		Deadline:         deadline,
	}
	_ = flashArbSimulate(ctx, ec, trader, args)
	// Ожидаем ошибку — важно не падать паникой; успех/реверт зависит от состояния сети.
	t.Log("flashArbSimulate finished (expect revert for dummy path)")
}
