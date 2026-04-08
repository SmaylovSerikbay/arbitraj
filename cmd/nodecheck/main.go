// nodecheck: проверка BASE_HTTP и BASE_WSS из .env — та же связка, что у бота.
//
// Запуск из корня репозитория:
//
//	go run ./cmd/nodecheck
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/joho/godotenv"
)

const wantChainID int64 = 8453 // Base mainnet

func main() {
	if err := godotenv.Load(".env"); err != nil {
		log.Printf("предупреждение: .env не загружен (%v), берём переменные из окружения", err)
	}

	httpURL := os.Getenv("BASE_HTTP")
	wssURL := os.Getenv("BASE_WSS")
	if httpURL == "" {
		log.Fatal("BASE_HTTP пуст — укажите HTTP JSON-RPC своей ноды в .env")
	}

	ctx := context.Background()

	fmt.Println("=== HTTP:", httpURL, "===")
	hc, err := ethclient.DialContext(ctx, httpURL)
	if err != nil {
		log.Fatalf("HTTP dial: %v", err)
	}
	defer hc.Close()

	chainID, err := hc.ChainID(ctx)
	if err != nil {
		log.Fatalf("eth_chainId: %v", err)
	}
	if chainID.Int64() != wantChainID {
		log.Fatalf("chainId=%s — ожидался Base mainnet (%d). Проверьте, что нода — Base.", chainID, wantChainID)
	}
	fmt.Println("OK  chainId = 8453 (Base mainnet)")

	bn, err := hc.BlockNumber(ctx)
	if err != nil {
		log.Fatalf("eth_blockNumber: %v", err)
	}
	fmt.Printf("OK  latest block = %d\n", bn)

	weth := common.HexToAddress("0x4200000000000000000000000000000000000006")
	code, err := hc.CodeAt(ctx, weth, nil)
	if err != nil {
		log.Fatalf("eth_getCode WETH: %v", err)
	}
	if len(code) < 2 {
		log.Fatal("байткод WETH пуст — это не похоже на синхронизированную Base")
	}
	fmt.Printf("OK  WETH bytecode на ноде, len=%d\n", len(code))

	gp, err := hc.SuggestGasPrice(ctx)
	if err != nil {
		fmt.Printf("WARN eth_gasPrice / SuggestGasPrice: %v\n", err)
	} else {
		fmt.Printf("OK  gas price (hint) = %s wei\n", gp.String())
	}

	if wssURL == "" {
		fmt.Println("\nBASE_WSS пуст — WebSocket не проверялся (бот без WSS не подпишется на логи).")
		fmt.Println("Итог: HTTP к ноде в порядке.")
		return
	}

	fmt.Println("\n=== WebSocket:", wssURL, "===")
	wsc, err := ethclient.DialContext(ctx, wssURL)
	if err != nil {
		log.Fatalf("WSS dial: %v", err)
	}
	defer wsc.Close()

	// Убедиться, что по тому же WS работают обычные вызовы
	wsBN, err := wsc.BlockNumber(ctx)
	if err != nil {
		log.Fatalf("WS eth_blockNumber: %v", err)
	}
	fmt.Printf("OK  по WebSocket eth_blockNumber = %d\n", wsBN)

	bnHTTPStart, _ := hc.BlockNumber(ctx)

	ch := make(chan *types.Header, 16)
	subCtx := context.Background()
	sub, err := wsc.SubscribeNewHead(subCtx, ch)
	if err != nil {
		log.Fatalf("eth_subscribe newHeads: %v", err)
	}
	defer sub.Unsubscribe()

	wait := 90 * time.Second
	fmt.Printf("ожидание newHead по eth_subscribe (до %s)…\n", wait)

	var gotHead *types.Header
	timer := time.NewTimer(wait)
	defer timer.Stop()

loop:
	for {
		select {
		case err := <-sub.Err():
			log.Fatalf("подписка WS: %v", err)
		case h := <-ch:
			if h != nil {
				gotHead = h
				break loop
			}
		case <-timer.C:
			break loop
		}
	}

	bnHTTPEnd, _ := hc.BlockNumber(context.Background())

	if gotHead != nil {
		fmt.Printf("OK  newHead: block=%d hash=%s\n", gotHead.Number.Uint64(), gotHead.Hash().Hex())
		fmt.Println("\nВсе проверки пройдены: HTTP + WS (включая eth_subscribe newHeads).")
		fmt.Println("Убедитесь, что в .env те же BASE_HTTP / BASE_WSS, что и у ./arbitraj.")
		return
	}

	fmt.Println("FAIL: за отведённое время не пришёл ни один newHead по WebSocket.")
	fmt.Printf("     HTTP latest block: было %d, стало %d (за тот же интервал)\n", bnHTTPStart, bnHTTPEnd)
	fmt.Println("     Примечание: бот использует eth_subscribe на логи (Sync / PairCreated / Swap), не newHeads.")
	fmt.Println("     Если ./arbitraj печатает «Subscribed to Sync» и растёт счётчик событий — для арба WS достаточно.")
	if bnHTTPEnd > bnHTTPStart {
		fmt.Println()
		fmt.Println("Цепь по HTTP движется, а newHead по WS нет — типичные причины:")
		fmt.Println("  • В конфиге ноды для WS не включён API eth (например Reth: --http.ws, ws.api должен содержать eth).")
		fmt.Println("  • Другой порт: попробуйте ws://127.0.0.1:8545, если WS на том же порту, что HTTP.")
		fmt.Println("  • Баг/особенность клиента Base (op-reth/op-geth) для подписок — смотрите доки и флаги ws.")
	} else {
		fmt.Println()
		fmt.Println("Номер блока по HTTP почти не менялся — нода может догонять голову или зависла; сначала дождитесь синхронизации.")
	}
	os.Exit(1)
}
