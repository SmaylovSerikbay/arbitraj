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

	ch := make(chan *types.Header, 8)
	sub, err := wsc.SubscribeNewHead(context.Background(), ch)
	if err != nil {
		log.Fatalf("eth_subscribe newHeads: %v", err)
	}
	defer sub.Unsubscribe()

	select {
	case err := <-sub.Err():
		log.Fatalf("подписка WS: %v", err)
	case h := <-ch:
		if h == nil {
			log.Fatal("получен пустой header")
		}
		fmt.Printf("OK  newHead: block=%d hash=%s\n", h.Number.Uint64(), h.Hash().Hex())
	case <-time.After(60 * time.Second):
		log.Fatal("таймаут 60s: нет newHead по WebSocket — проверьте порт WS и что нода отдаёт eth_subscribe")
	}

	fmt.Println("\nВсе проверки пройдены: HTTP + WS указывают на рабочий Base RPC.")
	fmt.Println("Убедитесь, что в .env те же BASE_HTTP / BASE_WSS, что и у ./arbitraj.")
}
