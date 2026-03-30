package main

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/ethclient"
)

var (
	statsRPCOverviewMu sync.RWMutex
	statsRPCOverview   = "—"
)

func setStatsRPCOverview(s string) {
	statsRPCOverviewMu.Lock()
	statsRPCOverview = s
	statsRPCOverviewMu.Unlock()
}

func getStatsRPCOverview() string {
	statsRPCOverviewMu.RLock()
	s := statsRPCOverview
	statsRPCOverviewMu.RUnlock()
	return s
}

// Счётчик неудачных eth_call / SuggestGasPrice после исчерпания ретраев (для STATS).
var rpcQuoteHardFailures uint64

func bumpRPCQuoteHardFailures() {
	atomic.AddUint64(&rpcQuoteHardFailures, 1)
}

func RPCQuoteHardFailures() uint64 {
	return atomic.LoadUint64(&rpcQuoteHardFailures)
}

func isRetryableRPCErr(err error) bool {
	if err == nil {
		return false
	}
	m := strings.ToLower(err.Error())
	return strings.Contains(m, "429") ||
		strings.Contains(m, "too many requests") ||
		strings.Contains(m, "rate limit") ||
		strings.Contains(m, "timeout") ||
		strings.Contains(m, "temporar") ||
		strings.Contains(m, "connection reset") ||
		strings.Contains(m, "eof") ||
		strings.Contains(m, "compute units") ||
		strings.Contains(m, "throughput") ||
		strings.Contains(m, "capacity") && strings.Contains(m, "exceeded") ||
		strings.Contains(m, "503") ||
		strings.Contains(m, "502") ||
		strings.Contains(m, "504")
}

// isAlchemyThroughputNoise — не печатать в лог как «красный» шум при обрыве подписки.
func isAlchemyThroughputNoise(err error) bool {
	if err == nil {
		return false
	}
	m := strings.ToLower(err.Error())
	return strings.Contains(m, "compute units") ||
		strings.Contains(m, "throughput") ||
		strings.Contains(m, "capacity exceeded") ||
		strings.Contains(m, "exceeded") && strings.Contains(m, "request") ||
		strings.Contains(m, "429") ||
		strings.Contains(m, "too many requests")
}

// callContractRetry — до 3 попыток, пауза 1 с; без логов на промежуточных ошибках.
func callContractRetry(ctx context.Context, ec *ethclient.Client, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
	if ec == nil {
		return nil, errors.New("nil eth client")
	}
	var last error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Second):
			}
		}
		out, err := ec.CallContract(ctx, msg, blockNumber)
		if err == nil {
			return out, nil
		}
		last = err
		if !isRetryableRPCErr(err) {
			return out, err
		}
	}
	bumpRPCQuoteHardFailures()
	return nil, last
}

// suggestGasPriceRetry — то же для газового оракула.
func suggestGasPriceRetry(ctx context.Context, ec *ethclient.Client) (*big.Int, error) {
	if ec == nil {
		return nil, errors.New("nil eth client")
	}
	var last error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Second):
			}
		}
		gp, err := ec.SuggestGasPrice(ctx)
		if err == nil && gp != nil && gp.Sign() > 0 {
			return gp, nil
		}
		last = err
		if err != nil && !isRetryableRPCErr(err) {
			return nil, err
		}
	}
	bumpRPCQuoteHardFailures()
	return nil, last
}
