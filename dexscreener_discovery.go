// DexScreener Discovery: агрегация пар с token-pairs/v1/base/{token} (официальный путь; latest/dex/tokens/… даёт 404).
// Топ-N токенов по max(volume h24) среди пар с liquidity ≥ порога; добавление в мониторинг как WETH_EXTRA_TOKENS.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

const dexScreenerTokenPairsBaseURL = "https://api.dexscreener.com/token-pairs/v1/base/"

// USDT на Base — исключаем из кандидатов «мемов» (стейбл).
var addrUSDTBase = common.HexToAddress("0xfde4C96c8593536E31F229EA8f37b2ADa2699bb2")

var (
	dexScreenerDiscoverEnabled = true
	dexScreenerPollInterval    = time.Hour
	dexScreenerMinLiqUSD       = 10_000.0
	dexScreenerTopN            = 30

	dexHTTPClient = &http.Client{Timeout: 60 * time.Second}
)

func loadDexScreenerDiscoverSettings() {
	s := strings.TrimSpace(strings.ToLower(os.Getenv("DEX_SCREENER_DISCOVER")))
	if s == "0" || s == "false" || s == "off" {
		dexScreenerDiscoverEnabled = false
		log.Printf("DEX_SCREENER_DISCOVER выключен — без подкачки списка с DexScreener")
		return
	}
	dexScreenerDiscoverEnabled = true
	if v := strings.TrimSpace(os.Getenv("DEX_SCREENER_POLL_SEC")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			dexScreenerPollInterval = time.Duration(n) * time.Second
		}
	}
	if v := strings.TrimSpace(os.Getenv("DEX_SCREENER_MIN_LIQ_USD")); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			dexScreenerMinLiqUSD = f
		}
	}
	if v := strings.TrimSpace(os.Getenv("DEX_SCREENER_TOP_N")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			dexScreenerTopN = n
		}
	}
	log.Printf("DEX_SCREENER: интервал %v | min liq $%.0f | топ %d токенов (token-pairs/v1/base/…)",
		dexScreenerPollInterval, dexScreenerMinLiqUSD, dexScreenerTopN)
}

func dexScreenerSeedAddresses() []common.Address {
	seen := make(map[common.Address]struct{})
	var out []common.Address
	add := func(a common.Address) {
		if a == (common.Address{}) {
			return
		}
		if _, ok := seen[a]; ok {
			return
		}
		seen[a] = struct{}{}
		out = append(out, a)
	}
	add(addrWETH)
	add(addrUSDC)
	add(addrUSDbC)
	add(addrDAI)
	for _, q := range defaultQuoteTokens {
		add(q.addr)
	}
	for _, q := range speculatorQuoteTokens {
		add(q.addr)
	}
	return out
}

func dexScreenerExcluded(addr common.Address) bool {
	switch addr {
	case addrWETH, addrUSDC, addrUSDbC, addrDAI, addrUSDTBase:
		return true
	default:
		return false
	}
}

type dexScreenerPair struct {
	ChainID     string `json:"chainId"`
	PairAddress string `json:"pairAddress"`
	Volume  struct {
		H24 float64 `json:"h24"`
	} `json:"volume"`
	Liquidity struct {
		USD float64 `json:"usd"`
	} `json:"liquidity"`
	BaseToken struct {
		Address string `json:"address"`
	} `json:"baseToken"`
	QuoteToken struct {
		Address string `json:"address"`
	} `json:"quoteToken"`
}

func fetchDexScreenerPairsForToken(ctx context.Context, token common.Address) ([]dexScreenerPair, error) {
	url := dexScreenerTokenPairsBaseURL + token.Hex()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := dexHTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("dexscreener %s: HTTP %s", url, resp.Status)
	}
	var pairs []dexScreenerPair
	if err := json.Unmarshal(body, &pairs); err != nil {
		return nil, fmt.Errorf("dexscreener json: %w", err)
	}
	return pairs, nil
}

type tokenVol struct {
	addr common.Address
	vol  float64
}

// dexScreenerTopBaseTokensByVolume — токены (кроме стейблов/WETH) с наибольшим max(h24 volume) по парам,
// где chainId=base и liquidity ≥ minLiqUSD. Данные собираются с нескольких «якорных» токенов API (по 30 пар на запрос).
func dexScreenerTopBaseTokensByVolume(ctx context.Context) ([]common.Address, error) {
	scores := make(map[common.Address]float64)
	seenPair := make(map[string]struct{})

	seeds := dexScreenerSeedAddresses()
	var firstErr error
	for i, seed := range seeds {
		if i > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(200 * time.Millisecond):
			}
		}
		pairs, err := fetchDexScreenerPairsForToken(ctx, seed)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		for _, p := range pairs {
			if !strings.EqualFold(p.ChainID, "base") {
				continue
			}
			if p.Liquidity.USD < dexScreenerMinLiqUSD {
				continue
			}
			a := common.HexToAddress(strings.TrimSpace(p.BaseToken.Address))
			b := common.HexToAddress(strings.TrimSpace(p.QuoteToken.Address))
			if a == (common.Address{}) || b == (common.Address{}) {
				continue
			}
			pk := strings.ToLower(strings.TrimSpace(p.PairAddress))
			if pk == "" {
				pk = strings.ToLower(strings.TrimSpace(p.BaseToken.Address) + "|" + strings.TrimSpace(p.QuoteToken.Address))
			}
			if _, dup := seenPair[pk]; dup {
				continue
			}
			seenPair[pk] = struct{}{}
			vol := p.Volume.H24
			if !dexScreenerExcluded(a) {
				if vol > scores[a] {
					scores[a] = vol
				}
			}
			if !dexScreenerExcluded(b) {
				if vol > scores[b] {
					scores[b] = vol
				}
			}
		}
	}
	if len(scores) == 0 && firstErr != nil {
		return nil, firstErr
	}
	if len(scores) == 0 {
		return nil, errors.New("dexscreener: нет пар после фильтра")
	}
	tv := make([]tokenVol, 0, len(scores))
	for a, v := range scores {
		tv = append(tv, tokenVol{addr: a, vol: v})
	}
	sort.Slice(tv, func(i, j int) bool {
		if tv[i].vol != tv[j].vol {
			return tv[i].vol > tv[j].vol
		}
		return strings.Compare(tv[i].addr.Hex(), tv[j].addr.Hex()) < 0
	})
	n := dexScreenerTopN
	if n > len(tv) {
		n = len(tv)
	}
	out := make([]common.Address, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, tv[i].addr)
	}
	return out, nil
}

func dexScreenerDiscoverOnce(ctx context.Context, ec *ethclient.Client, reg *registry, bump chan struct{}) {
	if ec == nil {
		return
	}
	top, err := dexScreenerTopBaseTokensByVolume(ctx)
	if err != nil {
		log.Printf("[DEX] Ошибка обновления: %v", err)
		return
	}
	v3Before := reg.v3PoolCount()
	syncBefore := reg.poolAddressCount()
	newMonitored := 0
	for _, addr := range top {
		wasMonitored := reg.isQuoteMonitored(addr)
		if wasMonitored {
			continue
		}
		if _, ok := tokenDecimals[addr]; !ok {
			tokenDecimals[addr] = 18
		}
		label := "WETH/" + addr.Hex()[:10] + "…"
		_, err := reg.tryRegisterWETHPair(ctx, ec, addr, label, autoMinWethPerPool)
		if err != nil && !isRPCThroughputErr(err) {
			log.Printf("[DEX] регистрация %s: %v", label, err)
		}
		reg.registerV3QuotePools(ctx, ec, addr, label)
		if reg.isQuoteMonitored(addr) {
			newMonitored++
		}
	}
	v3After := reg.v3PoolCount()
	syncAfter := reg.poolAddressCount()
	if bump != nil && (v3After != v3Before || syncAfter != syncBefore) {
		select {
		case bump <- struct{}{}:
		default:
		}
	}
	log.Printf("[DEX] Обновлен список токенов: +%d новых пар в мониторинге.", newMonitored)
}

func dexScreenerDiscoverLoop(ctx context.Context, ec *ethclient.Client, reg *registry, bump chan struct{}) {
	if !dexScreenerDiscoverEnabled {
		return
	}
	t := time.NewTicker(dexScreenerPollInterval)
	defer t.Stop()
	// Первый прогон вскоре после старта, дальше — по тикеру.
	dexScreenerDiscoverOnce(ctx, ec, reg, bump)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			dexScreenerDiscoverOnce(ctx, ec, reg, bump)
		}
	}
}
