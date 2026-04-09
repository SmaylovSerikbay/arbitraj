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
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

const dexScreenerTokenPairsBaseURL = "https://api.dexscreener.com/token-pairs/v1/base/"

// USDT на Base — исключаем из кандидатов «мемов» (стейбл).
var addrUSDTBase = common.HexToAddress("0xfde4C96c8593536E31F229EA8f37b2ADa2699bb2")

var (
	dexScreenerDiscoverEnabled = true
	dexScreenerPollInterval    = 10 * time.Minute
	dexScreenerMinLiqUSD       = 5_000.0
	dexScreenerTopN            = 50
	// Второй проход: дополнительные запросы token-pairs к топу по объёму (расширяет граф пар без ручного .env).
	dexScreenerSecondPassK = 40
	// Накопленные между тиками сиды (мемы/альты), чтобы следующий опрос DexScreener обходил шире.
	dexScreenerRollingSeedsMax = 120

	dexHTTPClient = &http.Client{Timeout: 60 * time.Second}

	dexRollingMu     sync.Mutex
	dexRollingSeeds  []common.Address
	dexRollingSeen   = make(map[string]struct{})
)

const dexScreenerUserAgent = "Mozilla/5.0 (compatible; arbitraj/1.0; +https://github.com/)"

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
	if v := strings.TrimSpace(os.Getenv("DEX_SCREENER_SECOND_PASS_K")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			dexScreenerSecondPassK = n
		}
	}
	if v := strings.TrimSpace(os.Getenv("DEX_SCREENER_ROLLING_MAX")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			dexScreenerRollingSeedsMax = n
		}
	}
	log.Printf("DEX_SCREENER: интервал %v | min liq $%.0f | топ %d токенов | 2-й проход %d сидов | rolling≤%d (token-pairs/v1/base/…)",
		dexScreenerPollInterval, dexScreenerMinLiqUSD, dexScreenerTopN, dexScreenerSecondPassK, dexScreenerRollingSeedsMax)
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
	dexRollingMu.Lock()
	for _, a := range dexRollingSeeds {
		add(a)
	}
	dexRollingMu.Unlock()
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
	req.Header.Set("User-Agent", dexScreenerUserAgent)
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

func mergePairScoresInto(seenPair map[string]struct{}, scores map[common.Address]float64, pairs []dexScreenerPair) {
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

func collectDexScreenerScores(ctx context.Context, seeds []common.Address, seenPair map[string]struct{}, scores map[common.Address]float64) error {
	var firstErr error
	for i, seed := range seeds {
		if seed == (common.Address{}) {
			continue
		}
		if i > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
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
		mergePairScoresInto(seenPair, scores, pairs)
	}
	return firstErr
}

func dexSeedKeySet(seeds []common.Address) map[string]struct{} {
	m := make(map[string]struct{}, len(seeds))
	for _, a := range seeds {
		if a == (common.Address{}) {
			continue
		}
		m[strings.ToLower(a.Hex())] = struct{}{}
	}
	return m
}

// secondPassSeeds — топ по объёму токены, по которым ещё не ходили как по сиду (второй проход расширяет охват).
func secondPassSeeds(scores map[common.Address]float64, already map[string]struct{}, k int) []common.Address {
	if k <= 0 || len(scores) == 0 {
		return nil
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
	var out []common.Address
	for _, x := range tv {
		if _, ok := already[strings.ToLower(x.addr.Hex())]; ok {
			continue
		}
		out = append(out, x.addr)
		if len(out) >= k {
			break
		}
	}
	return out
}

func dexScreenerMergeRolling(tv []tokenVol) {
	dexRollingMu.Lock()
	defer dexRollingMu.Unlock()
	for _, x := range tv {
		if x.addr == addrWETH || dexScreenerExcluded(x.addr) {
			continue
		}
		k := strings.ToLower(x.addr.Hex())
		if _, ok := dexRollingSeen[k]; ok {
			continue
		}
		dexRollingSeen[k] = struct{}{}
		dexRollingSeeds = append(dexRollingSeeds, x.addr)
		if len(dexRollingSeeds) > dexScreenerRollingSeedsMax {
			// сдвиг FIFO: убираем старые из слайса и seen (упрощённо — обрезаем хвост seen не чистим полностью)
			drop := dexRollingSeeds[0]
			dexRollingSeeds = dexRollingSeeds[1:]
			delete(dexRollingSeen, strings.ToLower(drop.Hex()))
		}
	}
}

// dexScreenerTopBaseTokensByVolume — токены (кроме стейблов/WETH) с наибольшим max(h24 volume) по парам,
// где chainId=base и liquidity ≥ minLiqUSD. Два прохода по сидам + rolling-сид без ручного WETH_EXTRA.
func dexScreenerTopBaseTokensByVolume(ctx context.Context) ([]common.Address, error) {
	scores := make(map[common.Address]float64)
	seenPair := make(map[string]struct{})

	seeds1 := dexScreenerSeedAddresses()
	firstErr := collectDexScreenerScores(ctx, seeds1, seenPair, scores)

	seen := dexSeedKeySet(seeds1)
	seeds2 := secondPassSeeds(scores, seen, dexScreenerSecondPassK)
	if len(seeds2) > 0 {
		_ = collectDexScreenerScores(ctx, seeds2, seenPair, scores)
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
	// Подмешиваем лидеров в rolling для следующих тиков (полная автоматизация охвата).
	nRoll := dexScreenerRollingSeedsMax
	if nRoll > len(tv) {
		nRoll = len(tv)
	}
	if nRoll > 0 {
		dexScreenerMergeRolling(tv[:nRoll])
	}

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
		setTokenDecimalsIfMissing(addr, 18)
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
