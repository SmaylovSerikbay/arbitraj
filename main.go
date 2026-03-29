// Atomic arbitrage monitor: Uniswap V2 vs SushiSwap V2 (Base), paper trading only.
//
// Мульти-пары WETH/X: конфиг токенов + авто-добавление по PairCreated с обеих фабрик.
// Подписка Sync — один eth_subscribe на все известные пул-адреса (перезапуск при новых парах).
//
// Бумажный режим: нет on-chain проверок на honeypot/налог 99%% — смотрите только как справочную
// картину; в бой без симуляции sell и аудита контракта нельзя.
//
// Base RPC: BASE_HTTP / BASE_WSS, MIN_NET_PROFIT_PCT, NOTIONAL_USD (размер «банка» для AMM-симуляции),
// ETH_USD_HINT — грубая цена ETH в USD для перевода $ → WETH и для оценки газа в USD (не оракул).
// Потенциал в логе — после двух свопов x*y=k (комиссия пула 0.3%), минус газ: SuggestGasPrice×GAS_LIMIT_ARB (кэш 12s).
// NET(sim) > 10% после AMM+газ → [LOW_LIQUIDITY] (подозрение на тонкий пул).
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"math/big"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/joho/godotenv"
)

// --- Base mainnet ---
var (
	addrUniswapV2Factory = common.HexToAddress("0x8909Dc15e40173Ff4699343b6eB8132c65e18eC6")
	addrSushiV2Factory   = common.HexToAddress("0x71524b4f93c58fcbf659783284e38825f0622859")
	addrWETH             = common.HexToAddress("0x4200000000000000000000000000000000000006")
	// Ликвидные и примеры «грязных» пар — обновляйте мемы с DexScreener Base / Trending.
	addrUSDC  = common.HexToAddress("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913")
	addrUSDbC = common.HexToAddress("0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA")
	addrDAI   = common.HexToAddress("0x50c5725949A6F0c72E6C4a641F24049A917DB0Cb")
	// DEGEN: уточняйте на DexScreener (ниже — часто цитируемый контракт; замените при расхождении).
	addrDEGEN = common.HexToAddress("0x4ed4E8615B61adEe945ee8F195E2B50c3Cf535d6")
	addrTOSHI = common.HexToAddress("0xAC1Bd2486aAf3B5C0fc3Fd868558b082a531B2B4")
	addrBRETT = common.HexToAddress("0x532f27101965dd16442E59d40670FaF5eBB142E4")
	addrAERO  = common.HexToAddress("0x940181a94A35A4569E4529A3CDfB74e38FD98631")
	addrcbETH = common.HexToAddress("0x2Ae3F1Ec7F1F5012CFEab0185bfc7aa3cf0DEc22")
)

const (
	reconnectMinDelay     = 2 * time.Second
	reconnectMaxDelay     = 60 * time.Second
	heartbeatInterval     = 1 * time.Minute
	statsSummaryInterval = 10 * time.Minute
)

// Фабрика: getPair + PairCreated (одинаковый ABI Uni/Sushi V2).
const factoryABIJSON = `[{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"token0","type":"address"},{"indexed":true,"internalType":"address","name":"token1","type":"address"},{"indexed":false,"internalType":"address","name":"pair","type":"address"},{"indexed":false,"internalType":"uint256","name":"","type":"uint256"}],"name":"PairCreated","type":"event"},{"inputs":[{"internalType":"address","name":"tokenA","type":"address"},{"internalType":"address","name":"tokenB","type":"address"}],"name":"getPair","outputs":[{"internalType":"address","name":"pair","type":"address"}],"stateMutability":"view","type":"function"}]`

const uniswapV2PairABI = `[{"constant":true,"inputs":[],"name":"token0","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getReserves","outputs":[{"internalType":"uint112","name":"_reserve0","type":"uint112"},{"internalType":"uint112","name":"_reserve1","type":"uint112"},{"internalType":"uint32","name":"_blockTimestampLast","type":"uint32"}],"payable":false,"stateMutability":"view","type":"function"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint112","name":"reserve0","type":"uint112"},{"indexed":false,"internalType":"uint112","name":"reserve1","type":"uint112"}],"name":"Sync","type":"event"}]`

var (
	factoryParsedABI abi.ABI
	pairParsedABI    abi.ABI
	syncTopic        common.Hash
	pairCreatedTopic common.Hash

	pow10tab [37]*big.Int

	minNetProfitThreshold        *big.Rat
	statsOpportunityThreshold    *big.Rat // net-% ≥ этого попадает в счётчик «оппортьюнити» (по умолчанию 0.5%)
	spamPrintMode                bool     // MIN_NET_PROFIT_PCT ≤ 0
	ratSwapFeesPct = big.NewRat(6, 10)
	ratGasUSD      = big.NewRat(5, 100) // фоллбэк, если RPC недоступен
	ratNotional    = big.NewRat(10, 1)   // перезапись в loadNotionalAndEthHint (по умолчанию $10)
	ethUsdHint     = big.NewRat(2500, 1) // подсказка для $ → WETH; перезапись из ETH_USD_HINT
	rat100         = big.NewRat(100, 1)

	notionalUSDForDisplay float64 = 10 // для STATS projection, синхрон с NOTIONAL_USD

	// Динамический газ: SuggestGasPrice × лимит (2 свопа), кэш ~12s; клиент выставляет сессия.
	gasOracleMu           sync.Mutex
	gasOracleClient       *ethclient.Client
	gasCostUSDCache       *big.Rat
	gasOracleUpdated      time.Time
	gasOracleTTL          = 12 * time.Second
	gasLimitTwoSwaps int64 = 300_000 // ~2 свопа на Base; переопределение GAS_LIMIT_ARB

	lowLiqPrintMu sync.Mutex
	lowLiqPrintAt map[string]time.Time // антиспам для [LOW_LIQUIDITY]
)

const lowLiquiditySuspiciousNetPct = 10.0 // net% после AMM+газ — выше считаем «тонкой» ликвидностью

func init() {
	var err error
	factoryParsedABI, err = abi.JSON(strings.NewReader(factoryABIJSON))
	if err != nil {
		panic(err)
	}
	pairParsedABI, err = abi.JSON(strings.NewReader(uniswapV2PairABI))
	if err != nil {
		panic(err)
	}
	syncTopic = crypto.Keccak256Hash([]byte("Sync(uint112,uint112)"))
	pairCreatedTopic = crypto.Keccak256Hash([]byte("PairCreated(address,address,address,uint256)"))
	for i := range pow10tab {
		pow10tab[i] = new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(i)), nil)
	}
	minNetProfitThreshold = big.NewRat(5, 10)
	statsOpportunityThreshold = big.NewRat(5, 10)
}

// Счётчики событий WebSocket (atomic).
var (
	syncEventsParsed        uint64
	pairCreatedLogsReceived uint64
)

// Накопительная статистика за время работы процесса (переживает переподключения RPC).
type sessionStats struct {
	mu              sync.Mutex
	started         time.Time
	opportunities   int64   // net-% ≥ statsOpportunityThreshold
	totalPotential  float64 // сумма $ potential
	bestPotential   float64
	bestPairLabel   string
}

var appStats sessionStats

func (s *sessionStats) recordOpportunity(potentialUSD float64, pairLabel string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.opportunities++
	s.totalPotential += potentialUSD
	if potentialUSD > s.bestPotential {
		s.bestPotential = potentialUSD
		s.bestPairLabel = pairLabel
	}
}

func formatWithCommas(n uint64) string {
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}
	lead := len(s) % 3
	if lead == 0 {
		lead = 3
	}
	var b strings.Builder
	b.WriteString(s[:lead])
	for i := lead; i < len(s); i += 3 {
		b.WriteByte(',')
		b.WriteString(s[i : i+3])
	}
	return b.String()
}

func printStatsSummaryBlock() {
	totalEv := atomic.LoadUint64(&syncEventsParsed) + atomic.LoadUint64(&pairCreatedLogsReceived)
	appStats.mu.Lock()
	opps := appStats.opportunities
	total := appStats.totalPotential
	best := appStats.bestPotential
	bestL := appStats.bestPairLabel
	start := appStats.started
	appStats.mu.Unlock()
	dur := time.Since(start).Round(time.Minute)
	h := int(dur.Hours())
	m := int(dur.Minutes()) % 60
	durLabel := fmt.Sprintf("%dh %dm", h, m)
	if h == 0 && m == 0 {
		durLabel = "<1m"
	} else if h == 0 {
		durLabel = fmt.Sprintf("%dm", m)
	}
	proj := 0.0
	if notionalUSDForDisplay > 0 {
		proj = (total / notionalUSDForDisplay) * 100.0
	}
	fmt.Println("================ STATS (" + durLabel + " RUN) ================")
	fmt.Printf("Total Events Scanned: %s\n", formatWithCommas(totalEv))
	fmt.Printf("Opportunities Found: %d  (net ≥ %s%%)\n", opps, statsOpportunityThreshold.FloatString(2))
	fmt.Printf("Total Potential Profit: $%.2f\n", total)
	if best > 0 {
		fmt.Printf("Best Single Trade: $%.2f (%s)\n", best, bestL)
	} else {
		fmt.Printf("Best Single Trade: —\n")
	}
	fmt.Printf("Current Bank Projection: +%.1f%%\n", proj)
	fmt.Println("====================================================")
}

func tenPowU8(d uint8) *big.Int {
	if int(d) < len(pow10tab) {
		return pow10tab[d]
	}
	return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(d)), nil)
}

// --- decimals: известные токены Base; неизвестным считаем 18 (мемы часто 18). Для боя нужен decimals() on-chain.
var tokenDecimals = map[common.Address]uint8{
	addrWETH:  18,
	addrUSDC:  6,
	addrUSDbC: 6,
	addrDAI:   18,
	addrDEGEN: 18,
	addrTOSHI: 18,
	addrBRETT: 18,
	addrAERO:  18,
	addrcbETH: 18,
}

func decimalsOf(a common.Address) uint8 {
	if d, ok := tokenDecimals[a]; ok {
		return d
	}
	return 18
}

type quoteToken struct {
	addr   common.Address
	symbol string
}

// Стартовый список WETH-пар (эталон USDC + ликвидность + примеры; мемы меняйте по рынку).
var defaultQuoteTokens = []quoteToken{
	{addrUSDC, "WETH/USDC"},
	{addrUSDbC, "WETH/USDbC"},
	{addrDAI, "WETH/DAI"},
	{addrDEGEN, "WETH/DEGEN"},
	{addrTOSHI, "WETH/TOSHI"},
	{addrBRETT, "WETH/BRETT"},
	{addrAERO, "WETH/AERO"},
	{addrcbETH, "WETH/cbETH"},
}

// --- Pair binding ---
type UniswapV2Pair struct {
	address  common.Address
	contract *bind.BoundContract
}

func NewUniswapV2Pair(address common.Address, backend bind.ContractBackend) (*UniswapV2Pair, error) {
	contract := bind.NewBoundContract(address, pairParsedABI, backend, backend, backend)
	return &UniswapV2Pair{address: address, contract: contract}, nil
}

func unpackBigInt(v interface{}) (*big.Int, error) {
	switch x := v.(type) {
	case *big.Int:
		if x == nil {
			return nil, errors.New("nil *big.Int")
		}
		return x, nil
	default:
		r, ok := abi.ConvertType(v, new(*big.Int)).(*big.Int)
		if !ok || r == nil {
			return nil, errors.New("not *big.Int")
		}
		return r, nil
	}
}

func unpackUint32(v interface{}) (uint32, error) {
	switch x := v.(type) {
	case uint32:
		return x, nil
	case *uint32:
		if x == nil {
			return 0, errors.New("nil *uint32")
		}
		return *x, nil
	case *big.Int:
		if x == nil {
			return 0, errors.New("nil timestamp")
		}
		return uint32(x.Uint64()), nil
	default:
		return 0, fmt.Errorf("timestamp type %T", v)
	}
}

func (p *UniswapV2Pair) GetReserves(opts *bind.CallOpts) (reserve0, reserve1 *big.Int, blockTimestampLast uint32, err error) {
	var out []interface{}
	err = p.contract.Call(opts, &out, "getReserves")
	if err != nil {
		return nil, nil, 0, err
	}
	if len(out) != 3 {
		return nil, nil, 0, errors.New("getReserves: unexpected output length")
	}
	r0, err := unpackBigInt(out[0])
	if err != nil {
		return nil, nil, 0, fmt.Errorf("getReserves reserve0: %w", err)
	}
	r1, err := unpackBigInt(out[1])
	if err != nil {
		return nil, nil, 0, fmt.Errorf("getReserves reserve1: %w", err)
	}
	ts, err := unpackUint32(out[2])
	if err != nil {
		return nil, nil, 0, fmt.Errorf("getReserves blockTimestampLast: %w", err)
	}
	return r0, r1, ts, nil
}

type UniswapV2PairSync struct {
	Raw      types.Log
	Reserve0 *big.Int
	Reserve1 *big.Int
}

func (p *UniswapV2Pair) ParseSync(log types.Log) (*UniswapV2PairSync, error) {
	ev := new(UniswapV2PairSync)
	ev.Raw = log
	if err := p.contract.UnpackLog(ev, "Sync", log); err != nil {
		return nil, err
	}
	return ev, nil
}

// --- Реестр пар Uni vs Sushi для одного (token0,token1) ---
type trackedPair struct {
	label        string
	token0       common.Address
	token1       common.Address
	dec0, dec1   uint8
	uniAddr      common.Address
	sushiAddr    common.Address
	uniBind      *UniswapV2Pair
	sushiBind    *UniswapV2Pair
	uniR0        *big.Int
	uniR1        *big.Int
	sushiR0      *big.Int
	sushiR1      *big.Int
	haveUni      uint32
	haveSushi    uint32
	mu           sync.Mutex
	registeredAt time.Time

	statMu          sync.Mutex
	lastOppSampleAt time.Time // не считать одну и ту же «вилку» тысячи раз на потоке Sync
}

func sortTokens(a, b common.Address) (t0, t1 common.Address) {
	if bytes.Compare(a[:], b[:]) < 0 {
		return a, b
	}
	return b, a
}

func pairKeyString(t0, t1 common.Address) string {
	return strings.ToLower(t0.Hex()) + "|" + strings.ToLower(t1.Hex())
}

type addrRef struct {
	tp  *trackedPair
	uni bool
}

type registry struct {
	mu    sync.RWMutex
	byKey map[string]*trackedPair
	refs  map[common.Address]*addrRef
}

func newRegistry() *registry {
	return &registry{
		byKey: make(map[string]*trackedPair),
		refs:  make(map[common.Address]*addrRef),
	}
}

func (r *registry) pairCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.byKey)
}

func (r *registry) poolAddressCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.refs)
}

func (r *registry) snapshotAddresses() []common.Address {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]common.Address, 0, len(r.refs))
	for a := range r.refs {
		out = append(out, a)
	}
	return out
}

func (r *registry) getRef(addr common.Address) *addrRef {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.refs[addr]
}

// getAmountOut — constant product + комиссия пула 0.3% (997/1000). Невалидные входы → 0.
func getAmountOut(amountIn, reserveIn, reserveOut *big.Int) *big.Int {
	if amountIn == nil || reserveIn == nil || reserveOut == nil {
		return big.NewInt(0)
	}
	if amountIn.Sign() <= 0 || reserveIn.Sign() <= 0 || reserveOut.Sign() <= 0 {
		return big.NewInt(0)
	}
	amountInWithFee := new(big.Int).Mul(amountIn, big.NewInt(997))
	numerator := new(big.Int).Mul(amountInWithFee, reserveOut)
	denominator := new(big.Int).Add(new(big.Int).Mul(reserveIn, big.NewInt(1000)), amountInWithFee)
	if denominator.Sign() == 0 {
		return big.NewInt(0)
	}
	return new(big.Int).Div(numerator, denominator)
}

func setGasOracleClient(c *ethclient.Client) {
	gasOracleMu.Lock()
	defer gasOracleMu.Unlock()
	gasOracleClient = c
	gasCostUSDCache = nil
	gasOracleUpdated = time.Time{}
}

func arbGasLimitWeiMultiplier() *big.Int {
	s := strings.TrimSpace(os.Getenv("GAS_LIMIT_ARB"))
	if s != "" {
		if v, err := strconv.ParseInt(s, 10, 64); err == nil && v > 0 {
			return big.NewInt(v)
		}
	}
	return big.NewInt(gasLimitTwoSwaps)
}

// dynamicGasUSD: gasPrice × GAS_LIMIT_ARB × ETH_USD_HINT / 1e18; при отсутствии RPC — ratGasUSD.
func dynamicGasUSD() *big.Rat {
	gasOracleMu.Lock()
	c := gasOracleClient
	cache := gasCostUSDCache
	upd := gasOracleUpdated
	gasOracleMu.Unlock()
	if c == nil {
		return new(big.Rat).Set(ratGasUSD)
	}
	if cache != nil && time.Since(upd) < gasOracleTTL {
		return new(big.Rat).Set(cache)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	gp, err := c.SuggestGasPrice(ctx)
	if err != nil || gp == nil || gp.Sign() <= 0 {
		gp = big.NewInt(20_000_000) // ~0.02 gwei фоллбэк
	}
	gasWei := new(big.Int).Mul(gp, arbGasLimitWeiMultiplier())
	usd := new(big.Rat).SetFrac(gasWei, tenPowU8(18))
	usd.Mul(usd, ethUsdHint)
	gasOracleMu.Lock()
	gasCostUSDCache = new(big.Rat).Set(usd)
	gasOracleUpdated = time.Now()
	gasOracleMu.Unlock()
	return new(big.Rat).Set(usd)
}

func shouldPrintLowLiquidity(pairLabel string) bool {
	lowLiqPrintMu.Lock()
	defer lowLiqPrintMu.Unlock()
	if lowLiqPrintAt == nil {
		lowLiqPrintAt = make(map[string]time.Time)
	}
	if t, ok := lowLiqPrintAt[pairLabel]; ok && time.Since(t) < 3*time.Second {
		return false
	}
	lowLiqPrintAt[pairLabel] = time.Now()
	return true
}

func ratFloorInt(r *big.Rat) *big.Int {
	if r == nil || r.Sign() <= 0 {
		return big.NewInt(0)
	}
	return new(big.Int).Div(r.Num(), r.Denom())
}

// notionalUSDToWETHWei: USD * 1e18 / ETH_USD_HINT
func notionalUSDToWETHWei() *big.Int {
	num := new(big.Rat).Mul(ratNotional, new(big.Rat).SetInt(tenPowU8(18)))
	num.Quo(num, ethUsdHint)
	return ratFloorInt(num)
}

// Симуляция: WETH -> X на «buy» пуле, X -> WETH на «sell» пуле. buySushi=true — сначала Sushi.
func simulateWETHRoundTrip(
	wethIsToken0 bool,
	buyR0, buyR1, sellR0, sellR1 *big.Int,
	wethIn *big.Int,
) (wethBack *big.Int, ok bool) {
	if wethIn == nil || wethIn.Sign() <= 0 {
		return nil, false
	}
	var xOut *big.Int
	if wethIsToken0 {
		xOut = getAmountOut(wethIn, buyR0, buyR1)
		if xOut.Sign() <= 0 {
			return nil, false
		}
		wethBack = getAmountOut(xOut, sellR1, sellR0)
	} else {
		xOut = getAmountOut(wethIn, buyR1, buyR0)
		if xOut.Sign() <= 0 {
			return nil, false
		}
		wethBack = getAmountOut(xOut, sellR0, sellR1)
	}
	if wethBack == nil || wethBack.Sign() <= 0 {
		return nil, false
	}
	return wethBack, true
}

func wethSideReserve(r0, r1 *big.Int, wethIsToken0 bool) *big.Int {
	if wethIsToken0 {
		return new(big.Int).Set(r0)
	}
	return new(big.Int).Set(r1)
}

// Цена token1 за 1 token0 (как в пуле): (r1 * 10^dec0) / (r0 * 10^dec1).
func ratPriceToken1PerToken0(r0, r1 *big.Int, dec0, dec1 uint8, out *big.Rat) {
	if r0.Sign() == 0 {
		out.SetInt64(0)
		return
	}
	num := new(big.Int).Mul(r1, tenPowU8(dec0))
	den := new(big.Int).Mul(r0, tenPowU8(dec1))
	out.SetFrac(num, den)
}

func (tp *trackedPair) evaluateAndMaybePrint() {
	if atomic.LoadUint32(&tp.haveUni) == 0 || atomic.LoadUint32(&tp.haveSushi) == 0 {
		return
	}
	if tp.token0 != addrWETH && tp.token1 != addrWETH {
		return
	}
	wethIsT0 := tp.token0 == addrWETH

	tp.mu.Lock()
	var pUni, pSushi big.Rat
	ratPriceToken1PerToken0(tp.uniR0, tp.uniR1, tp.dec0, tp.dec1, &pUni)
	ratPriceToken1PerToken0(tp.sushiR0, tp.sushiR1, tp.dec0, tp.dec1, &pSushi)
	u0 := new(big.Int).Set(tp.uniR0)
	u1 := new(big.Int).Set(tp.uniR1)
	s0 := new(big.Int).Set(tp.sushiR0)
	s1 := new(big.Int).Set(tp.sushiR1)
	tp.mu.Unlock()

	if pUni.Cmp(&pSushi) == 0 {
		return
	}
	var minP, maxP big.Rat
	var buyName, sellName string
	if pUni.Cmp(&pSushi) > 0 {
		maxP.Set(&pUni)
		minP.Set(&pSushi)
		buyName, sellName = "SushiSwap", "UniswapV2"
	} else {
		maxP.Set(&pSushi)
		minP.Set(&pUni)
		buyName, sellName = "UniswapV2", "SushiSwap"
	}
	var diff, spreadQuo, spreadPct, afterFees big.Rat
	diff.Sub(&maxP, &minP)
	if minP.Sign() == 0 {
		return
	}
	spreadQuo.Quo(&diff, &minP)
	spreadPct.Mul(&spreadQuo, rat100)
	afterFees.Sub(&spreadPct, ratSwapFeesPct)

	// «Mid» потенциал (как раньше) — только справочно, без слиппеджа AMM.
	gasUSD := dynamicGasUSD()
	var midGross, midBeforeGas, midPotNet, midNetPct big.Rat
	midGross.Mul(ratNotional, &afterFees)
	midBeforeGas.Quo(&midGross, rat100)
	midPotNet.Sub(&midBeforeGas, gasUSD)
	if midPotNet.Sign() <= 0 {
		return
	}
	var midNetQuo big.Rat
	midNetQuo.Quo(&midPotNet, ratNotional)
	midNetPct.Mul(&midNetQuo, rat100)

	var buyR0, buyR1, sellR0, sellR1 *big.Int
	if buyName == "SushiSwap" {
		buyR0, buyR1, sellR0, sellR1 = s0, s1, u0, u1
	} else {
		buyR0, buyR1, sellR0, sellR1 = u0, u1, s0, s1
	}

	wethIn := notionalUSDToWETHWei()
	if wethIn.Sign() <= 0 {
		return
	}
	wethSideBuy := wethSideReserve(buyR0, buyR1, wethIsT0)
	maxWethIn := new(big.Int).Div(wethSideBuy, big.NewInt(10))
	if maxWethIn.Sign() > 0 && wethIn.Cmp(maxWethIn) > 0 {
		wethIn = new(big.Int).Set(maxWethIn)
	}

	wethBack, simOK := simulateWETHRoundTrip(wethIsT0, buyR0, buyR1, sellR0, sellR1, wethIn)
	if !simOK {
		return
	}
	profitWei := new(big.Int).Sub(wethBack, wethIn)
	simProfitUSD := new(big.Rat).Mul(new(big.Rat).SetFrac(profitWei, tenPowU8(18)), ethUsdHint)
	var simPotNet big.Rat
	simPotNet.Sub(simProfitUSD, gasUSD)

	var simNetPct big.Rat
	if ratNotional.Sign() > 0 {
		var q big.Rat
		q.Quo(&simPotNet, ratNotional)
		simNetPct.Mul(&q, rat100)
	}

	simNetF, _ := simNetPct.Float64()
	simPotF, _ := simPotNet.Float64()
	midNetF, _ := midNetPct.Float64()
	midPotF, _ := midPotNet.Float64()

	// Подозрительно высокий net после AMM+газ — типичный признак «фейкового» спреда на тонкой ликвидности.
	if simPotNet.Sign() > 0 && simNetF > lowLiquiditySuspiciousNetPct {
		if shouldPrintLowLiquidity(tp.label) {
			ts := time.Now().Format("15:04:05.000")
			fmt.Printf("[%s] [LOW_LIQUIDITY] %s | BUY: %s | SELL: %s | NET(sim)=%.3f%% (~$%.2f) — вероятно иллюзия mid/тонкий пул\n",
				ts, tp.label, buyName, sellName, simNetF, simPotF)
		}
		return
	}

	// Сводка: только исполнимый по симуляции профит
	if simPotNet.Sign() > 0 && simNetPct.Cmp(statsOpportunityThreshold) >= 0 {
		tp.statMu.Lock()
		allow := time.Since(tp.lastOppSampleAt) >= 5*time.Second
		if allow {
			tp.lastOppSampleAt = time.Now()
		}
		tp.statMu.Unlock()
		if allow {
			appStats.recordOpportunity(simPotF, tp.label)
		}
	}

	if spamPrintMode {
		if midPotNet.Sign() <= 0 {
			return
		}
	} else {
		if simPotNet.Sign() <= 0 {
			return
		}
		if simNetPct.Cmp(minNetProfitThreshold) <= 0 {
			return
		}
	}

	printNet := simNetF
	if spamPrintMode && simPotNet.Sign() <= 0 {
		printNet = midNetF
	}

	if !shouldPrint(printNet, buyName, sellName, tp.label, spamPrintMode) {
		return
	}
	ts := time.Now().Format("15:04:05.000")
	nf, _ := ratNotional.Float64()
	if spamPrintMode && simPotNet.Sign() <= 0 {
		fmt.Printf("[%s] PROFIT FOUND: %.3f%% (mid-only) | %s | BUY: %s | SELL: %s | mid-ref: $%.2f | SIM $%.0f: убыток/ноль после AMM+газ (слиппедж/низкая ликв.)\n",
			ts, midNetF, tp.label, buyName, sellName, midPotF, nf)
		return
	}
	gasF, _ := gasUSD.Float64()
	fmt.Printf("[%s] PROFIT FOUND: %.3f%% NET(sim,$%.0f AMM+gas~$%.4f) | %s | BUY: %s | SELL: %s | POTENTIAL: $%.2f | mid-ref (без слиппеджа): $%.2f\n",
		ts, simNetF, nf, gasF, tp.label, buyName, sellName, simPotF, midPotF)
}

var printMu sync.Mutex
var lastPrintKey string
var lastPrintAt time.Time

func shouldPrint(netPct float64, buy, sell, pairLabel string, spam bool) bool {
	key := fmt.Sprintf("%.4f|%s|%s|%s", netPct, buy, sell, pairLabel)
	now := time.Now()
	minGap := 400 * time.Millisecond
	if spam {
		minGap = 80 * time.Millisecond
	}
	printMu.Lock()
	defer printMu.Unlock()
	if key == lastPrintKey && now.Sub(lastPrintAt) < minGap {
		return false
	}
	lastPrintKey = key
	lastPrintAt = now
	return true
}

func getPair(ctx context.Context, ec *ethclient.Client, factory, a, b common.Address) (common.Address, error) {
	data, err := factoryParsedABI.Pack("getPair", a, b)
	if err != nil {
		return common.Address{}, err
	}
	msg := ethereum.CallMsg{To: &factory, Data: data}
	out, err := ec.CallContract(ctx, msg, nil)
	if err != nil {
		return common.Address{}, err
	}
	var pair common.Address
	if err := factoryParsedABI.UnpackIntoInterface(&pair, "getPair", out); err != nil {
		return common.Address{}, err
	}
	return pair, nil
}

func (r *registry) tryRegisterWETHPair(ctx context.Context, ec *ethclient.Client, quote common.Address, label string) (added bool, err error) {
	t0, t1 := sortTokens(addrWETH, quote)
	key := pairKeyString(t0, t1)

	r.mu.Lock()
	if r.byKey[key] != nil {
		r.mu.Unlock()
		return false, nil
	}
	uniP, err := getPair(ctx, ec, addrUniswapV2Factory, addrWETH, quote)
	if err != nil {
		r.mu.Unlock()
		return false, err
	}
	sushiP, err := getPair(ctx, ec, addrSushiV2Factory, addrWETH, quote)
	if err != nil {
		r.mu.Unlock()
		return false, err
	}
	if uniP == (common.Address{}) || sushiP == (common.Address{}) {
		r.mu.Unlock()
		return false, nil
	}

	uniB, err := NewUniswapV2Pair(uniP, ec)
	if err != nil {
		r.mu.Unlock()
		return false, err
	}
	sushiB, err := NewUniswapV2Pair(sushiP, ec)
	if err != nil {
		r.mu.Unlock()
		return false, err
	}

	tp := &trackedPair{
		label:        label,
		token0:       t0,
		token1:       t1,
		dec0:         decimalsOf(t0),
		dec1:         decimalsOf(t1),
		uniAddr:      uniP,
		sushiAddr:    sushiP,
		uniBind:      uniB,
		sushiBind:    sushiB,
		uniR0:        new(big.Int),
		uniR1:        new(big.Int),
		sushiR0:      new(big.Int),
		sushiR1:      new(big.Int),
		registeredAt: time.Now(),
	}
	callOpts := &bind.CallOpts{Context: ctx}
	r0, r1, _, err := uniB.GetReserves(callOpts)
	if err != nil {
		r.mu.Unlock()
		return false, fmt.Errorf("uni getReserves %s: %w", label, err)
	}
	tp.uniR0.Set(r0)
	tp.uniR1.Set(r1)
	atomic.StoreUint32(&tp.haveUni, 1)
	r0, r1, _, err = sushiB.GetReserves(callOpts)
	if err != nil {
		r.mu.Unlock()
		return false, fmt.Errorf("sushi getReserves %s: %w", label, err)
	}
	tp.sushiR0.Set(r0)
	tp.sushiR1.Set(r1)
	atomic.StoreUint32(&tp.haveSushi, 1)

	r.byKey[key] = tp
	r.refs[uniP] = &addrRef{tp, true}
	r.refs[sushiP] = &addrRef{tp, false}
	r.mu.Unlock()

	tp.evaluateAndMaybePrint()
	log.Printf("зарегистрирована пара %s | Uni %s | Sushi %s", label, uniP.Hex(), sushiP.Hex())
	return true, nil
}

func parseExtraTokenAddresses() []quoteToken {
	raw := strings.TrimSpace(os.Getenv("WETH_EXTRA_TOKENS"))
	if raw == "" {
		return nil
	}
	var out []quoteToken
	for _, p := range strings.Split(raw, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if !common.IsHexAddress(p) {
			log.Printf("WETH_EXTRA_TOKENS: пропуск не-адреса %q", p)
			continue
		}
		a := common.HexToAddress(p)
		if _, ok := tokenDecimals[a]; !ok {
			tokenDecimals[a] = 18
		}
		out = append(out, quoteToken{a, "WETH/" + p[:8] + "…"})
	}
	return out
}

func bootstrapRegistry(ctx context.Context, ec *ethclient.Client, reg *registry) error {
	list := append([]quoteToken{}, defaultQuoteTokens...)
	list = append(list, parseExtraTokenAddresses()...)
	var firstErr error
	registered := 0
	for _, q := range list {
		added, err := reg.tryRegisterWETHPair(ctx, ec, q.addr, q.symbol)
		if err != nil && firstErr == nil {
			firstErr = err
		}
		if added {
			registered++
		}
	}
	if registered == 0 && firstErr != nil {
		return firstErr
	}
	if reg.pairCount() == 0 {
		return errors.New("ни одна WETH-пара не найдена на обоих DEX (getPair пустой)")
	}
	log.Printf("старт: отслеживаем %d пар (пулов в Sync: %d)", reg.pairCount(), reg.poolAddressCount())
	return nil
}

func parsePairCreated(log types.Log) (token0, token1, pair common.Address, ok bool) {
	if len(log.Topics) != 3 || log.Topics[0] != pairCreatedTopic {
		return common.Address{}, common.Address{}, common.Address{}, false
	}
	if len(log.Data) < 32 {
		return common.Address{}, common.Address{}, common.Address{}, false
	}
	token0 = common.BytesToAddress(log.Topics[1][12:])
	token1 = common.BytesToAddress(log.Topics[2][12:])
	pair = common.BytesToAddress(log.Data[12:32])
	return token0, token1, pair, true
}

func involvesWETH(t0, t1 common.Address) bool {
	return t0 == addrWETH || t1 == addrWETH
}

func otherTokenIfWETHPair(t0, t1 common.Address) (common.Address, bool) {
	if t0 == addrWETH {
		return t1, true
	}
	if t1 == addrWETH {
		return t0, true
	}
	return common.Address{}, false
}

func (r *registry) handlePairCreated(ctx context.Context, ec *ethclient.Client, lg types.Log, bump chan struct{}) {
	t0, t1, _, ok := parsePairCreated(lg)
	if !ok || !involvesWETH(t0, t1) {
		return
	}
	quote, ok := otherTokenIfWETHPair(t0, t1)
	if !ok {
		return
	}
	label := "WETH/" + quote.Hex()[:10] + "…"
	added, err := r.tryRegisterWETHPair(ctx, ec, quote, label)
	if err != nil {
		log.Printf("PairCreated: регистрация %s: %v", label, err)
		return
	}
	if added {
		select {
		case bump <- struct{}{}:
		default:
		}
	}
}

func listenPairCreated(ctx context.Context, ec *ethclient.Client, reg *registry, bump chan struct{}) error {
	ch := make(chan types.Log, 512)
	q := ethereum.FilterQuery{
		Addresses: []common.Address{addrUniswapV2Factory, addrSushiV2Factory},
		Topics:    [][]common.Hash{{pairCreatedTopic}},
	}
	sub, err := ec.SubscribeFilterLogs(ctx, q, ch)
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()
	log.Printf("Subscribed to PairCreated: Uni + Sushi factories")

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-sub.Err():
			return err
		case ev := <-ch:
			atomic.AddUint64(&pairCreatedLogsReceived, 1)
			reg.handlePairCreated(ctx, ec, ev, bump)
		}
	}
}

func listenSyncBatch(ctx context.Context, ec *ethclient.Client, reg *registry, addrs []common.Address) error {
	if len(addrs) == 0 {
		return errors.New("нет адресов для Sync")
	}
	ch := make(chan types.Log, 2048)
	q := ethereum.FilterQuery{
		Addresses: addrs,
		Topics:    [][]common.Hash{{syncTopic}},
	}
	sub, err := ec.SubscribeFilterLogs(ctx, q, ch)
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()
	log.Printf("Subscribed to Sync: %d pool address(es)", len(addrs))

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-sub.Err():
			return err
		case lg := <-ch:
			ref := reg.getRef(lg.Address)
			if ref == nil {
				continue
			}
			var bind *UniswapV2Pair
			if ref.uni {
				bind = ref.tp.uniBind
			} else {
				bind = ref.tp.sushiBind
			}
			ev, err := bind.ParseSync(lg)
			if err != nil || ev.Reserve0 == nil || ev.Reserve1 == nil {
				continue
			}
			atomic.AddUint64(&syncEventsParsed, 1)
			ref.tp.mu.Lock()
			if ref.uni {
				ref.tp.uniR0.Set(ev.Reserve0)
				ref.tp.uniR1.Set(ev.Reserve1)
				atomic.StoreUint32(&ref.tp.haveUni, 1)
			} else {
				ref.tp.sushiR0.Set(ev.Reserve0)
				ref.tp.sushiR1.Set(ev.Reserve1)
				atomic.StoreUint32(&ref.tp.haveSushi, 1)
			}
			ref.tp.mu.Unlock()
			ref.tp.evaluateAndMaybePrint()
		}
	}
}

func runSyncLoop(ctx context.Context, ec *ethclient.Client, reg *registry, bump <-chan struct{}) error {
	for {
		addrs := reg.snapshotAddresses()
		if len(addrs) == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-bump:
				continue
			case <-time.After(time.Second):
				continue
			}
		}
		innerCtx, cancel := context.WithCancel(ctx)
		errCh := make(chan error, 1)
		go func() {
			errCh <- listenSyncBatch(innerCtx, ec, reg, addrs)
		}()
		select {
		case <-ctx.Done():
			cancel()
			<-errCh
			return ctx.Err()
		case <-bump:
			cancel()
			<-errCh
			continue
		case err := <-errCh:
			cancel()
			if err != nil && !errors.Is(err, context.Canceled) {
				return err
			}
			continue
		}
	}
}

func heartbeatMinuteLoop(ctx context.Context, reg *registry) {
	t := time.NewTicker(heartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			parsed := atomic.LoadUint64(&syncEventsParsed) + atomic.LoadUint64(&pairCreatedLogsReceived)
			tm := time.Now().Format("15:04")
			log.Printf("[INFO] %s | Parsed %d events | Active pairs: %d | Status: Connected.",
				tm, parsed, reg.pairCount())
		}
	}
}

func statsSummaryLoop(ctx context.Context) {
	t := time.NewTicker(statsSummaryInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			printStatsSummaryBlock()
		}
	}
}

func runSessionWebSocket(ctx context.Context, wssURL, httpURL string, splitHTTP bool) error {
	wsCli, err := ethclient.DialContext(ctx, wssURL)
	if err != nil {
		return err
	}
	log.Printf("WebSocket connected")
	defer wsCli.Close()

	callCli := bind.ContractBackend(wsCli)
	if splitHTTP && httpURL != "" {
		hc, err := ethclient.DialContext(ctx, httpURL)
		if err != nil {
			return err
		}
		defer hc.Close()
		callCli = hc
	}
	ecCall, _ := callCli.(*ethclient.Client)
	setGasOracleClient(ecCall)
	defer setGasOracleClient(nil)

	reg := newRegistry()
	if err := bootstrapRegistry(ctx, ecCall, reg); err != nil {
		return err
	}

	innerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	bump := make(chan struct{}, 1)
	errPC := make(chan error, 1)
	go func() { errPC <- listenPairCreated(innerCtx, wsCli, reg, bump) }()
	errSY := make(chan error, 1)
	go func() { errSY <- runSyncLoop(innerCtx, wsCli, reg, bump) }()
	go heartbeatMinuteLoop(innerCtx, reg)
	go statsSummaryLoop(innerCtx)

	select {
	case <-ctx.Done():
		cancel()
		<-errPC
		<-errSY
		return ctx.Err()
	case err := <-errPC:
		cancel()
		<-errSY
		return err
	case err := <-errSY:
		cancel()
		<-errPC
		return err
	}
}

func runSessionHTTPPoll(ctx context.Context, httpURL string) error {
	if strings.TrimSpace(httpURL) == "" {
		return errors.New("нужен BASE_HTTP (HTTPS) для режима polling")
	}
	ec, err := ethclient.DialContext(ctx, httpURL)
	if err != nil {
		return err
	}
	defer ec.Close()
	setGasOracleClient(ec)
	defer setGasOracleClient(nil)
	reg := newRegistry()
	if err := bootstrapRegistry(ctx, ec, reg); err != nil {
		return err
	}
	log.Printf("BASE_FORCE_HTTP_POLL: HTTP опрос getReserves (аналитика, не для боя)")
	callOpts := &bind.CallOpts{Context: ctx}
	tick := time.NewTicker(750 * time.Millisecond)
	defer tick.Stop()
	go heartbeatMinuteLoop(ctx, reg)
	go statsSummaryLoop(ctx)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
			reg.mu.RLock()
			pairs := make([]*trackedPair, 0, len(reg.byKey))
			for _, tp := range reg.byKey {
				pairs = append(pairs, tp)
			}
			reg.mu.RUnlock()
			var polled uint64
			for _, tp := range pairs {
				r0, r1, _, e := tp.uniBind.GetReserves(callOpts)
				if e != nil {
					continue
				}
				tp.mu.Lock()
				tp.uniR0.Set(r0)
				tp.uniR1.Set(r1)
				tp.mu.Unlock()
				atomic.StoreUint32(&tp.haveUni, 1)
				r0, r1, _, e = tp.sushiBind.GetReserves(callOpts)
				if e != nil {
					continue
				}
				tp.mu.Lock()
				tp.sushiR0.Set(r0)
				tp.sushiR1.Set(r1)
				tp.mu.Unlock()
				atomic.StoreUint32(&tp.haveSushi, 1)
				tp.evaluateAndMaybePrint()
				polled++
			}
			if polled > 0 {
				atomic.AddUint64(&syncEventsParsed, polled)
			}
		}
	}
}

func runSession(ctx context.Context, wssURL, httpURL string, splitHTTP bool) error {
	if strings.TrimSpace(os.Getenv("BASE_FORCE_HTTP_POLL")) == "1" {
		log.Printf("BASE_FORCE_HTTP_POLL=1: режим без WebSocket (только HTTP)")
		return runSessionHTTPPoll(ctx, httpURL)
	}
	return runSessionWebSocket(ctx, wssURL, httpURL, splitHTTP)
}

func schemeHTTPToWS(u string) string {
	s := strings.TrimSpace(u)
	s = strings.Replace(s, "https://", "wss://", 1)
	s = strings.Replace(s, "http://", "ws://", 1)
	return s
}

func schemeWSToHTTP(u string) string {
	s := strings.TrimSpace(u)
	s = strings.Replace(s, "wss://", "https://", 1)
	s = strings.Replace(s, "ws://", "http://", 1)
	return s
}

func loadRPCEndpoints() (wss string, http string, splitHTTP bool) {
	h := strings.TrimSpace(os.Getenv("BASE_HTTP"))
	w := strings.TrimSpace(os.Getenv("BASE_WSS"))
	useHTTPCalls := strings.TrimSpace(os.Getenv("BASE_USE_HTTP_FOR_CALLS")) == "1"
	switch {
	case w == "" && h == "":
		return "", "", false
	case w != "" && h != "":
		return w, h, useHTTPCalls
	case w != "":
		return w, schemeWSToHTTP(w), false
	default:
		return schemeHTTPToWS(h), h, useHTTPCalls
	}
}

func ensureWSSURL(wss string) string {
	wss = strings.TrimSpace(wss)
	low := strings.ToLower(wss)
	if strings.HasPrefix(low, "https://") || strings.HasPrefix(low, "http://") {
		log.Printf("BASE_WSS: указан HTTP URL — для SubscribeFilterLogs нужен wss://, подменяю схему")
		return schemeHTTPToWS(wss)
	}
	if !strings.HasPrefix(low, "wss://") && !strings.HasPrefix(low, "ws://") {
		log.Fatal("BASE_WSS должен начинаться с wss:// или ws:// (WebSocket RPC для eth_subscribe)")
	}
	return wss
}

func loadMinNetProfitPct() {
	s := strings.TrimSpace(os.Getenv("MIN_NET_PROFIT_PCT"))
	if s == "" {
		return
	}
	r := new(big.Rat)
	if _, ok := r.SetString(s); ok {
		minNetProfitThreshold = r
		log.Printf("MIN_NET_PROFIT_PCT=%s (порог печати PROFIT FOUND; ≤0 = тестовый спам микроспредов)", r.FloatString(4))
		return
	}
	log.Printf("MIN_NET_PROFIT_PCT: не разобран %q, остаётся 0.5", s)
}

func loadStatsOpportunityThreshold() {
	s := strings.TrimSpace(os.Getenv("STATS_OPPORTUNITY_THRESHOLD_PCT"))
	if s == "" {
		return
	}
	r := new(big.Rat)
	if _, ok := r.SetString(s); ok {
		statsOpportunityThreshold = r
		log.Printf("STATS_OPPORTUNITY_THRESHOLD_PCT=%s (учёт в сводке Opportunities / суммы $)", r.FloatString(2))
		return
	}
	log.Printf("STATS_OPPORTUNITY_THRESHOLD_PCT: не разобран %q, остаётся 0.5", s)
}

func loadNotionalAndEthHint() {
	s := strings.TrimSpace(os.Getenv("NOTIONAL_USD"))
	if s != "" {
		r := new(big.Rat)
		if _, ok := r.SetString(s); ok && r.Sign() > 0 {
			ratNotional = r
			if f, _ := r.Float64(); f > 0 {
				notionalUSDForDisplay = f
			}
			log.Printf("NOTIONAL_USD=%s (размер сделки в AMM-симуляции)", r.FloatString(2))
		} else {
			log.Printf("NOTIONAL_USD: не разобран %q, остаётся 10", s)
		}
	}
	h := strings.TrimSpace(os.Getenv("ETH_USD_HINT"))
	if h != "" {
		r := new(big.Rat)
		if _, ok := r.SetString(h); ok && r.Sign() > 0 {
			ethUsdHint = r
			log.Printf("ETH_USD_HINT=%s ($ → WETH в симуляции)", r.FloatString(2))
		} else {
			log.Printf("ETH_USD_HINT: не разобран %q, остаётся 2500", h)
		}
	}
}

func loadDotEnv() {
	try := []string{".env"}
	if exe, err := os.Executable(); err == nil {
		try = append(try, filepath.Join(filepath.Dir(exe), ".env"))
	}
	for _, p := range try {
		if err := godotenv.Load(p); err == nil {
			log.Printf("загружен .env: %s", p)
			return
		}
	}
}

func main() {
	loadDotEnv()
	loadNotionalAndEthHint()
	loadMinNetProfitPct()
	loadStatsOpportunityThreshold()
	spamPrintMode = minNetProfitThreshold.Sign() <= 0
	if spamPrintMode {
		log.Printf("режим теста: MIN_NET_PROFIT_PCT≤0 — печать микроспредов (верните порог 0.3–0.5 для нормальной работы)")
	}
	appStats.started = time.Now()

	wss, http, splitHTTP := loadRPCEndpoints()
	if wss == "" {
		log.Fatal("задайте BASE_HTTP и/или BASE_WSS (например Alchemy Base)")
	}
	if strings.TrimSpace(os.Getenv("BASE_FORCE_HTTP_POLL")) != "1" {
		wss = ensureWSSURL(wss)
	}
	if splitHTTP {
		log.Printf("RPC: HTTP (вызовы) + WSS (Sync + PairCreated)")
	} else {
		log.Printf("RPC: один WebSocket — Sync на все пулы + PairCreated с фабрик")
	}

	rootCtx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	delay := reconnectMinDelay
	httpPollMode := strings.TrimSpace(os.Getenv("BASE_FORCE_HTTP_POLL")) == "1"
	for {
		if err := rootCtx.Err(); err != nil {
			log.Printf("shutdown: %v", err)
			return
		}
		if httpPollMode {
			log.Printf("HTTP RPC session (BASE_FORCE_HTTP_POLL=1)…")
		} else {
			log.Printf("dial WebSocket RPC…")
		}
		sessCtx, cancelSess := context.WithCancel(rootCtx)
		errDone := make(chan error, 1)
		go func() { errDone <- runSession(sessCtx, wss, http, splitHTTP) }()

		select {
		case <-rootCtx.Done():
			cancelSess()
			<-errDone
			log.Printf("shutdown: %v", rootCtx.Err())
			printStatsSummaryBlock()
			return
		case err := <-errDone:
			cancelSess()
			if err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("session ended: %v; reconnect in %v", err, delay)
				select {
				case <-time.After(delay):
				case <-rootCtx.Done():
					log.Printf("shutdown: %v", rootCtx.Err())
					return
				}
				if delay < reconnectMaxDelay {
					delay *= 2
					if delay > reconnectMaxDelay {
						delay = reconnectMaxDelay
					}
				}
				continue
			}
			if err != nil && errors.Is(err, context.Canceled) {
				log.Printf("shutdown: %v", err)
			}
			return
		}
	}
}
