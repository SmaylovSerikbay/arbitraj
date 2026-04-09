package main

import (
	"math/big"
	"os"
	"strconv"
	"strings"
)

// v2SimPessimismBps — базовый haircut на выход каждой V2-ноги (пессимизм к резервам и фронт-рану).
var v2SimPessimismBps uint64 = 40

// v2ImpactExtraBpsCap — потолок доп. bps за крупный trade относительно резерва.
var v2ImpactExtraBpsCap uint64 = 200

// aeroSimExtraBps — доп. haircut на выходе Aerodrome getAmountOut (нет резервов в руке без лишнего RPC).
var aeroSimExtraBps uint64 = 40

func loadV2PessimismSettings() {
	v2SimPessimismBps = 40
	v2ImpactExtraBpsCap = 200
	aeroSimExtraBps = 40
	if v := strings.TrimSpace(os.Getenv("V2_SIM_PESSIMISM_BPS")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 && n <= 500 {
			v2SimPessimismBps = uint64(n)
		}
	}
	if v := strings.TrimSpace(os.Getenv("V2_IMPACT_EXTRA_BPS_CAP")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 && n <= 1000 {
			v2ImpactExtraBpsCap = uint64(n)
		}
	}
	if v := strings.TrimSpace(os.Getenv("AERO_SIM_EXTRA_BPS")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 && n <= 500 {
			aeroSimExtraBps = uint64(n)
		}
	}
}

// v2HopImpactExtraBps: чем больше amountIn относительно reserveIn, тем выше доп. пессимизм (тонкие мем-пулы).
func v2HopImpactExtraBps(amountIn, reserveIn *big.Int) uint64 {
	if amountIn == nil || reserveIn == nil || reserveIn.Sign() == 0 || amountIn.Sign() <= 0 {
		return 0
	}
	// bps ≈ amountIn * 10000 / reserveIn
	x := new(big.Int).Mul(amountIn, big.NewInt(10000))
	x.Div(x, reserveIn)
	if x.Sign() <= 0 {
		return 0
	}
	if !x.IsUint64() {
		return v2ImpactExtraBpsCap
	}
	u := x.Uint64()
	if u > v2ImpactExtraBpsCap {
		return v2ImpactExtraBpsCap
	}
	return u
}

// applyBpsHaircut: out * (10000 - bps) / 10000
func applyBpsHaircut(out *big.Int, bps uint64) *big.Int {
	if out == nil || out.Sign() <= 0 || bps >= 10000 {
		return big.NewInt(0)
	}
	if bps == 0 {
		return new(big.Int).Set(out)
	}
	n := new(big.Int).Mul(out, big.NewInt(int64(10000-bps)))
	return n.Div(n, big.NewInt(10000))
}
