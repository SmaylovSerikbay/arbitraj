package main

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rpc"
)

// revertReasonFromBytes: Error(string), Panic(uint256) или сырой hex (кастомные селекторы UniV3 / TransferHelper).
func revertReasonFromBytes(data []byte) string {
	if len(data) == 0 {
		return ""
	}
	if s, err := abi.UnpackRevert(data); err == nil && s != "" {
		return s
	}
	if len(data) >= 4 {
		return fmt.Sprintf("custom revert selector=%s payload=%s", hex.EncodeToString(data[:4]), hex.EncodeToString(data[4:]))
	}
	return "0x" + hex.EncodeToString(data)
}

// revertReasonFromRPCErr извлекает revert payload из eth_call / estimateGas (rpc.DataError → hex).
func revertReasonFromRPCErr(err error) string {
	if err == nil {
		return ""
	}
	if de, ok := err.(rpc.DataError); ok {
		raw := de.ErrorData()
		switch t := raw.(type) {
		case string:
			if strings.HasPrefix(t, "0x") {
				b := common.FromHex(t)
				if len(b) > 0 {
					return revertReasonFromBytes(b)
				}
			}
			return t
		default:
			return fmt.Sprintf("%v", raw)
		}
	}
	return err.Error()
}
