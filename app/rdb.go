package main

import (
	"encoding/hex"
	"fmt"
	"os"
)

func getEmptyRdb() []byte {
	hexStr := "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
	bts, err := hex.DecodeString(hexStr)
	if err != nil {
		fmt.Printf("unable to parse empty RDB hex: %v", err)
		os.Exit(1)
	}
	return bts
}

func encodeRDBFile(rData []byte) string {
	return fmt.Sprintf("$%d\r\n%s", len(rData), rData)
}

func needsResponse(value Value) bool {
	if len(value.Array) >= 2 {
		return value.Array[0].Str == "REPLCONF" && value.Array[1].Str == "GETACK"
	}
	return false
}
