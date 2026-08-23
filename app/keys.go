package main

import "strings"

func getKeys(value Value) []string {
	command := strings.ToUpper(value.Array[0].Str)

	switch command {
	case "GET", "SET", "INCR", "TYPE":
		return []string{value.Array[1].Str}

	case "LPUSH", "RPUSH", "LPOP", "LLEN", "LRANGE":
		return []string{value.Array[1].Str}

	case "XADD", "XRANGE":
		return []string{value.Array[1].Str}

	case "XREAD":
		return getXReadKeys(value)

	default:
		return nil
	}
}

func getXReadKeys(value Value) []string {
	streamsIdx := -1
	for i, v := range value.Array {
		if strings.ToUpper(v.Str) == "STREAMS" {
			streamsIdx = i
			break
		}
	}
	if streamsIdx == -1 {
		return nil
	}
	rest := value.Array[streamsIdx+1:]
	if len(rest) == 0 || len(rest)%2 != 0 {
		return nil
	}
	n := len(rest) / 2
	keyVals := rest[:n]
	res := make([]string, 0, len(keyVals))
	for i := range keyVals {
		res = append(res, keyVals[i].Str)
	}
	return res
}
