package main

import (
	"fmt"
	"strconv"
)

func handleGeoAdd(c *Client, value Value) string {
	if len(value.Array) < 5 {
		return encodeError("ERR invalid arguments for 'GEOADD'")
	}
	key := value.Array[1].Str
	lat, err := strconv.ParseFloat(value.Array[2].Str, 64)
	if err != nil {
		return encodeError(fmt.Sprintf("ERR invalid longitude,latitude pair %s,%s", value.Array[2].Str, value.Array[3].Str))
	}
	lon, err := strconv.ParseFloat(value.Array[3].Str, 64)
	if err != nil {
		return encodeError(fmt.Sprintf("ERR invalid longitude,latitude pair %s,%s", value.Array[2].Str, value.Array[3].Str))
	}
	if !isValidCoords(lat, lon) {
		return encodeError(fmt.Sprintf("ERR invalid longitude,latitude pair %s,%s", value.Array[2].Str, value.Array[3].Str))
	}
	member := value.Array[4].Str
	return encodeInteger(c.server.storage.zAdd(0, key, member))
}

func isValidCoords(lat, lon float64) bool {
	return (lat > -180 && lat < 180) && (lon > -85.05112878 && lon < 85.05112878)
}
