package main

import (
	"fmt"
	"math"
	"strconv"
)

func handleGeoAdd(c *Client, value Value) string {
	if len(value.Array) < 5 {
		return encodeError("ERR invalid arguments for 'GEOADD'")
	}
	key := value.Array[1].Str
	lon, err := strconv.ParseFloat(value.Array[2].Str, 64)
	if err != nil {
		return encodeError(fmt.Sprintf("ERR invalid longitude,latitude pair %s,%s", value.Array[2].Str, value.Array[3].Str))
	}
	lat, err := strconv.ParseFloat(value.Array[3].Str, 64)
	if err != nil {
		return encodeError(fmt.Sprintf("ERR invalid longitude,latitude pair %s,%s", value.Array[2].Str, value.Array[3].Str))
	}
	if !isValidCoords(lat, lon) {
		return encodeError(fmt.Sprintf("ERR invalid longitude,latitude pair %s,%s", value.Array[2].Str, value.Array[3].Str))
	}
	member := value.Array[4].Str
	score := encodeCoords(lat, lon)
	return encodeInteger(c.server.storage.zAdd(float64(score), key, member))
}

func handleGeoPos(c *Client, value Value) string {
	if len(value.Array) < 3 {
		return encodeError("ERR invalid arguments for 'GEOPOS'")
	}
	key := value.Array[1].Str
	vals := make([]Value, 0)
	for i := 2; i < len(value.Array); i++ {
		member := value.Array[i].Str
		if _, err := c.server.storage.zScore(key, member); err != nil {
			vals = append(vals, Value{Type: Array, Array: nil})
		} else {
			pairs := Value{
				Type: Array,
				Array: []Value{
					{Type: BulkString, Str: "0"},
					{Type: BulkString, Str: "0"},
				},
			}
			vals = append(vals, pairs)
		}
	}
	return encodeArray(vals)
}

func isValidCoords(lat, lon float64) bool {
	return (lon > -180 && lon < 180) && (lat > -85.05112878 && lat < 85.05112878)
}

func encodeCoords(lat, lon float64) uint64 {
	MIN_LATITUDE := -85.05112878
	MAX_LATITUDE := 85.05112878
	MIN_LONGITUDE := -180.0
	MAX_LONGITUDE := 180.0

	latRange := MAX_LATITUDE - MIN_LATITUDE
	lonRange := MAX_LONGITUDE - MIN_LONGITUDE
	normLat := uint32(math.Pow(2, 26) * (lat - MIN_LATITUDE) / latRange)
	normLon := uint32(math.Pow(2, 26) * (lon - MIN_LONGITUDE) / lonRange)
	return interleave(normLat, normLon)
}

func interleave(lat, lon uint32) uint64 {
	var res uint64
	for i := 0; i < 26; i++ {
		// grab i'th bit from lat/lon
		latBit := (uint64(lat) >> i) & 1
		lonBit := (uint64(lon) >> i) & 1
		// alternate lat/lon bits to res
		res = res | (latBit << (2 * i))
		res = res | (lonBit << (2*i + 1))
	}
	return res
}
