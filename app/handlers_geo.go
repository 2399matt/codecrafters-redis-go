package main

import (
	"fmt"
	"math"
	"strconv"
)

var MIN_LATITUDE = -85.05112878
var MAX_LATITUDE = 85.05112878
var MIN_LONGITUDE = -180.0
var MAX_LONGITUDE = 180.0

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
		if scoreStr, err := c.server.storage.zScore(key, member); err != nil {
			vals = append(vals, Value{Type: Array, Array: nil})
		} else {
			scoreFloat, _ := strconv.ParseFloat(scoreStr, 64)
			score := uint64(scoreFloat)
			lat, lon := decodeCoords(score)
			pairs := Value{
				Type: Array,
				Array: []Value{
					{Type: BulkString, Str: strconv.FormatFloat(lon, 'f', -1, 64)},
					{Type: BulkString, Str: strconv.FormatFloat(lat, 'f', -1, 64)},
				},
			}
			vals = append(vals, pairs)
		}
	}
	return encodeArray(vals)
}

func handleGeoDist(c *Client, value Value) string {
	if len(value.Array) < 4 {
		return encodeError("ERR invalid arguments for 'GEODIST'")
	}
	key := value.Array[1].Str
	score1Str, err := c.server.storage.zScore(key, value.Array[2].Str)
	if err != nil {
		return encodeError(fmt.Sprintf("ERR %s", err.Error()))
	}
	score2Str, err := c.server.storage.zScore(key, value.Array[3].Str)
	if err != nil {
		return encodeError(fmt.Sprintf("ERR %s", err.Error()))
	}
	score1, err := strconv.ParseFloat(score1Str, 64)
	if err != nil {
		return encodeError(fmt.Sprintf("ERR %s", err.Error()))
	}
	score2, err := strconv.ParseFloat(score2Str, 64)
	if err != nil {
		return encodeError(fmt.Sprintf("ERR %s", err.Error()))
	}
	fLat, fLon := decodeCoords(uint64(score1))
	sLat, sLon := decodeCoords(uint64(score2))
	dist := haversine(fLat, fLon, sLat, sLon)
	return encodeBulkString(strconv.FormatFloat(dist, 'f', 4, 64))
}

func handleGeoSearch(c *Client, value Value) string {
	if len(value.Array) < 8 {
		return encodeError("ERR invalid arguments for 'GEOSEARCH'")
	}
	vals := make([]Value, 0)
	key := value.Array[1].Str
	target, err := strconv.ParseFloat(value.Array[6].Str, 64)
	if err != nil {
		fmt.Printf("%s\n", err.Error())
		return encodeError("ERR invalid targe distance")
	}
	cLat, err := strconv.ParseFloat(value.Array[4].Str, 64)
	if err != nil {
		fmt.Printf("%s\n", err.Error())
		return encodeError(fmt.Sprintf("ERR %s", err.Error()))
	}
	cLon, err := strconv.ParseFloat(value.Array[3].Str, 64)
	if err != nil {
		fmt.Printf("%s\n", err.Error())
		return encodeError(fmt.Sprintf("ERR %s", err.Error()))
	}
	set := c.server.storage.getSetMembers(key)
	if set == nil {
		return encodeError("ERR set not found")
	}
	for _, mem := range set {
		currLat, currLon := decodeCoords(uint64(mem.score))
		if haversine(currLat, currLon, cLat, cLon) <= target {
			fmt.Printf("ADDING MEMBER: %s\n", mem.member)
			vals = append(vals, Value{Type: BulkString, Str: mem.member})
		}
	}
	return encodeArray(vals)
}

func haversine(lat1, lon1, lat2, lon2 float64) float64 {
	rEarth := 6372797.560856
	lat1 = lat1 * (math.Pi / 180)
	lat2 = lat2 * (math.Pi / 180)
	lon1 = lon1 * (math.Pi / 180)
	lon2 = lon2 * (math.Pi / 180)
	dLat := lat2 - lat1
	dLon := lon2 - lon1
	// can probably get rid of Pow and just mult
	a := math.Pow(math.Sin(dLat/2), 2) + (math.Cos(lat1) * math.Cos(lat2) * math.Pow(math.Sin(dLon/2), 2))
	c := 2 * math.Asin(math.Sqrt(a))
	return rEarth * c
}

func isValidCoords(lat, lon float64) bool {
	return (lon > -180 && lon < 180) && (lat > -85.05112878 && lat < 85.05112878)
}

func encodeCoords(lat, lon float64) uint64 {
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

func deInterleave(score uint64) (uint32, uint32) {
	var lat, lon uint32
	for i := range 26 {
		latBit := score >> (2 * i) & 1
		lonBit := score >> (2*i + 1) & 1
		lat = lat | uint32(latBit)<<i
		lon = lon | uint32(lonBit)<<i
	}
	return lat, lon
}

func decodeCoords(score uint64) (float64, float64) {
	var lat, lon float64
	normLat, normLon := deInterleave(score)
	latRange := MAX_LATITUDE - MIN_LATITUDE
	lonRange := MAX_LONGITUDE - MIN_LONGITUDE
	// have to center here
	latMin := (latRange*float64(normLat))/math.Pow(2, 26) + MIN_LATITUDE
	latMax := (latRange*float64(normLat+1))/math.Pow(2, 26) + MIN_LATITUDE
	lonMin := (lonRange*float64(normLon))/math.Pow(2, 26) + MIN_LONGITUDE
	lonMax := (lonRange*float64(normLon+1))/math.Pow(2, 26) + MIN_LONGITUDE
	lat = (latMin + latMax) / 2
	lon = (lonMin + lonMax) / 2
	return lat, lon
}
