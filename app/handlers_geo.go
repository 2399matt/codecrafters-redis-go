package main

func handleGeoAdd(c *Client, value Value) string {
	if len(value.Array) < 5 {
		return encodeError("ERR invalid arguments for 'GEOADD'")
	}
	// key := value.Array[1].Str
	// lat := value.Array[2].Str
	// lon := value.Array[3].Str
	// member := value.Array[4].Str
	return encodeInteger(1)
}
