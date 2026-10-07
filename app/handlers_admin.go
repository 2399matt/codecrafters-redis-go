package main

import (
	"fmt"
	"strings"
)

func handleInfo(c *Client, value Value) string {
	if len(value.Array) < 2 {
		return encodeError("ERR invalid arguments for 'INFO'")
	}
	return encodeBulkString(fmt.Sprintf("role:%smaster_replid:%smaster_repl_offset:%d", c.server.config.role, c.server.config.masterReplID, c.server.replOffset))
}

func handleKeys(c *Client, value Value) string {
	if len(value.Array) < 2 {
		return encodeError("ERR invalid arguments for 'KEYS'")
	}
	target := value.Array[1].Str
	if target == "*" {
		target = ""
	}
	keys := c.server.storage.getKeys(target)
	vals := make([]Value, 0, len(keys))
	for _, key := range keys {
		vals = append(vals, Value{Type: BulkString, Str: key})
	}
	return encodeArray(vals)
}

func handleConfig(c *Client, value Value) string {
	if len(value.Array) < 2 {
		return encodeError("ERR invalid arguments for 'CONFIG'")
	}
	cmd := value.Array[1].Str
	switch cmd {
	case "GET":
		if len(value.Array) < 3 {
			return encodeError("missing value for 'GET'")
		}
		key := value.Array[2].Str
		vals := make([]Value, 0, 2)
		vals = append(vals, Value{Type: BulkString, Str: key})
		switch key {
		case "dir":
			vals = append(vals, Value{Type: BulkString, Str: c.server.config.dir})
		case "appendonly":
			if c.server.aof.config.enabled {
				vals = append(vals, Value{Type: BulkString, Str: "yes"})
			} else {
				vals = append(vals, Value{Type: BulkString, Str: "no"})
			}
		case "appenddirname":
			vals = append(vals, Value{Type: BulkString, Str: c.server.aof.config.dirName})
		case "appendfilename":
			vals = append(vals, Value{Type: BulkString, Str: c.server.aof.config.fileName})
		case "appendfsync":
			vals = append(vals, Value{Type: BulkString, Str: c.server.aof.config.appendfSync})
		default:
			return encodeError("ERR unknown value for 'CONFIG GET'")
		}
		return encodeArray(vals)
	default:
		return encodeError("ERR unknown value for 'CONFIG'")
	}
}

func handleAcl(c *Client, value Value) string {
	cmd := strings.ToUpper(value.Array[1].Str)
	switch cmd {
	case "WHOAMI":
		return encodeBulkString("default")
	case "GETUSER":
		name := value.Array[2].Str
		user := c.server.acl.Get(name)
		return handleGetUser(user)
	case "SETUSER":
		if len(value.Array) < 4 {
			return encodeError("ERR invalid arguments for 'SETUSER'")
		}
		name := value.Array[2].Str
		user := c.server.acl.Get(name)
		return handleSetUser(c, user, value)
	default:
		return encodeError(fmt.Sprintf("ERR unknown command for ACL: %s", cmd))
	}
}

func handleSetUser(c *Client, user *User, value Value) string {
	if user == nil {
		return encodeError("ERR user not found")
	}
	pass := value.Array[3].Str
	update := false
	if strings.HasPrefix(pass, ">") {
		pass = pass[1:]
		update = true
	}
	if update {
		c.server.acl.updateUser(user, pass)
	}
	return encodeSimpleString("OK")

}

func handleGetUser(user *User) string {
	if user == nil {
		return encodeError("ERR user not found")
	}
	vals := []Value{{Type: BulkString, Str: "flags"}}
	flagVals := Value{Type: Array, Array: make([]Value, 0)}
	passVals := Value{Type: Array, Array: make([]Value, 0)}
	for _, flag := range user.flags {
		flagVals.Array = append(flagVals.Array, Value{Type: BulkString, Str: flag})
	}
	for _, pass := range user.passwords {
		passVals.Array = append(passVals.Array, Value{Type: BulkString, Str: pass})
	}
	vals = append(vals, flagVals)
	vals = append(vals, Value{Type: BulkString, Str: "passwords"})
	vals = append(vals, passVals)
	return encodeArray(vals)
}
