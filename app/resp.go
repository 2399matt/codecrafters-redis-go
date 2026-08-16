package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const (
	SimpleString = '+'
	Error        = '-'
	Integer      = ':'
	BulkString   = '$'
	Array        = '*'
)

type Value struct {
	Type  byte
	Str   string
	Num   int
	Array []Value
}

type Parser struct {
	reader *bufio.Reader
}

func NewParser(reader io.Reader) *Parser {
	return &Parser{reader: bufio.NewReader(reader)}
}

func (p *Parser) parse() (Value, error) {
	first, err := p.reader.ReadByte()
	if err != nil {
		return Value{}, err
	}
	switch first {
	case SimpleString:
		text, err := p.readLine()
		if err != nil {
			return Value{}, err
		}
		return Value{Type: SimpleString, Str: string(text)}, nil
	case Error:
		text, err := p.readLine()
		if err != nil {
			return Value{}, err
		}
		return Value{Type: Error, Str: string(text)}, nil
	case Integer:
		text, err := p.readLine()
		if err != nil {
			return Value{}, err
		}
		num, err := strconv.Atoi(string(text))
		if err != nil {
			return Value{}, err
		}
		return Value{Type: Integer, Num: num}, nil
	case BulkString:
		line, err := p.readLine()
		if err != nil {
			return Value{}, err
		}
		length, err := strconv.Atoi(string(line))
		if err != nil {
			return Value{}, err
		}
		buf := make([]byte, length+2)
		if _, err := io.ReadFull(p.reader, buf); err != nil {
			return Value{}, err
		}
		return Value{Type: BulkString, Str: string(buf[:length])}, nil
	case Array:
		text, err := p.readLine()
		if err != nil {
			return Value{}, err
		}
		length, err := strconv.Atoi(string(text))
		if err != nil {
			return Value{}, err
		}
		arr := make([]Value, length)
		for i := 0; i < len(arr); i++ {
			arr[i], err = p.parse()
			if err != nil {
				return Value{}, err
			}
		}
		return Value{Type: Array, Array: arr}, nil
	}
	return Value{}, fmt.Errorf("Unknown symbol: %v", first)
}

func (p *Parser) readLine() ([]byte, error) {
	// +OK\r\n
	line, err := p.reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, fmt.Errorf("Unknown format")
	}
	return line[:len(line)-2], nil
}

func encode(v Value) string {
	switch v.Type {
	case SimpleString:
		return encodeSimpleString(v.Str)
	case Error:
		return encodeError(v.Str)
	case Integer:
		return encodeInteger(v.Num)
	case BulkString:
		return encodeBulkString(v.Str)
	case Array:
		return encodeArray(v.Array)
	default:
		panic("unknown RESP type")
	}
}

func encodeSimpleString(response string) string {
	return fmt.Sprintf("+%s\r\n", response)
}

func encodeBulkString(response string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(response), response)
}

func encodeError(errResponse string) string {
	return fmt.Sprintf("-%s\r\n", errResponse)
}

func encodeInteger(val int) string {
	return fmt.Sprintf(":%d\r\n", val)
}

func encodeEmptyArray() string {
	return "*0\r\n"
}

func encodeNullArray() string {
	return "*-1\r\n"
}

func encodeArray(values []Value) string {
	var result strings.Builder
	result.WriteString(fmt.Sprintf("*%d\r\n", len(values)))
	for _, val := range values {
		result.WriteString(encode(val))
	}
	return result.String()
}

func encodeNullString() string {
	return "$-1\r\n"
}
