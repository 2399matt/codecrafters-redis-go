package main

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"
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

func (s *Server) parseRDB() error {
	path := filepath.Join(s.config.dir, s.config.dbFilename)
	fmt.Printf("PATH FOR RDB: %s\n", path)
	file, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE, 0x644)
	if err != nil {
		return err
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	header := make([]byte, 9)
	if _, err := io.ReadFull(reader, header); err != nil {
		if err == io.EOF {
			return nil
		}
		return err
	}
	for {
		b, err := reader.ReadByte()
		if err != nil {
			return err
		}
		switch b {
		case 0xFA:
			readMetadata(reader)
		case 0xFE:
			readDatabase(s, reader)
		case 0xFF:
			buf := make([]byte, 8)
			if _, err := io.ReadFull(reader, buf); err != nil {
				return err
			}
			return nil
		default:
			return fmt.Errorf("unknown opcode found: %v", b)
		}
	}
}

func readDatabase(s *Server, r *bufio.Reader) error {
	// index val, throwing away for now
	if _, err := readLength(r); err != nil {
		return err
	}
	for {
		b, err := r.ReadByte()
		if err != nil {
			return err
		}
		switch b {
		case 0xFB:
			if _, err := readLength(r); err != nil {
				return err
			}
			if _, err := readLength(r); err != nil {
				return err
			}
			continue
		case 0xFC, 0xFD:
			expiry, err := readExpiry(b, r)
			if err != nil {
				return err
			}
			valType, err := r.ReadByte()
			if err != nil {
				return err
			}
			if err := readEntry(s, valType, expiry, r); err != nil {
				return err
			}
		case 0xFE, 0xFF:
			return r.UnreadByte()
		default:
			if err := readEntry(s, b, time.Time{}, r); err != nil {
				return err
			}
		}
	}
}

func readEntry(s *Server, valType byte, expiry time.Time, r *bufio.Reader) error {
	switch valType {
	case 0:
		key, err := readString(r)
		if err != nil {
			return err
		}
		val, err := readString(r)
		if err != nil {
			return err
		}
		s.storage.Set(key, Entry{Type: StringType, payload: val, expiration: expiry})
		return nil
	default:
		return fmt.Errorf("unsupported value type from readEntry: %v", valType)
	}
}

func readExpiry(op byte, r *bufio.Reader) (time.Time, error) {
	switch op {
	case 0xFC:
		buf := make([]byte, 8)
		if _, err := io.ReadFull(r, buf); err != nil {
			return time.Time{}, err
		}
		ms := binary.LittleEndian.Uint64(buf)
		return time.UnixMilli(int64(ms)), nil
	case 0xFD:
		buf := make([]byte, 4)
		if _, err := io.ReadFull(r, buf); err != nil {
			return time.Time{}, err
		}
		sec := binary.LittleEndian.Uint32(buf)
		return time.Unix(int64(sec), 0), nil
	default:
		return time.Time{}, fmt.Errorf("unknown opcode specified for expiry")
	}
}

func readMetadata(r *bufio.Reader) error {
	if _, err := readString(r); err != nil {
		return err
	}
	if _, err := readString(r); err != nil {
		return err
	}
	return nil
}

func readLength(r *bufio.Reader) (uint64, error) {
	b, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	encoding := b >> 6
	switch encoding {
	case 0:
		return uint64(b & 0b00111111), nil
	case 1:
		nxt, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		return uint64(b&0b00111111)<<8 | uint64(nxt), nil
	case 2:
		buf := make([]byte, 4)
		_, err := io.ReadFull(r, buf)
		if err != nil {
			return 0, err
		}
		var len uint64
		binary.BigEndian.PutUint64(buf, len)
		return len, nil
	default:
		return 0, fmt.Errorf("unknown encoding length")
	}

}

func readString(r *bufio.Reader) (string, error) {
	first, err := r.ReadByte()
	if err != nil {
		return "", err
	}
	// for ints stored as strings:
	if first >= 0xC0 {
		switch first {
		case 0xC0:
			val, err := r.ReadByte()
			return strconv.Itoa(int(int8(val))), err
		case 0xC1:
			buf := make([]byte, 2)
			if _, err := io.ReadFull(r, buf); err != nil {
				return "", err
			}
			val := binary.LittleEndian.Uint16(buf)
			return strconv.Itoa(int(int16(val))), nil
		case 0xC2:
			buf := make([]byte, 4)
			if _, err := io.ReadFull(r, buf); err != nil {
				return "", err
			}
			val := binary.LittleEndian.Uint32(buf)
			return strconv.Itoa(int(int32(val))), nil
		default:
			return "", fmt.Errorf("unsupported encoding")
		}
	}
	// normal string value
	r.UnreadByte()
	len, err := readLength(r)
	if err != nil {
		return "", err
	}
	buf := make([]byte, len)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}
