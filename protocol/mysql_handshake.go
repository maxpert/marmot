package protocol

import (
	"encoding/binary"
	"fmt"

	"vitess.io/vitess/go/mysql"
)

// HandshakeInfo contains parsed information from a MySQL HandshakeResponse41 packet
type HandshakeInfo struct {
	Username             string
	Database             string
	CharacterSet         uint8
	Capabilities         uint32
	AuthMethod           string
	AuthResponse         []byte
	ConnectionAttributes map[string]string
}

// ParseHandshakeResponse parses a MySQL HandshakeResponse41 packet.
// Returns HandshakeInfo and error (nil if parsing succeeded).
//
// Reference: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_response.html
func ParseHandshakeResponse(data []byte) (*HandshakeInfo, error) {
	pos := 0
	info := &HandshakeInfo{}

	// Client flags, 4 bytes
	clientFlags, pos, ok := readUint32(data, pos)
	if !ok {
		return nil, fmt.Errorf("failed to read client capability flags")
	}
	info.Capabilities = clientFlags

	// Verify Protocol41 support
	if clientFlags&mysql.CapabilityClientProtocol41 == 0 {
		return nil, fmt.Errorf("only MySQL protocol 4.1 is supported")
	}

	// Max packet size, 4 bytes - we ignore this value
	_, pos, ok = readUint32(data, pos)
	if !ok {
		return nil, fmt.Errorf("failed to read max packet size")
	}

	// Character set, 1 byte
	characterSet, pos, ok := readByte(data, pos)
	if !ok {
		return nil, fmt.Errorf("failed to read character set")
	}
	info.CharacterSet = characterSet

	// Reserved, 23 bytes (skip)
	if len(data) < pos+23 {
		return nil, fmt.Errorf("packet too short for reserved bytes")
	}
	pos += 23

	// Username (null-terminated string)
	username, pos, ok := readNullString(data, pos)
	if !ok {
		return nil, fmt.Errorf("failed to read username")
	}
	info.Username = username

	// Auth response (three possible formats based on capability flags)
	var authResponse []byte
	if clientFlags&mysql.CapabilityClientPluginAuthLenencClientData != 0 {
		// Length-encoded integer + auth data
		authLen, newPos, ok := readLenEncInt(data, pos)
		if !ok {
			return nil, fmt.Errorf("failed to read length-encoded auth response length")
		}
		pos = newPos
		authResponse, pos, ok = readBytesCopy(data, pos, int(authLen))
		if !ok {
			return nil, fmt.Errorf("failed to read length-encoded auth response data")
		}
	} else if clientFlags&mysql.CapabilityClientSecureConnection != 0 {
		// 1-byte length + auth data
		authLen, newPos, ok := readByte(data, pos)
		if !ok {
			return nil, fmt.Errorf("failed to read secure connection auth response length")
		}
		pos = newPos
		authResponse, pos, ok = readBytesCopy(data, pos, int(authLen))
		if !ok {
			return nil, fmt.Errorf("failed to read secure connection auth response data")
		}
	} else {
		// Null-terminated string (old style)
		authStr, newPos, ok := readNullString(data, pos)
		if !ok {
			return nil, fmt.Errorf("failed to read null-terminated auth response")
		}
		pos = newPos
		authResponse = []byte(authStr)
	}
	info.AuthResponse = authResponse

	// Database name (only if CLIENT_CONNECT_WITH_DB flag is set)
	if clientFlags&mysql.CapabilityClientConnectWithDB != 0 {
		dbName, newPos, ok := readNullString(data, pos)
		if !ok {
			return nil, fmt.Errorf("failed to read database name")
		}
		pos = newPos
		info.Database = dbName
	}

	// Auth method (only if CLIENT_PLUGIN_AUTH flag is set)
	// Note: We don't treat failure to parse auth method as fatal since we already
	// have the critical info (username, database) at this point
	info.AuthMethod = "mysql_native_password" // default
	if clientFlags&mysql.CapabilityClientPluginAuth != 0 && pos < len(data) {
		authMethodStr, newPos, ok := readNullString(data, pos)
		if ok {
			pos = newPos
			if authMethodStr != "" {
				info.AuthMethod = authMethodStr
			}
		}
	}

	// Connection attributes (only if CLIENT_CONNECT_ATTRS flag is set)
	if clientFlags&mysql.CapabilityClientConnAttr != 0 {
		attrs, _, ok := parseConnAttrs(data, pos)
		if ok {
			info.ConnectionAttributes = attrs
		}
		// Note: We don't treat failure to parse attributes as fatal
	}

	return info, nil
}

// readByte reads a single byte from data at pos.
// Returns the byte, new position, and success flag.
func readByte(data []byte, pos int) (byte, int, bool) {
	if pos >= len(data) {
		return 0, pos, false
	}
	return data[pos], pos + 1, true
}

// readUint32 reads a little-endian uint32 from data at pos.
// Returns the value, new position, and success flag.
func readUint32(data []byte, pos int) (uint32, int, bool) {
	if pos+4 > len(data) {
		return 0, pos, false
	}
	return binary.LittleEndian.Uint32(data[pos : pos+4]), pos + 4, true
}

// readNullString reads a null-terminated string from data at pos.
// Returns the string (without null terminator), new position, and success flag.
func readNullString(data []byte, pos int) (string, int, bool) {
	start := pos
	for pos < len(data) && data[pos] != 0 {
		pos++
	}
	if pos >= len(data) {
		return "", pos, false
	}
	return string(data[start:pos]), pos + 1, true
}

// readLenEncInt reads a MySQL length-encoded integer from data at pos.
// Returns the value, new position, and success flag.
//
// MySQL length-encoded integers:
// - If first byte < 0xFB: value is the byte itself
// - If first byte == 0xFC: value is next 2 bytes (little-endian)
// - If first byte == 0xFD: value is next 3 bytes (little-endian)
// - If first byte == 0xFE: value is next 8 bytes (little-endian)
func readLenEncInt(data []byte, pos int) (uint64, int, bool) {
	if pos >= len(data) {
		return 0, pos, false
	}

	switch data[pos] {
	case 0xFC:
		// 2-byte integer
		if pos+3 > len(data) {
			return 0, pos, false
		}
		val := uint64(data[pos+1]) | uint64(data[pos+2])<<8
		return val, pos + 3, true
	case 0xFD:
		// 3-byte integer
		if pos+4 > len(data) {
			return 0, pos, false
		}
		val := uint64(data[pos+1]) | uint64(data[pos+2])<<8 | uint64(data[pos+3])<<16
		return val, pos + 4, true
	case 0xFE:
		// 8-byte integer
		if pos+9 > len(data) {
			return 0, pos, false
		}
		val := binary.LittleEndian.Uint64(data[pos+1 : pos+9])
		return val, pos + 9, true
	default:
		// 1-byte integer
		return uint64(data[pos]), pos + 1, true
	}
}

// readBytesCopy reads size bytes from data at pos and returns a copy.
// Returns the bytes, new position, and success flag.
func readBytesCopy(data []byte, pos int, size int) ([]byte, int, bool) {
	if pos+size > len(data) {
		return nil, pos, false
	}
	result := make([]byte, size)
	copy(result, data[pos:pos+size])
	return result, pos + size, true
}

// readLenEncString reads a length-encoded string from data at pos.
// Returns the string, new position, and success flag.
func readLenEncString(data []byte, pos int) (string, int, bool) {
	length, pos, ok := readLenEncInt(data, pos)
	if !ok {
		return "", pos, false
	}
	str, pos, ok := readBytesCopy(data, pos, int(length))
	if !ok {
		return "", pos, false
	}
	return string(str), pos, true
}

// parseConnAttrs parses MySQL connection attributes.
// Returns the attributes map, new position, and success flag.
//
// Format: length-encoded total size, then pairs of length-encoded strings (key, value)
func parseConnAttrs(data []byte, pos int) (map[string]string, int, bool) {
	// Read total length of all attributes
	totalLen, pos, ok := readLenEncInt(data, pos)
	if !ok {
		return nil, pos, false
	}

	// Verify we have enough data
	if pos+int(totalLen) > len(data) {
		return nil, pos, false
	}

	attrs := make(map[string]string)
	endPos := pos + int(totalLen)

	// Parse key-value pairs
	for pos < endPos {
		key, newPos, ok := readLenEncString(data, pos)
		if !ok {
			return attrs, pos, false
		}
		pos = newPos

		value, newPos, ok := readLenEncString(data, pos)
		if !ok {
			return attrs, pos, false
		}
		pos = newPos

		attrs[key] = value
	}

	return attrs, pos, true
}
