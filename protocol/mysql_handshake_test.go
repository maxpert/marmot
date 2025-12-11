package protocol

import (
	"encoding/binary"
	"testing"

	"vitess.io/vitess/go/mysql"
)

// TestParseHandshakeResponse_ValidPacket tests parsing a valid HandshakeResponse41 packet
func TestParseHandshakeResponse_ValidPacket(t *testing.T) {
	// Build a minimal valid HandshakeResponse41 packet
	// Format: capability_flags(4) + max_packet_size(4) + character_set(1) + reserved(23) + username(null-terminated) + auth(1-byte-len + data) + database(null-terminated)
	var packet []byte

	// Capability flags (include Protocol41, ConnectWithDB, SecureConnection)
	capFlags := uint32(mysql.CapabilityClientProtocol41 |
		mysql.CapabilityClientConnectWithDB |
		mysql.CapabilityClientSecureConnection)
	capBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(capBytes, capFlags)
	packet = append(packet, capBytes...)

	// Max packet size (ignored)
	packet = append(packet, 0x00, 0x00, 0x01, 0x00)

	// Character set
	packet = append(packet, 0x08) // utf8_general_ci

	// Reserved (23 bytes)
	packet = append(packet, make([]byte, 23)...)

	// Username (null-terminated)
	packet = append(packet, []byte("testuser")...)
	packet = append(packet, 0x00)

	// Auth response (1-byte length + data for SecureConnection)
	authData := []byte("authdata12345678")
	packet = append(packet, byte(len(authData)))
	packet = append(packet, authData...)

	// Database (null-terminated)
	packet = append(packet, []byte("testdb")...)
	packet = append(packet, 0x00)

	// Parse the packet
	info, err := ParseHandshakeResponse(packet)
	if err != nil {
		t.Fatalf("ParseHandshakeResponse failed: %v", err)
	}

	// Verify parsed data
	if info.Username != "testuser" {
		t.Errorf("Expected username 'testuser', got '%s'", info.Username)
	}
	if info.Database != "testdb" {
		t.Errorf("Expected database 'testdb', got '%s'", info.Database)
	}
	if info.CharacterSet != 0x08 {
		t.Errorf("Expected character set 0x08, got 0x%02x", info.CharacterSet)
	}
	if info.Capabilities != capFlags {
		t.Errorf("Expected capabilities 0x%08x, got 0x%08x", capFlags, info.Capabilities)
	}
	if string(info.AuthResponse) != "authdata12345678" {
		t.Errorf("Expected auth response 'authdata12345678', got '%s'", string(info.AuthResponse))
	}
}

// TestParseHandshakeResponse_NoDatabase tests parsing when CLIENT_CONNECT_WITH_DB is not set
func TestParseHandshakeResponse_NoDatabase(t *testing.T) {
	var packet []byte

	// Capability flags (Protocol41 but NOT ConnectWithDB)
	capFlags := uint32(mysql.CapabilityClientProtocol41 | mysql.CapabilityClientSecureConnection)
	capBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(capBytes, capFlags)
	packet = append(packet, capBytes...)

	// Max packet size
	packet = append(packet, 0x00, 0x00, 0x01, 0x00)

	// Character set
	packet = append(packet, 0x21) // utf8mb4

	// Reserved (23 bytes)
	packet = append(packet, make([]byte, 23)...)

	// Username
	packet = append(packet, []byte("user")...)
	packet = append(packet, 0x00)

	// Auth response (1-byte length)
	packet = append(packet, 0x00) // Empty auth

	// Parse the packet
	info, err := ParseHandshakeResponse(packet)
	if err != nil {
		t.Fatalf("ParseHandshakeResponse failed: %v", err)
	}

	// Verify database is empty
	if info.Database != "" {
		t.Errorf("Expected empty database, got '%s'", info.Database)
	}
	if info.Username != "user" {
		t.Errorf("Expected username 'user', got '%s'", info.Username)
	}
}

// TestParseHandshakeResponse_WithAuthPlugin tests parsing with CLIENT_PLUGIN_AUTH
func TestParseHandshakeResponse_WithAuthPlugin(t *testing.T) {
	var packet []byte

	// Capability flags (include PluginAuth)
	capFlags := uint32(mysql.CapabilityClientProtocol41 |
		mysql.CapabilityClientSecureConnection |
		mysql.CapabilityClientPluginAuth)
	capBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(capBytes, capFlags)
	packet = append(packet, capBytes...)

	// Max packet size
	packet = append(packet, 0x00, 0x00, 0x01, 0x00)

	// Character set
	packet = append(packet, 0x08)

	// Reserved (23 bytes)
	packet = append(packet, make([]byte, 23)...)

	// Username
	packet = append(packet, []byte("admin")...)
	packet = append(packet, 0x00)

	// Auth response (1-byte length)
	packet = append(packet, 0x04)
	packet = append(packet, []byte("test")...)

	// Auth plugin name
	packet = append(packet, []byte("caching_sha2_password")...)
	packet = append(packet, 0x00)

	// Parse the packet
	info, err := ParseHandshakeResponse(packet)
	if err != nil {
		t.Fatalf("ParseHandshakeResponse failed: %v", err)
	}

	// Verify auth method
	if info.AuthMethod != "caching_sha2_password" {
		t.Errorf("Expected auth method 'caching_sha2_password', got '%s'", info.AuthMethod)
	}
}

// TestParseHandshakeResponse_TooShort tests error handling for short packets
func TestParseHandshakeResponse_TooShort(t *testing.T) {
	// Packet too short (less than minimum required)
	packet := []byte{0x01, 0x02, 0x03}

	_, err := ParseHandshakeResponse(packet)
	if err == nil {
		t.Fatal("Expected error for too-short packet, got nil")
	}
}

// TestParseHandshakeResponse_NonProtocol41 tests error for non-Protocol41
func TestParseHandshakeResponse_NonProtocol41(t *testing.T) {
	var packet []byte

	// Capability flags WITHOUT Protocol41
	capFlags := uint32(0x0000)
	capBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(capBytes, capFlags)
	packet = append(packet, capBytes...)

	// Add minimal rest of packet
	packet = append(packet, make([]byte, 28)...)

	_, err := ParseHandshakeResponse(packet)
	if err == nil {
		t.Fatal("Expected error for non-Protocol41 packet, got nil")
	}
	if err.Error() != "only MySQL protocol 4.1 is supported" {
		t.Errorf("Expected specific error message, got: %v", err)
	}
}

// TestReadLenEncInt tests length-encoded integer parsing
func TestReadLenEncInt(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected uint64
		newPos   int
		ok       bool
	}{
		{"1-byte < 251", []byte{0x05}, 5, 1, true},
		{"1-byte 250", []byte{0xFA}, 250, 1, true},
		{"2-byte", []byte{0xFC, 0x01, 0x02}, 0x0201, 3, true},
		{"3-byte", []byte{0xFD, 0x01, 0x02, 0x03}, 0x030201, 4, true},
		{"8-byte", []byte{0xFE, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}, 0x0807060504030201, 9, true},
		{"too short for 2-byte", []byte{0xFC, 0x01}, 0, 0, false},
		{"too short for 3-byte", []byte{0xFD, 0x01}, 0, 0, false},
		{"too short for 8-byte", []byte{0xFE, 0x01, 0x02}, 0, 0, false},
		{"empty", []byte{}, 0, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, pos, ok := readLenEncInt(tt.data, 0)
			if ok != tt.ok {
				t.Errorf("Expected ok=%v, got %v", tt.ok, ok)
			}
			if ok && val != tt.expected {
				t.Errorf("Expected value %d, got %d", tt.expected, val)
			}
			if ok && pos != tt.newPos {
				t.Errorf("Expected position %d, got %d", tt.newPos, pos)
			}
		})
	}
}

// TestReadNullString tests null-terminated string parsing
func TestReadNullString(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected string
		newPos   int
		ok       bool
	}{
		{"simple string", []byte("hello\x00world"), "hello", 6, true},
		{"empty string", []byte("\x00"), "", 1, true},
		{"no null terminator", []byte("hello"), "", 5, false},
		{"empty data", []byte{}, "", 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			str, pos, ok := readNullString(tt.data, 0)
			if ok != tt.ok {
				t.Errorf("Expected ok=%v, got %v", tt.ok, ok)
			}
			if ok && str != tt.expected {
				t.Errorf("Expected string '%s', got '%s'", tt.expected, str)
			}
			if ok && pos != tt.newPos {
				t.Errorf("Expected position %d, got %d", tt.newPos, pos)
			}
		})
	}
}
