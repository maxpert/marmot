package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/rs/zerolog/log"
)

// MySQLServer implements a basic MySQL protocol server
type MySQLServer struct {
	address    string
	listener   net.Listener
	quit       chan struct{}
	wg         sync.WaitGroup
	handler    ConnectionHandler
	connIDGen  uint64
	connIDLock sync.Mutex
}

// ConnectionSession represents per-connection state
type ConnectionSession struct {
	ConnID          uint64
	CurrentDatabase string
	RemoteAddr      string
}

// ConnectionHandler defines the interface for handling MySQL commands
type ConnectionHandler interface {
	HandleQuery(session *ConnectionSession, query string) (*ResultSet, error)
}

// ResultSet represents a MySQL result set
type ResultSet struct {
	Columns []ColumnDef
	Rows    [][]interface{}
}

// ColumnDef represents a column definition
type ColumnDef struct {
	Name string
	Type byte
}

// NewMySQLServer creates a new MySQL server
func NewMySQLServer(address string, handler ConnectionHandler) *MySQLServer {
	return &MySQLServer{
		address: address,
		quit:    make(chan struct{}),
		handler: handler,
	}
}

// Start starts the MySQL server
func (s *MySQLServer) Start() error {
	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}
	s.listener = listener

	log.Info().Str("address", s.address).Msg("MySQL server started")

	s.wg.Add(1)
	go s.acceptLoop()

	return nil
}

// Stop stops the MySQL server
func (s *MySQLServer) Stop() {
	close(s.quit)
	if s.listener != nil {
		s.listener.Close()
	}
	s.wg.Wait()
}

// nextConnID generates a unique connection ID
func (s *MySQLServer) nextConnID() uint64 {
	s.connIDLock.Lock()
	defer s.connIDLock.Unlock()
	s.connIDGen++
	return s.connIDGen
}

func (s *MySQLServer) acceptLoop() {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return
			default:
				log.Error().Err(err).Msg("Accept error")
				continue
			}
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleConnection(conn)
		}()
	}
}

func (s *MySQLServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	// Create session for this connection
	session := &ConnectionSession{
		ConnID:          s.nextConnID(),
		CurrentDatabase: "marmot", // Default database
		RemoteAddr:      conn.RemoteAddr().String(),
	}

	log.Debug().Uint64("conn_id", session.ConnID).Str("remote", session.RemoteAddr).Msg("New connection")

	// 1. Send Initial Handshake Packet
	if err := s.writeHandshake(conn); err != nil {
		log.Error().Err(err).Msg("Failed to write handshake")
		return
	}

	// 2. Read Handshake Response
	if _, err := s.readPacket(conn); err != nil {
		log.Error().Err(err).Msg("Failed to read handshake response")
		return
	}

	// 3. Send OK Packet (Authentication successful)
	if err := s.writeOK(conn, 2); err != nil {
		log.Error().Err(err).Msg("Failed to write OK packet")
		return
	}

	// 4. Command Loop
	for {
		// Reset sequence for new command cycle
		// Command packet is always sequence 0
		payload, err := s.readPacket(conn)
		if err != nil {
			if err != io.EOF {
				log.Error().Err(err).Msg("Read packet error")
			}
			return
		}

		if len(payload) == 0 {
			continue
		}

		cmd := payload[0]
		switch cmd {
		case 0x02: // COM_INIT_DB (USE database)
			dbName := string(payload[1:])
			session.CurrentDatabase = dbName
			log.Debug().Uint64("conn_id", session.ConnID).Str("database", dbName).Msg("Changed database")
			s.writeOK(conn, 1)
		case 0x03: // COM_QUERY
			query := string(payload[1:])
			s.processQuery(conn, session, query)
		case 0x0E: // COM_PING
			s.writeOK(conn, 1)
		case 0x01: // COM_QUIT
			return
		default:
			log.Warn().Hex("cmd", []byte{cmd}).Msg("Unsupported command")
			s.writeError(conn, 1, 1047, "Unknown command")
		}
	}
}

func (s *MySQLServer) processQuery(conn net.Conn, session *ConnectionSession, query string) {
	// Parse first to check validity (optional, but good for sanity)
	// For now, we pass directly to handler which will use the parser/coordinator

	rs, err := s.handler.HandleQuery(session, query)
	if err != nil {
		s.writeError(conn, 1, 1064, err.Error())
		return
	}

	if rs == nil {
		// OK response for non-SELECT
		s.writeOK(conn, 1)
		return
	}

	// Send Result Set
	s.writeResultSet(conn, 1, rs)
}

// --- Packet Writing Helpers ---

func (s *MySQLServer) writeHandshake(w io.Writer) error {
	// Basic handshake packet
	// https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::Handshake

	buf := new(bytes.Buffer)

	// Protocol version (10)
	buf.WriteByte(10)

	// Server version
	buf.WriteString("5.7.0-Marmot-V2\x00")

	// Connection ID
	binary.Write(buf, binary.LittleEndian, uint32(1))

	// Auth plugin data part 1 (8 bytes)
	buf.WriteString("12345678")

	// Filler
	buf.WriteByte(0)

	// Capability flags (lower 2 bytes)
	// CLIENT_PROTOCOL_41 (0x200) | CLIENT_SECURE_CONNECTION (0x8000) | CLIENT_PLUGIN_AUTH (0x80000 - in upper bytes)
	// Lower 16 bits: 0xa200 = CLIENT_LONG_PASSWORD (0x0001) | CLIENT_PROTOCOL_41 (0x0200) | CLIENT_SECURE_CONNECTION (0x8000) | CLIENT_TRANSACTIONS (0x2000)
	binary.Write(buf, binary.LittleEndian, uint16(0xa201))

	// Character set (utf8_general_ci = 33)
	buf.WriteByte(33)

	// Status flags
	binary.Write(buf, binary.LittleEndian, uint16(2))

	// Capability flags (upper 2 bytes)
	// CLIENT_PLUGIN_AUTH (0x80000) = bit 19, which is bit 3 in upper 16 bits = 0x0008
	binary.Write(buf, binary.LittleEndian, uint16(0x0008))

	// Auth plugin data length
	buf.WriteByte(21) // 8 + 13

	// Reserved (10 bytes)
	buf.Write([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0})

	// Auth plugin data part 2 (13 bytes)
	buf.WriteString("123456789012\x00")

	// Auth plugin name
	buf.WriteString("mysql_native_password\x00")

	return s.writePacket(w, 0, buf.Bytes())
}

func (s *MySQLServer) writeOK(w io.Writer, seq byte) error {
	buf := []byte{0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00}
	return s.writePacket(w, seq, buf)
}

func (s *MySQLServer) writeError(w io.Writer, seq byte, code uint16, msg string) error {
	buf := new(bytes.Buffer)
	buf.WriteByte(0xFF) // Error packet header
	binary.Write(buf, binary.LittleEndian, code)
	buf.WriteByte('#')
	buf.WriteString("HY000") // SQL State
	buf.WriteString(msg)

	return s.writePacket(w, seq, buf.Bytes())
}

func (s *MySQLServer) writeResultSet(w io.Writer, seq byte, rs *ResultSet) error {
	// 1. Column Count
	if err := s.writePacket(w, seq, packLengthEncodedInt(uint64(len(rs.Columns)))); err != nil {
		return err
	}
	seq++

	// 2. Column Definitions
	for _, col := range rs.Columns {
		buf := new(bytes.Buffer)

		writeLenEncString(buf, "def")    // Catalog
		writeLenEncString(buf, "")       // Schema
		writeLenEncString(buf, "tbl")    // Table
		writeLenEncString(buf, "tbl")    // Org Table
		writeLenEncString(buf, col.Name) // Name
		writeLenEncString(buf, col.Name) // Org Name

		buf.WriteByte(0x0c)                                  // Length of fixed fields
		binary.Write(buf, binary.LittleEndian, uint16(33))   // Charset
		binary.Write(buf, binary.LittleEndian, uint32(1024)) // Length
		buf.WriteByte(col.Type)                              // Type
		binary.Write(buf, binary.LittleEndian, uint16(0))    // Flags
		buf.WriteByte(0)                                     // Decimals
		buf.Write([]byte{0, 0})                              // Filler

		if err := s.writePacket(w, seq, buf.Bytes()); err != nil {
			return err
		}
		seq++
	}

	// 3. EOF Packet
	if err := s.writePacket(w, seq, []byte{0xFE, 0, 0, 0x02, 0}); err != nil {
		return err
	}
	seq++

	// 4. Rows
	for _, row := range rs.Rows {
		buf := new(bytes.Buffer)
		for _, val := range row {
			if val == nil {
				buf.WriteByte(0xFB) // NULL
			} else {
				strVal := fmt.Sprintf("%v", val)
				writeLenEncString(buf, strVal)
			}
		}
		if err := s.writePacket(w, seq, buf.Bytes()); err != nil {
			return err
		}
		seq++
	}

	// 5. EOF Packet
	return s.writePacket(w, seq, []byte{0xFE, 0, 0, 0x02, 0})
}

func (s *MySQLServer) writePacket(w io.Writer, seq byte, payload []byte) error {
	length := len(payload)
	header := []byte{
		byte(length),
		byte(length >> 8),
		byte(length >> 16),
		seq,
	}

	if _, err := w.Write(header); err != nil {
		return err
	}
	if _, err := w.Write(payload); err != nil {
		return err
	}
	return nil
}

func (s *MySQLServer) readPacket(r io.Reader) ([]byte, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}

	length := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
	// seq := header[3] // We ignore sequence check for simplicity

	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}

	return payload, nil
}

// --- Utils ---

func packLengthEncodedInt(n uint64) []byte {
	if n < 251 {
		return []byte{byte(n)}
	}
	// Simplified for this example
	return []byte{byte(n)}
}

func writeLenEncString(buf *bytes.Buffer, s string) {
	l := uint64(len(s))
	if l < 251 {
		buf.WriteByte(byte(l))
	} else if l < 65536 {
		buf.WriteByte(0xFC)
		binary.Write(buf, binary.LittleEndian, uint16(l))
	} else {
		// ... handle larger strings
		buf.WriteByte(0xFD)
		binary.Write(buf, binary.LittleEndian, uint32(l))
	}
	buf.WriteString(s)
}
