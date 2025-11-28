package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
	"strings"
	"sync"

	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol/query"
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

// SessionTransaction holds transaction state for explicit BEGIN/COMMIT
type SessionTransaction struct {
	TxnID      uint64
	StartTS    hlc.Timestamp
	Statements []Statement
	Database   string
}

// ConnectionSession represents per-connection state
type ConnectionSession struct {
	ConnID           uint64
	CurrentDatabase  string
	RemoteAddr       string
	preparedStmts    map[uint32]*PreparedStatement
	preparedStmtLock sync.Mutex
	nextStmtID       uint32

	// Transaction state for explicit BEGIN/COMMIT
	activeTxn   *SessionTransaction
	activeTxnMu sync.Mutex
}

// InTransaction returns true if session has an active explicit transaction
func (s *ConnectionSession) InTransaction() bool {
	s.activeTxnMu.Lock()
	defer s.activeTxnMu.Unlock()
	return s.activeTxn != nil
}

// BeginTransaction starts a new explicit transaction
func (s *ConnectionSession) BeginTransaction(txnID uint64, startTS hlc.Timestamp, database string) {
	s.activeTxnMu.Lock()
	defer s.activeTxnMu.Unlock()
	s.activeTxn = &SessionTransaction{
		TxnID:      txnID,
		StartTS:    startTS,
		Statements: []Statement{},
		Database:   database,
	}
}

// AddStatement adds a statement to the active transaction buffer
func (s *ConnectionSession) AddStatement(stmt Statement) {
	s.activeTxnMu.Lock()
	defer s.activeTxnMu.Unlock()
	if s.activeTxn != nil {
		s.activeTxn.Statements = append(s.activeTxn.Statements, stmt)
	}
}

// GetTransaction returns the active transaction (nil if none)
func (s *ConnectionSession) GetTransaction() *SessionTransaction {
	s.activeTxnMu.Lock()
	defer s.activeTxnMu.Unlock()
	return s.activeTxn
}

// EndTransaction clears the active transaction
func (s *ConnectionSession) EndTransaction() {
	s.activeTxnMu.Lock()
	defer s.activeTxnMu.Unlock()
	s.activeTxn = nil
}

// PreparedStatement represents a prepared statement
type PreparedStatement struct {
	ID           uint32
	Query        string
	ParamCount   uint16
	ParamTypes   []byte // Cached parameter types for subsequent executions
	OriginalType StatementType
	Context      *query.QueryContext
}

// ConnectionHandler defines the interface for handling MySQL commands
type ConnectionHandler interface {
	HandleQuery(session *ConnectionSession, query string) (*ResultSet, error)
}

// ResultSet represents a MySQL result set
type ResultSet struct {
	Columns      []ColumnDef
	Rows         [][]interface{}
	RowsAffected int64
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
		preparedStmts:   make(map[uint32]*PreparedStatement),
		nextStmtID:      1,
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
	if err := s.writeOK(conn, 2, 0); err != nil {
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
		log.Debug().Uint64("conn_id", session.ConnID).Hex("cmd", []byte{cmd}).Msg("Received command")

		switch cmd {
		case 0x02: // COM_INIT_DB (USE database)
			dbName := string(payload[1:])
			session.CurrentDatabase = dbName
			log.Debug().Uint64("conn_id", session.ConnID).Str("database", dbName).Msg("Changed database")
			s.writeOK(conn, 1, 0)
		case 0x03: // COM_QUERY
			query := string(payload[1:])
			s.processQuery(conn, session, query)
		case 0x16: // COM_STMT_PREPARE
			query := string(payload[1:])
			s.handleStmtPrepare(conn, session, query)
		case 0x17: // COM_STMT_EXECUTE
			log.Debug().Uint64("conn_id", session.ConnID).Msg("COM_STMT_EXECUTE received")
			s.handleStmtExecute(conn, session, payload[1:])
		case 0x19: // COM_STMT_CLOSE
			if len(payload) >= 5 {
				stmtID := binary.LittleEndian.Uint32(payload[1:5])
				session.preparedStmtLock.Lock()
				delete(session.preparedStmts, stmtID)
				session.preparedStmtLock.Unlock()
				log.Debug().Uint64("conn_id", session.ConnID).Uint32("stmt_id", stmtID).Msg("Statement closed")
			}
			// COM_STMT_CLOSE doesn't send a response
		case 0x0E: // COM_PING
			s.writeOK(conn, 1, 0)
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
		s.writeMySQLErr(conn, 1, err)
		return
	}

	if rs == nil || (len(rs.Columns) == 0 && len(rs.Rows) == 0) {
		// OK response for non-SELECT (INSERT/UPDATE/DELETE/etc)
		rowsAffected := int64(0)
		if rs != nil {
			rowsAffected = rs.RowsAffected
		}
		s.writeOK(conn, 1, rowsAffected)
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

func (s *MySQLServer) writeOK(w io.Writer, seq byte, rowsAffected int64) error {
	buf := new(bytes.Buffer)
	buf.WriteByte(0x00) // OK packet header
	// Affected rows (length-encoded integer)
	buf.Write(packLengthEncodedInt(uint64(rowsAffected)))
	// Last insert ID (length-encoded integer)
	buf.Write(packLengthEncodedInt(0))
	// Status flags
	binary.Write(buf, binary.LittleEndian, uint16(0x0002)) // SERVER_STATUS_AUTOCOMMIT
	// Warnings
	binary.Write(buf, binary.LittleEndian, uint16(0))
	return s.writePacket(w, seq, buf.Bytes())
}

func (s *MySQLServer) writeError(w io.Writer, seq byte, code uint16, msg string) error {
	return s.writeErrorWithState(w, seq, code, "HY000", msg)
}

func (s *MySQLServer) writeErrorWithState(w io.Writer, seq byte, code uint16, sqlState, msg string) error {
	buf := new(bytes.Buffer)
	buf.WriteByte(0xFF) // Error packet header
	binary.Write(buf, binary.LittleEndian, code)
	buf.WriteByte('#')
	buf.WriteString(sqlState)
	buf.WriteString(msg)

	return s.writePacket(w, seq, buf.Bytes())
}

// writeMySQLErr writes a MySQLError to the connection, extracting code/state from the error
func (s *MySQLServer) writeMySQLErr(w io.Writer, seq byte, err error) error {
	if mysqlErr, ok := err.(*MySQLError); ok {
		return s.writeErrorWithState(w, seq, mysqlErr.Code, mysqlErr.SQLState, mysqlErr.Message)
	}
	// Default to generic error code 1105 (ER_UNKNOWN_ERROR)
	return s.writeError(w, seq, 1105, err.Error())
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

// writeBinaryResultSet writes a result set in MySQL binary protocol format (for prepared statements)
// https://dev.mysql.com/doc/internals/en/binary-protocol-resultset.html
func (s *MySQLServer) writeBinaryResultSet(w io.Writer, seq byte, rs *ResultSet) error {
	// 1. Column Count
	if err := s.writePacket(w, seq, packLengthEncodedInt(uint64(len(rs.Columns)))); err != nil {
		return err
	}
	seq++

	// 2. Column Definitions (same as text protocol)
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

	// 3. EOF Packet after columns
	if err := s.writePacket(w, seq, []byte{0xFE, 0, 0, 0x02, 0}); err != nil {
		return err
	}
	seq++

	// 4. Binary Rows
	for _, row := range rs.Rows {
		buf := new(bytes.Buffer)

		// Binary row header (0x00)
		buf.WriteByte(0x00)

		// NULL bitmap: (column_count + 7 + 2) / 8 bytes
		// The +2 offset is because the first 2 bits are reserved
		nullBitmapLen := (len(rs.Columns) + 7 + 2) / 8
		nullBitmap := make([]byte, nullBitmapLen)

		// Set NULL bits for null values
		for i, val := range row {
			if val == nil {
				bytePos := (i + 2) / 8
				bitPos := (i + 2) % 8
				nullBitmap[bytePos] |= 1 << bitPos
			}
		}
		buf.Write(nullBitmap)

		// Write values in binary format
		for i, val := range row {
			if val == nil {
				continue // Already marked in NULL bitmap
			}

			// Convert value to binary format based on column type
			switch rs.Columns[i].Type {
			case 0x01: // TINY
				buf.WriteByte(byte(fmt.Sprintf("%v", val)[0]))
			case 0x02: // SHORT
				v := int16(0)
				fmt.Sscanf(fmt.Sprintf("%v", val), "%d", &v)
				binary.Write(buf, binary.LittleEndian, v)
			case 0x03, 0x09: // LONG, INT24
				v := int32(0)
				fmt.Sscanf(fmt.Sprintf("%v", val), "%d", &v)
				binary.Write(buf, binary.LittleEndian, v)
			case 0x08: // LONGLONG
				v := int64(0)
				fmt.Sscanf(fmt.Sprintf("%v", val), "%d", &v)
				binary.Write(buf, binary.LittleEndian, v)
			case 0x04: // FLOAT
				v := float32(0)
				fmt.Sscanf(fmt.Sprintf("%v", val), "%f", &v)
				binary.Write(buf, binary.LittleEndian, v)
			case 0x05: // DOUBLE
				v := float64(0)
				fmt.Sscanf(fmt.Sprintf("%v", val), "%f", &v)
				binary.Write(buf, binary.LittleEndian, v)
			default: // STRING, VARCHAR, TEXT, BLOB, etc.
				strVal := fmt.Sprintf("%v", val)
				writeLenEncString(buf, strVal)
			}
		}

		if err := s.writePacket(w, seq, buf.Bytes()); err != nil {
			return err
		}
		seq++
	}

	// 5. EOF Packet after rows
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
	if n < 65536 {
		return []byte{0xFC, byte(n), byte(n >> 8)}
	}
	if n < 16777216 {
		return []byte{0xFD, byte(n), byte(n >> 8), byte(n >> 16)}
	}
	buf := make([]byte, 9)
	buf[0] = 0xFE
	binary.LittleEndian.PutUint64(buf[1:], n)
	return buf
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

// --- Prepared Statement Handlers ---

func (s *MySQLServer) handleStmtPrepare(conn net.Conn, session *ConnectionSession, sql string) {
	ctx := query.NewContext(sql, nil)
	ctx.RequiresPrepare = true

	if err := globalPipeline.Process(ctx); err != nil {
		log.Debug().
			Uint64("conn_id", session.ConnID).
			Str("query", sql).
			Err(err).
			Msg("Failed to process SQL in PREPARE")
		s.writeError(conn, 1, 1064, err.Error())
		return
	}

	if !ctx.IsValid {
		errorMsg := "You have an error in your SQL syntax"
		if ctx.ValidationErr != nil {
			errorMsg = ctx.ValidationErr.Error()
		}
		log.Debug().
			Uint64("conn_id", session.ConnID).
			Str("query", sql).
			Str("error", errorMsg).
			Msg("Invalid SQL in PREPARE")
		s.writeError(conn, 1, 1064, errorMsg)
		return
	}

	paramCount := uint16(countPlaceholders(ctx.TranspiledSQL))

	session.preparedStmtLock.Lock()
	stmtID := session.nextStmtID
	session.nextStmtID++
	session.preparedStmts[stmtID] = &PreparedStatement{
		ID:           stmtID,
		Query:        ctx.TranspiledSQL,
		ParamCount:   paramCount,
		OriginalType: StatementType(ctx.StatementType),
		Context:      ctx,
	}
	session.preparedStmtLock.Unlock()

	log.Debug().
		Uint64("conn_id", session.ConnID).
		Uint32("stmt_id", stmtID).
		Str("query", sql).
		Uint16("params", paramCount).
		Msg("Statement prepared")

	// Send COM_STMT_PREPARE_OK response
	buf := new(bytes.Buffer)

	// Status (0x00 for OK)
	buf.WriteByte(0x00)

	// Statement ID
	binary.Write(buf, binary.LittleEndian, stmtID)

	// Number of columns (0 for INSERT/UPDATE/DELETE, >0 for SELECT)
	binary.Write(buf, binary.LittleEndian, uint16(0))

	// Number of parameters
	binary.Write(buf, binary.LittleEndian, paramCount)

	// Filler
	buf.WriteByte(0x00)

	// Warning count
	binary.Write(buf, binary.LittleEndian, uint16(0))

	s.writePacket(conn, 1, buf.Bytes())

	// Send parameter definitions if params > 0
	// This is required by the MySQL protocol
	seqNum := byte(2)
	if paramCount > 0 {
		for i := uint16(0); i < paramCount; i++ {
			paramDefBuf := new(bytes.Buffer)
			writeLenEncString(paramDefBuf, "def")
			writeLenEncString(paramDefBuf, "")
			writeLenEncString(paramDefBuf, "")
			writeLenEncString(paramDefBuf, "")
			writeLenEncString(paramDefBuf, "?")
			writeLenEncString(paramDefBuf, "")
			paramDefBuf.WriteByte(0x0c)                                // length of fixed fields
			binary.Write(paramDefBuf, binary.LittleEndian, uint16(63)) // charset (binary)
			binary.Write(paramDefBuf, binary.LittleEndian, uint32(0))  // length
			paramDefBuf.WriteByte(0xFD)                                // type (VAR_STRING)
			binary.Write(paramDefBuf, binary.LittleEndian, uint16(0))  // flags
			paramDefBuf.WriteByte(0x00)                                // decimals
			binary.Write(paramDefBuf, binary.LittleEndian, uint16(0))  // filler

			s.writePacket(conn, seqNum, paramDefBuf.Bytes())
			seqNum++
		}

		// Send EOF packet after parameters
		eofBuf := new(bytes.Buffer)
		eofBuf.WriteByte(0xFE)
		binary.Write(eofBuf, binary.LittleEndian, uint16(0))      // warnings
		binary.Write(eofBuf, binary.LittleEndian, uint16(0x0002)) // SERVER_STATUS_AUTOCOMMIT
		s.writePacket(conn, seqNum, eofBuf.Bytes())
	}
}

func (s *MySQLServer) handleStmtExecute(conn net.Conn, session *ConnectionSession, payload []byte) {
	if len(payload) < 9 {
		s.writeError(conn, 1, 1064, "Invalid COM_STMT_EXECUTE packet")
		return
	}

	// Parse statement ID
	stmtID := binary.LittleEndian.Uint32(payload[0:4])

	// Get prepared statement
	session.preparedStmtLock.Lock()
	stmt, ok := session.preparedStmts[stmtID]
	session.preparedStmtLock.Unlock()

	if !ok {
		s.writeError(conn, 1, 1243, fmt.Sprintf("Unknown statement ID: %d", stmtID))
		return
	}

	// Parse flags (byte 4)
	// flags := payload[4]

	// Iteration count (bytes 5-8) - always 1
	// iterCount := binary.LittleEndian.Uint32(payload[5:9])

	// Parse parameters if any
	params := make([]interface{}, stmt.ParamCount)
	if stmt.ParamCount > 0 {
		// Parse NULL bitmap and new-params-bound-flag
		nullBitmapLen := (int(stmt.ParamCount) + 7) / 8
		if len(payload) < 9+nullBitmapLen+1 {
			s.writeError(conn, 1, 1064, "Invalid parameter data")
			return
		}

		nullBitmap := payload[9 : 9+nullBitmapLen]
		newParamsBoundFlag := payload[9+nullBitmapLen]

		offset := 9 + nullBitmapLen + 1

		// Get parameter types - either from payload or cached
		var paramTypes []byte
		if newParamsBoundFlag == 1 {
			// New types provided - parse and cache them
			if len(payload) < offset+int(stmt.ParamCount)*2 {
				s.writeError(conn, 1, 1064, "Invalid parameter types")
				return
			}
			paramTypes = make([]byte, int(stmt.ParamCount)*2)
			copy(paramTypes, payload[offset:offset+int(stmt.ParamCount)*2])
			offset += int(stmt.ParamCount) * 2

			// Cache param types for subsequent executions
			session.preparedStmtLock.Lock()
			stmt.ParamTypes = paramTypes
			session.preparedStmtLock.Unlock()
		} else {
			// Use cached param types
			paramTypes = stmt.ParamTypes
		}

		// Parse parameter values using the types
		for i := uint16(0); i < stmt.ParamCount; i++ {
			// Check NULL bitmap
			if nullBitmap[i/8]&(1<<(i%8)) != 0 {
				params[i] = nil
				continue
			}

			// Parse value based on type
			if len(paramTypes) > int(i)*2 {
				paramType := paramTypes[i*2]

				var val interface{}
				var err error
				offset, val, err = parseParamValue(payload, offset, paramType)
				if err != nil {
					s.writeError(conn, 1, 1064, fmt.Sprintf("Failed to parse parameter %d: %v", i, err))
					return
				}
				params[i] = val
			}
		}
	}

	// Build final query by replacing ? with actual values
	// Note: stmt.Query is already transpiled to SQLite syntax
	finalQuery := buildQueryWithParams(stmt.Query, params)

	log.Debug().
		Uint64("conn_id", session.ConnID).
		Uint32("stmt_id", stmtID).
		Str("query", finalQuery).
		Msg("Executing prepared statement")

	// Execute the query directly (it's already transpiled SQLite syntax)
	// We need to determine if it's a SELECT to know how to format the response
	// Use the original statement type we stored during PREPARE
	isSelect := stmt.OriginalType == StatementSelect

	// Execute query
	rs, err := s.handler.HandleQuery(session, finalQuery)
	if err != nil {
		s.writeMySQLErr(conn, 1, err)
		return
	}

	log.Debug().
		Uint64("conn_id", session.ConnID).
		Uint32("stmt_id", stmtID).
		Int("original_type", int(stmt.OriginalType)).
		Bool("is_select", isSelect).
		Int64("rows_affected", func() int64 {
			if rs != nil {
				return rs.RowsAffected
			}
			return -1
		}()).
		Msg("Prepared statement result")

	// Use the original statement type to determine response format
	if isSelect {
		// Write binary result set for SELECT queries
		if rs != nil {
			s.writeBinaryResultSet(conn, 1, rs)
		} else {
			// Empty result set
			s.writeBinaryResultSet(conn, 1, &ResultSet{Columns: []ColumnDef{}, Rows: [][]interface{}{}})
		}
	} else {
		// Write OK packet for DML queries
		rowsAffected := int64(0)
		if rs != nil {
			rowsAffected = rs.RowsAffected
		}
		s.writeOK(conn, 1, rowsAffected)
	}
}

// Helper functions

func countPlaceholders(query string) int {
	count := 0
	inString := false
	escapeNext := false

	for _, ch := range query {
		if escapeNext {
			escapeNext = false
			continue
		}

		if ch == '\\' {
			escapeNext = true
			continue
		}

		if ch == '\'' {
			inString = !inString
			continue
		}

		if !inString && ch == '?' {
			count++
		}
	}

	return count
}

func parseParamValue(payload []byte, offset int, paramType byte) (int, interface{}, error) {
	const (
		MYSQL_TYPE_DECIMAL     = 0x00
		MYSQL_TYPE_TINY        = 0x01
		MYSQL_TYPE_SHORT       = 0x02
		MYSQL_TYPE_LONG        = 0x03
		MYSQL_TYPE_FLOAT       = 0x04
		MYSQL_TYPE_DOUBLE      = 0x05
		MYSQL_TYPE_NULL        = 0x06
		MYSQL_TYPE_TIMESTAMP   = 0x07
		MYSQL_TYPE_LONGLONG    = 0x08
		MYSQL_TYPE_INT24       = 0x09
		MYSQL_TYPE_DATE        = 0x0A
		MYSQL_TYPE_TIME        = 0x0B
		MYSQL_TYPE_DATETIME    = 0x0C
		MYSQL_TYPE_YEAR        = 0x0D
		MYSQL_TYPE_VARCHAR     = 0x0F
		MYSQL_TYPE_BIT         = 0x10
		MYSQL_TYPE_NEWDECIMAL  = 0xF6
		MYSQL_TYPE_ENUM        = 0xF7
		MYSQL_TYPE_SET         = 0xF8
		MYSQL_TYPE_TINY_BLOB   = 0xF9
		MYSQL_TYPE_MEDIUM_BLOB = 0xFA
		MYSQL_TYPE_LONG_BLOB   = 0xFB
		MYSQL_TYPE_BLOB        = 0xFC
		MYSQL_TYPE_VAR_STRING  = 0xFD
		MYSQL_TYPE_STRING      = 0xFE
		MYSQL_TYPE_GEOMETRY    = 0xFF
	)

	switch paramType {
	case MYSQL_TYPE_NULL:
		return offset, nil, nil

	case MYSQL_TYPE_TINY:
		if len(payload) < offset+1 {
			return offset, nil, fmt.Errorf("not enough data for TINY")
		}
		return offset + 1, int8(payload[offset]), nil

	case MYSQL_TYPE_SHORT, MYSQL_TYPE_YEAR:
		if len(payload) < offset+2 {
			return offset, nil, fmt.Errorf("not enough data for SHORT")
		}
		return offset + 2, int16(binary.LittleEndian.Uint16(payload[offset:])), nil

	case MYSQL_TYPE_LONG, MYSQL_TYPE_INT24:
		if len(payload) < offset+4 {
			return offset, nil, fmt.Errorf("not enough data for LONG")
		}
		return offset + 4, int32(binary.LittleEndian.Uint32(payload[offset:])), nil

	case MYSQL_TYPE_LONGLONG:
		if len(payload) < offset+8 {
			return offset, nil, fmt.Errorf("not enough data for LONGLONG")
		}
		return offset + 8, int64(binary.LittleEndian.Uint64(payload[offset:])), nil

	case MYSQL_TYPE_FLOAT:
		if len(payload) < offset+4 {
			return offset, nil, fmt.Errorf("not enough data for FLOAT")
		}
		bits := binary.LittleEndian.Uint32(payload[offset:])
		return offset + 4, math.Float32frombits(bits), nil

	case MYSQL_TYPE_DOUBLE:
		if len(payload) < offset+8 {
			return offset, nil, fmt.Errorf("not enough data for DOUBLE")
		}
		bits := binary.LittleEndian.Uint64(payload[offset:])
		return offset + 8, math.Float64frombits(bits), nil

	case MYSQL_TYPE_STRING, MYSQL_TYPE_VAR_STRING, MYSQL_TYPE_VARCHAR,
		MYSQL_TYPE_BLOB, MYSQL_TYPE_TINY_BLOB, MYSQL_TYPE_MEDIUM_BLOB, MYSQL_TYPE_LONG_BLOB,
		MYSQL_TYPE_DECIMAL, MYSQL_TYPE_NEWDECIMAL,
		MYSQL_TYPE_BIT, MYSQL_TYPE_ENUM, MYSQL_TYPE_SET,
		MYSQL_TYPE_DATE, MYSQL_TYPE_DATETIME, MYSQL_TYPE_TIMESTAMP, MYSQL_TYPE_TIME,
		MYSQL_TYPE_GEOMETRY:
		// All these types use length-encoded string format
		length, n := readLengthEncodedInt(payload[offset:])
		if n == 0 {
			return offset, nil, fmt.Errorf("failed to read length")
		}
		offset += n
		if len(payload) < offset+int(length) {
			return offset, nil, fmt.Errorf("not enough data for string (need %d, have %d)", length, len(payload)-offset)
		}
		// Return as []byte to preserve binary data
		data := payload[offset : offset+int(length)]
		return offset + int(length), data, nil

	default:
		// For unknown types, try to read as length-encoded string
		length, n := readLengthEncodedInt(payload[offset:])
		if n == 0 {
			return offset, nil, fmt.Errorf("failed to read length for unknown type 0x%02X", paramType)
		}
		offset += n
		if len(payload) < offset+int(length) {
			return offset, nil, fmt.Errorf("not enough data for unknown type")
		}
		data := payload[offset : offset+int(length)]
		return offset + int(length), data, nil
	}
}

func readLengthEncodedInt(b []byte) (uint64, int) {
	if len(b) == 0 {
		return 0, 0
	}

	switch b[0] {
	case 0xFC:
		if len(b) < 3 {
			return 0, 1
		}
		return uint64(binary.LittleEndian.Uint16(b[1:3])), 3
	case 0xFD:
		if len(b) < 4 {
			return 0, 1
		}
		return uint64(binary.LittleEndian.Uint32(b[1:4])), 4
	case 0xFE:
		if len(b) < 9 {
			return 0, 1
		}
		return binary.LittleEndian.Uint64(b[1:9]), 9
	default:
		return uint64(b[0]), 1
	}
}

func buildQueryWithParams(query string, params []interface{}) string {
	result := ""
	paramIdx := 0
	inString := false
	escapeNext := false

	for _, ch := range query {
		if escapeNext {
			result += string(ch)
			escapeNext = false
			continue
		}

		if ch == '\\' {
			result += string(ch)
			escapeNext = true
			continue
		}

		if ch == '\'' {
			inString = !inString
			result += string(ch)
			continue
		}

		if !inString && ch == '?' {
			if paramIdx < len(params) {
				result += formatParam(params[paramIdx])
				paramIdx++
			} else {
				result += "?"
			}
		} else {
			result += string(ch)
		}
	}

	return result
}

func formatParam(param interface{}) string {
	if param == nil {
		return "NULL"
	}

	switch v := param.(type) {
	case string:
		return "'" + escapeString(v) + "'"
	case []byte:
		// Binary data needs to be escaped properly
		return "'" + escapeString(string(v)) + "'"
	case int8, int16, int32, int64, int:
		return fmt.Sprintf("%d", v)
	case uint8, uint16, uint32, uint64, uint:
		return fmt.Sprintf("%d", v)
	case float32:
		return fmt.Sprintf("%f", v)
	case float64:
		return fmt.Sprintf("%f", v)
	case bool:
		if v {
			return "1"
		}
		return "0"
	default:
		// Fallback: convert to string and escape
		return "'" + escapeString(fmt.Sprintf("%v", v)) + "'"
	}
}

func escapeString(s string) string {
	var escaped strings.Builder
	escaped.Grow(len(s))

	for _, ch := range s {
		switch ch {
		case '\x00':
			escaped.WriteString("\\0")
		case '\'':
			escaped.WriteString("''")
		case '"':
			escaped.WriteString("\\\"")
		case '\b':
			escaped.WriteString("\\b")
		case '\n':
			escaped.WriteString("\\n")
		case '\r':
			escaped.WriteString("\\r")
		case '\t':
			escaped.WriteString("\\t")
		case '\\':
			escaped.WriteString("\\\\")
		default:
			escaped.WriteRune(ch)
		}
	}

	return escaped.String()
}
