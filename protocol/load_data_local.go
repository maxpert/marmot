package protocol

import (
	"bytes"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
)

const maxLoadDataLocalBytes = 64 << 20 // 64 MiB safety cap for phase-1 LOCAL INFILE support
const capabilityClientLocalFiles uint32 = 1 << 7
const loadDataInsertBatchSize = 500

var (
	loadDataAnyPattern       = regexp.MustCompile(`(?is)^\s*LOAD\s+DATA\s+`)
	loadDataLocalPattern     = regexp.MustCompile(`(?is)^\s*LOAD\s+DATA\s+(?:LOW_PRIORITY|CONCURRENT\s+)?LOCAL\s+INFILE\s+`)
	loadDataTablePattern     = regexp.MustCompile(`(?is)\bINTO\s+TABLE\s+((?:` + "`" + `[^` + "`" + `]+` + "`" + `)|(?:[A-Za-z0-9_\.]+))`)
	loadDataLocalPathPattern = regexp.MustCompile(`(?is)\bLOCAL\s+INFILE\s+'((?:\\.|[^'])*)'`)
	loadDataFieldsTerminated = regexp.MustCompile(`(?is)\bFIELDS\s+TERMINATED\s+BY\s+'((?:\\.|[^'])*)'`)
	loadDataLinesTerminated  = regexp.MustCompile(`(?is)\bLINES\s+TERMINATED\s+BY\s+'((?:\\.|[^'])*)'`)
	loadDataIgnoreLines      = regexp.MustCompile(`(?is)\bIGNORE\s+(\d+)\s+LINES?\b`)
	loadDataColumnsPattern   = regexp.MustCompile(`(?is)\(([^)]*)\)\s*$`)
	loadDataUnsupported      = regexp.MustCompile(`(?is)\b(?:ENCLOSED\s+BY|ESCAPED\s+BY|STARTING\s+BY|SET\s+)`)
)

type loadDataLocalSpec struct {
	Table           string
	Columns         []string
	FieldTerminator string
	LineTerminator  string
	IgnoreLines     int
}

func isLoadDataStatement(sql string) bool {
	return loadDataAnyPattern.MatchString(sql)
}

func isLoadDataLocalStatement(sql string) bool {
	return loadDataLocalPattern.MatchString(sql)
}

func parseLoadDataLocalSpec(sql string) (*loadDataLocalSpec, error) {
	if !isLoadDataLocalStatement(sql) {
		return nil, fmt.Errorf("only LOAD DATA LOCAL INFILE is supported in v1")
	}
	if loadDataUnsupported.MatchString(sql) {
		return nil, fmt.Errorf("LOAD DATA LOCAL uses unsupported options in v1")
	}

	matches := loadDataTablePattern.FindStringSubmatch(sql)
	if len(matches) < 2 {
		return nil, fmt.Errorf("LOAD DATA LOCAL missing INTO TABLE target")
	}

	spec := &loadDataLocalSpec{
		Table:           strings.TrimSpace(matches[1]),
		FieldTerminator: "\t",
		LineTerminator:  "\n",
	}

	if m := loadDataFieldsTerminated.FindStringSubmatch(sql); len(m) > 1 {
		v, err := unescapeLoadDataToken(m[1])
		if err != nil {
			return nil, fmt.Errorf("invalid FIELDS TERMINATED BY value: %w", err)
		}
		spec.FieldTerminator = v
	}
	if m := loadDataLinesTerminated.FindStringSubmatch(sql); len(m) > 1 {
		v, err := unescapeLoadDataToken(m[1])
		if err != nil {
			return nil, fmt.Errorf("invalid LINES TERMINATED BY value: %w", err)
		}
		spec.LineTerminator = v
	}
	if m := loadDataIgnoreLines.FindStringSubmatch(sql); len(m) > 1 {
		ignore, err := strconv.Atoi(m[1])
		if err != nil {
			return nil, fmt.Errorf("invalid IGNORE LINES value: %w", err)
		}
		spec.IgnoreLines = ignore
	}
	if m := loadDataColumnsPattern.FindStringSubmatch(sql); len(m) > 1 {
		raw := strings.Split(m[1], ",")
		spec.Columns = make([]string, 0, len(raw))
		for _, col := range raw {
			col = strings.TrimSpace(col)
			if col == "" {
				continue
			}
			spec.Columns = append(spec.Columns, col)
		}
	}

	if spec.FieldTerminator == "" {
		return nil, fmt.Errorf("FIELDS TERMINATED BY must not be empty")
	}
	if spec.LineTerminator == "" {
		return nil, fmt.Errorf("LINES TERMINATED BY must not be empty")
	}
	return spec, nil
}

func unescapeLoadDataToken(s string) (string, error) {
	if !strings.Contains(s, `\`) {
		return s, nil
	}
	// Reuse Go string unquoting rules by wrapping in double quotes.
	v, err := strconv.Unquote(`"` + s + `"`)
	if err != nil {
		return "", err
	}
	return v, nil
}

func buildInsertForLoadData(spec *loadDataLocalSpec, fieldCount, rowCount int) string {
	rowPlaceholder := make([]string, fieldCount)
	for i := range rowPlaceholder {
		rowPlaceholder[i] = "?"
	}
	rows := make([]string, rowCount)
	for i := range rows {
		rows[i] = "(" + strings.Join(rowPlaceholder, ", ") + ")"
	}
	var cols string
	if len(spec.Columns) > 0 {
		cols = "(" + strings.Join(spec.Columns, ", ") + ")"
	}
	if cols != "" {
		return fmt.Sprintf("INSERT INTO %s %s VALUES %s", spec.Table, cols, strings.Join(rows, ", "))
	}
	return fmt.Sprintf("INSERT INTO %s VALUES %s", spec.Table, strings.Join(rows, ", "))
}

func splitLoadDataRows(data []byte, spec *loadDataLocalSpec) [][]string {
	rawRows := strings.Split(string(data), spec.LineTerminator)
	rows := make([][]string, 0, len(rawRows))
	for i, row := range rawRows {
		if i < spec.IgnoreLines {
			continue
		}
		if row == "" {
			continue
		}
		row = strings.TrimSuffix(row, "\r")
		fields := strings.Split(row, spec.FieldTerminator)
		rows = append(rows, fields)
	}
	return rows
}

func (s *MySQLServer) processLoadDataQuery(conn io.ReadWriter, session *ConnectionSession, query string) error {
	if !s.localInfile {
		return fmt.Errorf("LOAD DATA LOCAL INFILE is disabled")
	}
	if session.ClientCaps&capabilityClientLocalFiles == 0 {
		return fmt.Errorf("client does not advertise LOCAL INFILE capability")
	}
	if !isLoadDataLocalStatement(query) {
		return fmt.Errorf("LOAD DATA INFILE without LOCAL is not supported")
	}

	filename := ""
	if m := loadDataLocalPathPattern.FindStringSubmatch(query); len(m) > 1 {
		filename = m[1]
	}

	if err := s.writePacket(conn, 1, append([]byte{0xFB}, []byte(filename)...)); err != nil {
		return err
	}

	var payload bytes.Buffer
	lastSeq := byte(1)
	for {
		packet, seq, readErr := s.readPacketWithSeq(conn)
		if readErr != nil {
			return fmt.Errorf("failed reading LOCAL INFILE payload: %w", readErr)
		}
		lastSeq = seq
		if len(packet) == 0 {
			break // EOF marker from client
		}
		if payload.Len()+len(packet) > maxLoadDataLocalBytes {
			return fmt.Errorf("LOCAL INFILE payload exceeds %d bytes", maxLoadDataLocalBytes)
		}
		payload.Write(packet)
	}

	if handler, ok := s.handler.(LoadDataHandler); ok {
		rs, execErr := handler.HandleLoadData(session, query, payload.Bytes())
		if execErr != nil {
			return execErr
		}
		rowsAffected := int64(0)
		lastInsertID := int64(0)
		if rs != nil {
			rowsAffected = rs.RowsAffected
			lastInsertID = rs.LastInsertId
		}
		return s.writeOK(conn, lastSeq+1, rowsAffected, lastInsertID)
	}

	spec, err := parseLoadDataLocalSpec(query)
	if err != nil {
		return err
	}
	rows, lastInsertID, _, err := executeLocalLoadDataRowsWithHandler(session, s.handler, spec, payload.Bytes())
	if err != nil {
		return err
	}
	return s.writeOK(conn, lastSeq+1, rows, lastInsertID)
}

// ExecuteLoadDataLocal applies LOAD DATA LOCAL payload by translating to parameterized INSERT batches.
func ExecuteLoadDataLocal(session *ConnectionSession, handler ConnectionHandler, query string, data []byte) (*ResultSet, error) {
	spec, err := parseLoadDataLocalSpec(query)
	if err != nil {
		return nil, err
	}

	rows, lastInsertID, committedTxnID, err := executeLocalLoadDataRowsWithHandler(session, handler, spec, data)
	if err != nil {
		return nil, err
	}
	return &ResultSet{
		RowsAffected:   rows,
		LastInsertId:   lastInsertID,
		CommittedTxnId: committedTxnID,
	}, nil
}

func executeLocalLoadDataRowsWithHandler(session *ConnectionSession, handler ConnectionHandler, spec *loadDataLocalSpec, data []byte) (int64, int64, uint64, error) {
	rows := splitLoadDataRows(data, spec)
	if len(rows) == 0 {
		return 0, 0, 0, nil
	}

	startedTxn := false
	if !session.InTransaction() {
		if _, err := handler.HandleQuery(session, "BEGIN", nil); err != nil {
			return 0, 0, 0, err
		}
		startedTxn = true
	}

	var inserted int64
	var lastInsertID int64
	var committedTxnID uint64
	for i := 0; i < len(rows); {
		fieldCount := len(rows[i])
		if len(spec.Columns) > 0 && fieldCount != len(spec.Columns) {
			if startedTxn {
				_, _ = handler.HandleQuery(session, "ROLLBACK", nil)
			}
			return 0, 0, 0, fmt.Errorf("LOAD DATA LOCAL row column mismatch: got %d, expected %d", fieldCount, len(spec.Columns))
		}

		batchEnd := i + loadDataInsertBatchSize
		if batchEnd > len(rows) {
			batchEnd = len(rows)
		}
		for j := i + 1; j < batchEnd; j++ {
			if len(rows[j]) != fieldCount {
				batchEnd = j
				break
			}
		}

		stmt := buildInsertForLoadData(spec, fieldCount, batchEnd-i)
		params := make([]interface{}, 0, (batchEnd-i)*fieldCount)
		for _, row := range rows[i:batchEnd] {
			for _, col := range row {
				params = append(params, col)
			}
		}

		rs, err := handler.HandleQuery(session, stmt, params)
		if err != nil {
			if startedTxn {
				_, _ = handler.HandleQuery(session, "ROLLBACK", nil)
			}
			return 0, 0, 0, err
		}
		if rs != nil {
			inserted += rs.RowsAffected
			lastInsertID = rs.LastInsertId
			if rs.CommittedTxnId > 0 {
				committedTxnID = rs.CommittedTxnId
			}
		} else {
			inserted += int64(batchEnd - i)
		}
		i = batchEnd
	}

	if startedTxn {
		rs, err := handler.HandleQuery(session, "COMMIT", nil)
		if err != nil {
			_, _ = handler.HandleQuery(session, "ROLLBACK", nil)
			return 0, 0, 0, err
		}
		if rs != nil && rs.RowsAffected > inserted {
			inserted = rs.RowsAffected
		}
		if rs != nil {
			lastInsertID = rs.LastInsertId
			if rs.CommittedTxnId > 0 {
				committedTxnID = rs.CommittedTxnId
			}
		}
	}
	return inserted, lastInsertID, committedTxnID, nil
}
