package coordinator

import (
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// materializeSQLWithParams replaces top-level '?' placeholders with SQL literals.
// This is used only for statement-based DML fallback when CDC hooks are unavailable.
func materializeSQLWithParams(sql string, params []interface{}) (string, error) {
	if !strings.Contains(sql, "?") {
		if len(params) > 0 {
			return "", fmt.Errorf("statement has no placeholders but got %d parameters", len(params))
		}
		return sql, nil
	}

	var b strings.Builder
	b.Grow(len(sql) + len(params)*8)

	inSingle := false
	inDouble := false
	inBacktick := false
	paramIdx := 0

	for i := 0; i < len(sql); i++ {
		ch := sql[i]

		if ch == '?' && !inSingle && !inDouble && !inBacktick {
			if paramIdx >= len(params) {
				return "", fmt.Errorf("placeholder count exceeds parameter count")
			}
			lit, err := toSQLLiteral(params[paramIdx])
			if err != nil {
				return "", err
			}
			b.WriteString(lit)
			paramIdx++
			continue
		}

		b.WriteByte(ch)

		switch ch {
		case '\'':
			if inDouble || inBacktick {
				continue
			}
			if inSingle && i+1 < len(sql) && sql[i+1] == '\'' {
				b.WriteByte(sql[i+1])
				i++
				continue
			}
			inSingle = !inSingle
		case '"':
			if inSingle || inBacktick {
				continue
			}
			if inDouble && i+1 < len(sql) && sql[i+1] == '"' {
				b.WriteByte(sql[i+1])
				i++
				continue
			}
			inDouble = !inDouble
		case '`':
			if inSingle || inDouble {
				continue
			}
			if inBacktick && i+1 < len(sql) && sql[i+1] == '`' {
				b.WriteByte(sql[i+1])
				i++
				continue
			}
			inBacktick = !inBacktick
		}
	}

	if paramIdx != len(params) {
		return "", fmt.Errorf("parameter count exceeds placeholder count")
	}

	return b.String(), nil
}

func toSQLLiteral(v interface{}) (string, error) {
	switch x := v.(type) {
	case nil:
		return "NULL", nil
	case string:
		return "'" + strings.ReplaceAll(x, "'", "''") + "'", nil
	case []byte:
		return "X'" + strings.ToUpper(hex.EncodeToString(x)) + "'", nil
	case bool:
		if x {
			return "1", nil
		}
		return "0", nil
	case time.Time:
		return "'" + x.UTC().Format("2006-01-02 15:04:05.999999999") + "'", nil
	case int:
		return strconv.Itoa(x), nil
	case int8:
		return strconv.FormatInt(int64(x), 10), nil
	case int16:
		return strconv.FormatInt(int64(x), 10), nil
	case int32:
		return strconv.FormatInt(int64(x), 10), nil
	case int64:
		return strconv.FormatInt(x, 10), nil
	case uint:
		return strconv.FormatUint(uint64(x), 10), nil
	case uint8:
		return strconv.FormatUint(uint64(x), 10), nil
	case uint16:
		return strconv.FormatUint(uint64(x), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(x), 10), nil
	case uint64:
		return strconv.FormatUint(x, 10), nil
	case float32:
		if math.IsNaN(float64(x)) || math.IsInf(float64(x), 0) {
			return "", fmt.Errorf("unsupported float parameter value: %v", x)
		}
		return strconv.FormatFloat(float64(x), 'g', -1, 32), nil
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return "", fmt.Errorf("unsupported float parameter value: %v", x)
		}
		return strconv.FormatFloat(x, 'g', -1, 64), nil
	default:
		return "'" + strings.ReplaceAll(fmt.Sprintf("%v", x), "'", "''") + "'", nil
	}
}
