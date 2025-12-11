package db

import (
	"fmt"
	"math"
	"strings"

	"github.com/mattn/go-sqlite3"
)

// RegisterMySQLStringFuncs registers MySQL-compatible string functions for SQLite
func RegisterMySQLStringFuncs(conn *sqlite3.SQLiteConn) error {
	funcs := []struct {
		name string
		impl interface{}
		pure bool
	}{
		{"concat_ws", mysqlConcatWS, true},
		{"left", mysqlLeft, true},
		{"right", mysqlRight, true},
		{"reverse", mysqlReverse, true},
		{"lpad", mysqlLPad, true},
		{"rpad", mysqlRPad, true},
		{"repeat", mysqlRepeat, true},
		{"find_in_set", mysqlFindInSet, true},
		{"field", mysqlField, true},
		{"elt", mysqlElt, true},
		{"substring_index", mysqlSubstringIndex, true},
		{"if", mysqlIf, true},
	}

	for _, f := range funcs {
		if err := conn.RegisterFunc(f.name, f.impl, f.pure); err != nil {
			return fmt.Errorf("failed to register function %s: %w", f.name, err)
		}
	}
	return nil
}

func mysqlConcatWS(args ...interface{}) interface{} {
	if len(args) < 2 {
		return nil
	}

	sep, ok := args[0].(string)
	if !ok {
		return nil
	}

	var parts []string
	for _, arg := range args[1:] {
		if arg == nil {
			continue
		}
		if s, ok := arg.(string); ok {
			parts = append(parts, s)
		} else if i, ok := arg.(int64); ok {
			parts = append(parts, fmt.Sprintf("%d", i))
		} else if f, ok := arg.(float64); ok {
			parts = append(parts, fmt.Sprintf("%g", f))
		}
	}
	return strings.Join(parts, sep)
}

func mysqlLeft(str interface{}, length interface{}) interface{} {
	if str == nil || length == nil {
		return nil
	}

	s, ok := str.(string)
	if !ok {
		return nil
	}

	var l int64
	switch v := length.(type) {
	case int64:
		l = v
	case float64:
		l = int64(v)
	default:
		return nil
	}

	if l <= 0 {
		return ""
	}

	runes := []rune(s)
	if l >= int64(len(runes)) {
		return s
	}
	return string(runes[:l])
}

func mysqlRight(str interface{}, length interface{}) interface{} {
	if str == nil || length == nil {
		return nil
	}

	s, ok := str.(string)
	if !ok {
		return nil
	}

	var l int64
	switch v := length.(type) {
	case int64:
		l = v
	case float64:
		l = int64(v)
	default:
		return nil
	}

	if l <= 0 {
		return ""
	}

	runes := []rune(s)
	if l >= int64(len(runes)) {
		return s
	}
	return string(runes[len(runes)-int(l):])
}

func mysqlReverse(str interface{}) interface{} {
	if str == nil {
		return nil
	}

	s, ok := str.(string)
	if !ok {
		return nil
	}

	runes := []rune(s)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes)
}

func mysqlLPad(str interface{}, length interface{}, padStr interface{}) interface{} {
	if str == nil || length == nil || padStr == nil {
		return nil
	}

	s, ok := str.(string)
	if !ok {
		return nil
	}

	pad, ok := padStr.(string)
	if !ok || pad == "" {
		return nil
	}

	var l int64
	switch v := length.(type) {
	case int64:
		l = v
	case float64:
		l = int64(v)
	default:
		return nil
	}

	if l <= 0 {
		return ""
	}

	runes := []rune(s)
	targetLen := int(l)

	if len(runes) >= targetLen {
		return string(runes[:targetLen])
	}

	padRunes := []rune(pad)
	if len(padRunes) == 0 {
		return nil
	}

	neededPad := targetLen - len(runes)
	var result []rune

	for len(result) < neededPad {
		for _, r := range padRunes {
			if len(result) >= neededPad {
				break
			}
			result = append(result, r)
		}
	}

	result = append(result, runes...)
	return string(result)
}

func mysqlRPad(str interface{}, length interface{}, padStr interface{}) interface{} {
	if str == nil || length == nil || padStr == nil {
		return nil
	}

	s, ok := str.(string)
	if !ok {
		return nil
	}

	pad, ok := padStr.(string)
	if !ok || pad == "" {
		return nil
	}

	var l int64
	switch v := length.(type) {
	case int64:
		l = v
	case float64:
		l = int64(v)
	default:
		return nil
	}

	if l <= 0 {
		return ""
	}

	runes := []rune(s)
	targetLen := int(l)

	if len(runes) >= targetLen {
		return string(runes[:targetLen])
	}

	padRunes := []rune(pad)
	if len(padRunes) == 0 {
		return nil
	}

	result := make([]rune, len(runes))
	copy(result, runes)

	for len(result) < targetLen {
		for _, r := range padRunes {
			if len(result) >= targetLen {
				break
			}
			result = append(result, r)
		}
	}

	return string(result[:targetLen])
}

func mysqlRepeat(str interface{}, count interface{}) interface{} {
	if str == nil || count == nil {
		return nil
	}

	s, ok := str.(string)
	if !ok {
		return nil
	}

	var c int64
	switch v := count.(type) {
	case int64:
		c = v
	case float64:
		c = int64(v)
	default:
		return nil
	}

	if c <= 0 {
		return ""
	}

	if c > 1000000 {
		return nil
	}

	return strings.Repeat(s, int(c))
}

func mysqlFindInSet(str interface{}, strList interface{}) interface{} {
	if str == nil || strList == nil {
		return nil
	}

	needle, ok := str.(string)
	if !ok {
		return nil
	}

	list, ok := strList.(string)
	if !ok {
		return nil
	}

	if list == "" {
		return int64(0)
	}

	parts := strings.Split(list, ",")
	for i, part := range parts {
		if part == needle {
			return int64(i + 1)
		}
	}
	return int64(0)
}

func mysqlField(args ...interface{}) interface{} {
	if len(args) < 2 {
		return int64(0)
	}

	if args[0] == nil {
		return int64(0)
	}

	needle := fmt.Sprintf("%v", args[0])

	for i, arg := range args[1:] {
		if arg == nil {
			continue
		}
		if fmt.Sprintf("%v", arg) == needle {
			return int64(i + 1)
		}
	}
	return int64(0)
}

func mysqlElt(args ...interface{}) interface{} {
	if len(args) < 2 {
		return nil
	}

	if args[0] == nil {
		return nil
	}

	var idx int64
	switch v := args[0].(type) {
	case int64:
		idx = v
	case float64:
		idx = int64(v)
	default:
		return nil
	}

	if idx < 1 || idx > int64(len(args)-1) {
		return nil
	}

	return args[idx]
}

func mysqlSubstringIndex(str interface{}, delim interface{}, count interface{}) interface{} {
	if str == nil || delim == nil || count == nil {
		return nil
	}

	s, ok := str.(string)
	if !ok {
		return nil
	}

	d, ok := delim.(string)
	if !ok || d == "" {
		return nil
	}

	var c int64
	switch v := count.(type) {
	case int64:
		c = v
	case float64:
		c = int64(v)
	default:
		return nil
	}

	if c == 0 {
		return ""
	}

	if c > 0 {
		parts := strings.SplitN(s, d, int(c)+1)
		if len(parts) <= int(c) {
			return s
		}
		return strings.Join(parts[:c], d)
	}

	absCount := int(-c)
	parts := strings.Split(s, d)
	if len(parts) <= absCount {
		return s
	}

	return strings.Join(parts[len(parts)-absCount:], d)
}

func mysqlIf(condition interface{}, trueVal interface{}, falseVal interface{}) interface{} {
	if condition == nil {
		return falseVal
	}

	var cond bool
	switch v := condition.(type) {
	case bool:
		cond = v
	case int64:
		cond = v != 0
	case float64:
		cond = v != 0 && !math.IsNaN(v)
	case string:
		cond = v != "" && v != "0"
	default:
		cond = false
	}

	if cond {
		return trueVal
	}
	return falseVal
}
