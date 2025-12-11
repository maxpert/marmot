package db

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/mattn/go-sqlite3"
)

const (
	mysqlDateLayout     = "2006-01-02"
	mysqlTimeLayout     = "15:04:05"
	mysqlDateTimeLayout = "2006-01-02 15:04:05"
)

// RegisterMySQLDateTimeFuncs registers MySQL-compatible date/time functions
func RegisterMySQLDateTimeFuncs(conn *sqlite3.SQLiteConn) error {
	funcs := []struct {
		name string
		impl interface{}
		pure bool
	}{
		{"year", mysqlYear, true},
		{"month", mysqlMonth, true},
		{"day", mysqlDay, true},
		{"hour", mysqlHour, true},
		{"minute", mysqlMinute, true},
		{"second", mysqlSecond, true},
		{"dayofweek", mysqlDayOfWeek, true},
		{"dayofyear", mysqlDayOfYear, true},
		{"weekday", mysqlWeekday, true},
		{"week", mysqlWeek, true},
		{"quarter", mysqlQuarter, true},
		{"now", mysqlNow, false},
		{"curdate", mysqlCurDate, false},
		{"curtime", mysqlCurTime, false},
		{"utc_timestamp", mysqlUTCTimestamp, false},
		{"date_format", mysqlDateFormat, true},
		{"from_unixtime", mysqlFromUnixtime, true},
		{"unix_timestamp", mysqlUnixTimestamp, true},
		{"datediff", mysqlDateDiff, true},
		{"last_day", mysqlLastDay, true},
	}

	for _, f := range funcs {
		if err := conn.RegisterFunc(f.name, f.impl, f.pure); err != nil {
			return fmt.Errorf("failed to register function %s: %w", f.name, err)
		}
	}
	return nil
}

// parseDateTime attempts to parse a date/time string in multiple formats
func parseDateTime(dateStr interface{}) (time.Time, bool, error) {
	if dateStr == nil {
		return time.Time{}, true, nil
	}

	var str string
	switch v := dateStr.(type) {
	case string:
		str = v
	case []byte:
		str = string(v)
	default:
		return time.Time{}, false, fmt.Errorf("invalid date type: %T", dateStr)
	}

	str = strings.TrimSpace(str)
	if str == "" {
		return time.Time{}, true, nil
	}

	layouts := []string{
		mysqlDateTimeLayout,
		mysqlDateLayout,
		time.RFC3339,
		"2006-01-02T15:04:05",
	}

	var lastErr error
	for _, layout := range layouts {
		if t, err := time.Parse(layout, str); err == nil {
			return t, false, nil
		} else {
			lastErr = err
		}
	}

	return time.Time{}, false, fmt.Errorf("unable to parse date '%s': %w", str, lastErr)
}

// mysqlYear extracts the year from a date
func mysqlYear(dateStr interface{}) (interface{}, error) {
	t, isNull, err := parseDateTime(dateStr)
	if err != nil {
		return nil, err
	}
	if isNull {
		return nil, nil
	}
	return t.Year(), nil
}

// mysqlMonth extracts the month (1-12) from a date
func mysqlMonth(dateStr interface{}) (interface{}, error) {
	t, isNull, err := parseDateTime(dateStr)
	if err != nil {
		return nil, err
	}
	if isNull {
		return nil, nil
	}
	return int(t.Month()), nil
}

// mysqlDay extracts the day (1-31) from a date
func mysqlDay(dateStr interface{}) (interface{}, error) {
	t, isNull, err := parseDateTime(dateStr)
	if err != nil {
		return nil, err
	}
	if isNull {
		return nil, nil
	}
	return t.Day(), nil
}

// mysqlHour extracts the hour (0-23) from a datetime
func mysqlHour(dateStr interface{}) (interface{}, error) {
	t, isNull, err := parseDateTime(dateStr)
	if err != nil {
		return nil, err
	}
	if isNull {
		return nil, nil
	}
	return t.Hour(), nil
}

// mysqlMinute extracts the minute (0-59) from a datetime
func mysqlMinute(dateStr interface{}) (interface{}, error) {
	t, isNull, err := parseDateTime(dateStr)
	if err != nil {
		return nil, err
	}
	if isNull {
		return nil, nil
	}
	return t.Minute(), nil
}

// mysqlSecond extracts the second (0-59) from a datetime
func mysqlSecond(dateStr interface{}) (interface{}, error) {
	t, isNull, err := parseDateTime(dateStr)
	if err != nil {
		return nil, err
	}
	if isNull {
		return nil, nil
	}
	return t.Second(), nil
}

// mysqlDayOfWeek returns the day of week (1=Sunday, 2=Monday, ..., 7=Saturday)
func mysqlDayOfWeek(dateStr interface{}) (interface{}, error) {
	t, isNull, err := parseDateTime(dateStr)
	if err != nil {
		return nil, err
	}
	if isNull {
		return nil, nil
	}
	return int(t.Weekday()) + 1, nil
}

// mysqlDayOfYear returns the day of year (1-366)
func mysqlDayOfYear(dateStr interface{}) (interface{}, error) {
	t, isNull, err := parseDateTime(dateStr)
	if err != nil {
		return nil, err
	}
	if isNull {
		return nil, nil
	}
	return t.YearDay(), nil
}

// mysqlWeekday returns the weekday index (0=Monday, 1=Tuesday, ..., 6=Sunday)
func mysqlWeekday(dateStr interface{}) (interface{}, error) {
	t, isNull, err := parseDateTime(dateStr)
	if err != nil {
		return nil, err
	}
	if isNull {
		return nil, nil
	}
	wd := int(t.Weekday())
	if wd == 0 {
		return 6, nil
	}
	return wd - 1, nil
}

// mysqlWeek returns the week number (0-53)
func mysqlWeek(dateStr interface{}) (interface{}, error) {
	t, isNull, err := parseDateTime(dateStr)
	if err != nil {
		return nil, err
	}
	if isNull {
		return nil, nil
	}
	_, week := t.ISOWeek()
	return week, nil
}

// mysqlQuarter returns the quarter of the year (1-4)
func mysqlQuarter(dateStr interface{}) (interface{}, error) {
	t, isNull, err := parseDateTime(dateStr)
	if err != nil {
		return nil, err
	}
	if isNull {
		return nil, nil
	}
	month := int(t.Month())
	return (month-1)/3 + 1, nil
}

// mysqlNow returns the current datetime
func mysqlNow() string {
	return time.Now().Format(mysqlDateTimeLayout)
}

// mysqlCurDate returns the current date
func mysqlCurDate() string {
	return time.Now().Format(mysqlDateLayout)
}

// mysqlCurTime returns the current time
func mysqlCurTime() string {
	return time.Now().Format(mysqlTimeLayout)
}

// mysqlUTCTimestamp returns the current UTC datetime
func mysqlUTCTimestamp() string {
	return time.Now().UTC().Format(mysqlDateTimeLayout)
}

// mysqlDateFormat formats a date according to MySQL format specifiers
func mysqlDateFormat(dateStr interface{}, format interface{}) (interface{}, error) {
	if dateStr == nil || format == nil {
		return nil, nil
	}

	t, isNull, err := parseDateTime(dateStr)
	if err != nil {
		return nil, err
	}
	if isNull {
		return nil, nil
	}

	formatStr, ok := format.(string)
	if !ok {
		return nil, fmt.Errorf("format must be a string")
	}

	result := formatStr
	monthNames := []string{"January", "February", "March", "April", "May", "June",
		"July", "August", "September", "October", "November", "December"}
	monthAbbr := []string{"Jan", "Feb", "Mar", "Apr", "May", "Jun",
		"Jul", "Aug", "Sep", "Oct", "Nov", "Dec"}

	replacements := map[string]string{
		"%Y": fmt.Sprintf("%04d", t.Year()),
		"%y": fmt.Sprintf("%02d", t.Year()%100),
		"%m": fmt.Sprintf("%02d", t.Month()),
		"%c": fmt.Sprintf("%d", t.Month()),
		"%d": fmt.Sprintf("%02d", t.Day()),
		"%e": fmt.Sprintf("%d", t.Day()),
		"%H": fmt.Sprintf("%02d", t.Hour()),
		"%h": fmt.Sprintf("%02d", ((t.Hour()+11)%12)+1),
		"%I": fmt.Sprintf("%02d", ((t.Hour()+11)%12)+1),
		"%i": fmt.Sprintf("%02d", t.Minute()),
		"%s": fmt.Sprintf("%02d", t.Second()),
		"%S": fmt.Sprintf("%02d", t.Second()),
		"%p": func() string {
			if t.Hour() < 12 {
				return "AM"
			}
			return "PM"
		}(),
		"%W": t.Weekday().String(),
		"%a": t.Weekday().String()[:3],
		"%M": monthNames[t.Month()-1],
		"%b": monthAbbr[t.Month()-1],
		"%w": fmt.Sprintf("%d", t.Weekday()),
		"%j": fmt.Sprintf("%03d", t.YearDay()),
	}

	for pattern, replacement := range replacements {
		result = strings.ReplaceAll(result, pattern, replacement)
	}

	return result, nil
}

// mysqlFromUnixtime converts a Unix timestamp to datetime
func mysqlFromUnixtime(timestamp interface{}) (interface{}, error) {
	if timestamp == nil {
		return nil, nil
	}

	var ts int64
	switch v := timestamp.(type) {
	case int64:
		ts = v
	case int:
		ts = int64(v)
	case float64:
		ts = int64(v)
	case string:
		var err error
		ts, err = strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid timestamp: %w", err)
		}
	default:
		return nil, fmt.Errorf("invalid timestamp type: %T", timestamp)
	}

	t := time.Unix(ts, 0).UTC()
	return t.Format(mysqlDateTimeLayout), nil
}

// mysqlUnixTimestamp converts a datetime to Unix timestamp
// UNIX_TIMESTAMP() with no args returns current time
// UNIX_TIMESTAMP(date) converts date to Unix timestamp
func mysqlUnixTimestamp(args ...interface{}) (interface{}, error) {
	if len(args) == 0 {
		return time.Now().Unix(), nil
	}

	if args[0] == nil {
		return nil, nil
	}

	t, isNull, err := parseDateTime(args[0])
	if err != nil {
		return nil, err
	}
	if isNull {
		return nil, nil
	}
	return t.Unix(), nil
}

// mysqlDateDiff returns the difference in days between two dates
func mysqlDateDiff(date1, date2 interface{}) (interface{}, error) {
	if date1 == nil || date2 == nil {
		return nil, nil
	}

	t1, _, err := parseDateTime(date1)
	if err != nil {
		return nil, fmt.Errorf("invalid date1: %w", err)
	}

	t2, _, err := parseDateTime(date2)
	if err != nil {
		return nil, fmt.Errorf("invalid date2: %w", err)
	}

	t1 = time.Date(t1.Year(), t1.Month(), t1.Day(), 0, 0, 0, 0, time.UTC)
	t2 = time.Date(t2.Year(), t2.Month(), t2.Day(), 0, 0, 0, 0, time.UTC)

	diff := t1.Sub(t2)
	hours := diff.Hours()
	var days int
	if hours >= 0 {
		days = int(hours/24 + 0.5)
	} else {
		days = int(hours/24 - 0.5)
	}

	return days, nil
}

// mysqlLastDay returns the last day of the month for a given date
func mysqlLastDay(dateStr interface{}) (interface{}, error) {
	if dateStr == nil {
		return nil, nil
	}

	t, _, err := parseDateTime(dateStr)
	if err != nil {
		return nil, err
	}
	if t.IsZero() {
		return nil, nil
	}

	firstOfNextMonth := time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, t.Location())
	lastDay := firstOfNextMonth.Add(-24 * time.Hour)

	return lastDay.Format(mysqlDateLayout), nil
}
