package db

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"math"
	"math/rand"

	"github.com/mattn/go-sqlite3"
)

// toFloat64 converts interface{} to float64, handling both int64 and float64
func toFloat64(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case int64:
		return float64(val)
	case int:
		return float64(val)
	default:
		return 0
	}
}

// RegisterMySQLMathFuncs registers MySQL-compatible math and hash functions
func RegisterMySQLMathFuncs(conn *sqlite3.SQLiteConn) error {
	funcs := []struct {
		name string
		impl interface{}
		pure bool
	}{
		{"rand", mysqlRand, false},
		{"sign", mysqlSign, true},
		{"truncate", mysqlTruncate, true},
		{"degrees", mysqlDegrees, true},
		{"radians", mysqlRadians, true},
		{"pi", mysqlPi, true},
		{"cot", mysqlCot, true},
		{"log", mysqlLog, true},
		{"log2", mysqlLog2, true},
		{"pow", mysqlPow, true},
		{"power", mysqlPow, true},
		{"md5", mysqlMD5, true},
		{"sha1", mysqlSHA1, true},
		{"sha", mysqlSHA1, true},
		{"sha2", mysqlSHA2, true},
	}

	for _, f := range funcs {
		if err := conn.RegisterFunc(f.name, f.impl, f.pure); err != nil {
			return err
		}
	}
	return nil
}

// mysqlRand returns random float 0-1, optionally seeded
// RAND() - returns different value each call (non-deterministic)
// RAND(seed) - returns deterministic value based on seed
func mysqlRand(args ...interface{}) float64 {
	if len(args) > 0 {
		if seed, ok := args[0].(int64); ok {
			r := rand.New(rand.NewSource(seed))
			return r.Float64()
		}
	}
	return rand.Float64()
}

// mysqlSign returns -1, 0, or 1 based on the sign of x
func mysqlSign(x float64) int {
	if x < 0 {
		return -1
	}
	if x > 0 {
		return 1
	}
	return 0
}

// mysqlTruncate truncates x to d decimal places without rounding
func mysqlTruncate(x float64, d int64) float64 {
	multiplier := math.Pow(10, float64(d))
	return math.Trunc(x*multiplier) / multiplier
}

// mysqlDegrees converts radians to degrees
func mysqlDegrees(radians float64) float64 {
	return radians * 180.0 / math.Pi
}

// mysqlRadians converts degrees to radians
func mysqlRadians(degrees float64) float64 {
	return degrees * math.Pi / 180.0
}

// mysqlPi returns the value of pi
func mysqlPi() float64 {
	return math.Pi
}

// mysqlCot returns the cotangent (1/tan(x))
func mysqlCot(x float64) float64 {
	return 1.0 / math.Tan(x)
}

// mysqlLog returns natural logarithm or logarithm with base
// LOG(x) - natural logarithm of x
// LOG(base, x) - logarithm of x with specified base
func mysqlLog(args ...interface{}) interface{} {
	if len(args) == 0 {
		return nil
	}

	if len(args) == 1 {
		x := toFloat64(args[0])
		if x <= 0 {
			return nil
		}
		return math.Log(x)
	}

	if len(args) == 2 {
		base := toFloat64(args[0])
		x := toFloat64(args[1])
		if base <= 0 || base == 1 || x <= 0 {
			return nil
		}
		return math.Log(x) / math.Log(base)
	}

	return nil
}

// mysqlLog2 returns base-2 logarithm
func mysqlLog2(val interface{}) interface{} {
	x := toFloat64(val)
	if x <= 0 {
		return nil
	}
	return math.Log2(x)
}

// mysqlPow returns x raised to the power of y
func mysqlPow(xVal, yVal interface{}) float64 {
	x := toFloat64(xVal)
	y := toFloat64(yVal)
	return math.Pow(x, y)
}

// mysqlMD5 returns MD5 hash as lowercase hex string
func mysqlMD5(s string) string {
	hash := md5.Sum([]byte(s))
	return hex.EncodeToString(hash[:])
}

// mysqlSHA1 returns SHA-1 hash as lowercase hex string
func mysqlSHA1(s string) string {
	hash := sha1.Sum([]byte(s))
	return hex.EncodeToString(hash[:])
}

// mysqlSHA2 returns SHA-2 hash with specified bit length
// Supported lengths: 224, 256, 384, 512
// Returns NULL for invalid lengths
func mysqlSHA2(s string, bits int64) interface{} {
	switch bits {
	case 224:
		hash := sha256.Sum224([]byte(s))
		return hex.EncodeToString(hash[:])
	case 256:
		hash := sha256.Sum256([]byte(s))
		return hex.EncodeToString(hash[:])
	case 384:
		hash := sha512.Sum384([]byte(s))
		return hex.EncodeToString(hash[:])
	case 512:
		hash := sha512.Sum512([]byte(s))
		return hex.EncodeToString(hash[:])
	default:
		return nil
	}
}
