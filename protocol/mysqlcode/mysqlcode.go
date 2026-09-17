// Package mysqlcode is the single home for MySQL wire-protocol error-code and
// SQLSTATE numeric/string constants. It imports nothing from this repo, so
// any package - including ones below protocol, which cannot import protocol
// itself without creating an import cycle - can depend on it without pulling
// in protocol's error-mapping logic.
package mysqlcode

// MySQL error code constants.
const (
	ErrCodeUnknown         uint16 = 1105
	ErrCodeBadNull         uint16 = 1048
	ErrCodeTableExists     uint16 = 1050
	ErrCodeBadField        uint16 = 1054
	ErrCodeDupFieldName    uint16 = 1060
	ErrCodeDupEntry        uint16 = 1062
	ErrCodeParseError      uint16 = 1064
	ErrCodeTooBigRowsize   uint16 = 1118
	ErrCodeNoSuchTable     uint16 = 1146
	ErrCodeNoDB            uint16 = 1046
	ErrCodeLockTimeout     uint16 = 1205
	ErrCodeDeadlock        uint16 = 1213
	ErrCodeReadOnly        uint16 = 1290
	ErrCodeServerShutdown  uint16 = 1053
	ErrCodeNoReferencedRow uint16 = 1452
	ErrCodeCheckConstraint uint16 = 3819

	// ErrCodeNotSupportedYet is MySQL's ER_NOT_SUPPORTED_YET: the server
	// understands the statement but does not implement this form of it.
	ErrCodeNotSupportedYet uint16 = 1235
)

// SQLSTATE constants.
const (
	SQLStateGeneral     = "HY000"
	SQLStateIntegrity   = "23000"
	SQLStateSyntax      = "42000"
	SQLStateDeadlock    = "40001"
	SQLStateTableExists = "42S01"
	SQLStateNoSuchTable = "42S02"
	SQLStateNoSuchCol   = "42S22"
	SQLStateDupColumn   = "42S21"
	// SQLStateConnFailure is MySQL's class for a connection that is going away,
	// used for ER_SERVER_SHUTDOWN.
	SQLStateConnFailure = "08S01"
	SQLStateNoDB        = "3D000"
)
