package protocol

import "strings"

// ReservedSystemSchemaNames are MySQL/information-schema-style pseudo-databases that Marmot
// treats as virtual: always selectable, never created, never subject to auto_create_database.
// MySQL servers always have information_schema, performance_schema, mysql and sys; clients and
// admin tools (MySQL Workbench, DBeaver, LLDAP, etc.) connect to or query them for catalogue
// information regardless of what real databases exist. Only qualified information_schema.*
// SELECTs are specially routed (see isInformationSchemaQuery in protocol/parser_vitess.go); the
// name itself is never backed by a real, creatable database on any node.
var ReservedSystemSchemaNames = []string{"information_schema", "performance_schema", "mysql", "sys"}

// IsReservedSystemSchema reports whether name is one of ReservedSystemSchemaNames, compared
// case-insensitively (MySQL database names are compared case-insensitively for these schemas
// regardless of the underlying filesystem's case sensitivity).
func IsReservedSystemSchema(name string) bool {
	for _, reserved := range ReservedSystemSchemaNames {
		if strings.EqualFold(name, reserved) {
			return true
		}
	}
	return false
}
