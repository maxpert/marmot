package protocol

import (
	"regexp"
	"strings"
	"sync"
)

// Marmot session command patterns
var (
	// SET marmot_transpilation = ON/OFF
	setTranspilationRe = regexp.MustCompile(`(?i)^\s*SET\s+marmot_transpilation\s*=\s*(ON|OFF)\s*;?\s*$`)

	// SET marmot_wait_for_replication = ON/OFF/1/0
	setWaitForReplicationRe = regexp.MustCompile(`(?i)^\s*SET\s+marmot_wait_for_replication\s*=\s*(ON|OFF|1|0)\s*;?\s*$`)

	// LOAD EXTENSION <name>
	// Extension name can contain alphanumeric, dash, underscore, and dot characters
	loadExtensionRe = regexp.MustCompile(`(?i)^\s*LOAD\s+EXTENSION\s+([\w\-\.]+)\s*;?\s*$`)
)

// ExtensionLoader interface for loading SQLite extensions.
// This interface is used to avoid import cycles between coordinator and db packages.
type ExtensionLoader interface {
	LoadExtensionGlobally(name string) error
}

var (
	globalExtLoader   ExtensionLoader
	globalExtLoaderMu sync.RWMutex
)

// RegisterExtensionLoader sets the global extension loader.
// Called during initialization from the db package.
func RegisterExtensionLoader(loader ExtensionLoader) {
	globalExtLoaderMu.Lock()
	defer globalExtLoaderMu.Unlock()
	globalExtLoader = loader
}

// GetExtensionLoader returns the registered extension loader.
func GetExtensionLoader() ExtensionLoader {
	globalExtLoaderMu.RLock()
	defer globalExtLoaderMu.RUnlock()
	return globalExtLoader
}

// MarmotSessionCommand represents parsed marmot session commands
type MarmotSessionCommand int

const (
	MarmotCommandNone MarmotSessionCommand = iota
	MarmotCommandSetTranspilation
	MarmotCommandLoadExtension
)

// ParseMarmotSetCommand parses SET marmot_* commands.
// Returns the variable type (e.g., "transpilation"), value ("ON"/"OFF"), and whether it matched.
func ParseMarmotSetCommand(sql string) (varType, value string, matched bool) {
	// Check transpilation first
	matches := setTranspilationRe.FindStringSubmatch(sql)
	if matches != nil {
		return "transpilation", strings.ToUpper(matches[1]), true
	}

	// Check wait_for_replication
	matches = setWaitForReplicationRe.FindStringSubmatch(sql)
	if matches != nil {
		val := strings.ToUpper(matches[1])
		if val == "1" {
			val = "ON"
		} else if val == "0" {
			val = "OFF"
		}
		return "wait_for_replication", val, true
	}

	return "", "", false
}

// ParseLoadExtensionCommand parses LOAD EXTENSION commands.
// Returns the extension name and whether it matched.
func ParseLoadExtensionCommand(sql string) (extName string, matched bool) {
	matches := loadExtensionRe.FindStringSubmatch(sql)
	if matches == nil {
		return "", false
	}

	return matches[1], true
}

// IsMarmotSessionCommand checks if the SQL is a marmot session command.
func IsMarmotSessionCommand(sql string) bool {
	upper := strings.ToUpper(strings.TrimSpace(sql))
	return strings.HasPrefix(upper, "SET MARMOT_TRANSPILATION") ||
		strings.HasPrefix(upper, "SET MARMOT_WAIT_FOR_REPLICATION") ||
		strings.HasPrefix(upper, "LOAD EXTENSION")
}
