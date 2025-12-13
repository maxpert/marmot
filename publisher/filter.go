package publisher

import (
	"fmt"

	"github.com/gobwas/glob"
)

// GlobFilter filters CDC events using glob patterns
type GlobFilter struct {
	tableGlobs    []glob.Glob
	databaseGlobs []glob.Glob
}

// NewGlobFilter creates a new glob-based filter
// Empty patterns match everything
func NewGlobFilter(tablePatterns, dbPatterns []string) (*GlobFilter, error) {
	filter := &GlobFilter{
		tableGlobs:    make([]glob.Glob, 0, len(tablePatterns)),
		databaseGlobs: make([]glob.Glob, 0, len(dbPatterns)),
	}

	// Compile table patterns
	for _, pattern := range tablePatterns {
		g, err := glob.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("invalid table pattern %q: %w", pattern, err)
		}
		filter.tableGlobs = append(filter.tableGlobs, g)
	}

	// Compile database patterns
	for _, pattern := range dbPatterns {
		g, err := glob.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("invalid database pattern %q: %w", pattern, err)
		}
		filter.databaseGlobs = append(filter.databaseGlobs, g)
	}

	return filter, nil
}

// Match returns true if the database and table match the configured patterns
// If no patterns are configured, all events match
func (f *GlobFilter) Match(database, table string) bool {
	// If no database patterns, match all databases
	dbMatch := len(f.databaseGlobs) == 0
	if !dbMatch {
		for _, g := range f.databaseGlobs {
			if g.Match(database) {
				dbMatch = true
				break
			}
		}
	}

	// If database doesn't match, short-circuit
	if !dbMatch {
		return false
	}

	// If no table patterns, match all tables
	tableMatch := len(f.tableGlobs) == 0
	if !tableMatch {
		for _, g := range f.tableGlobs {
			if g.Match(table) {
				tableMatch = true
				break
			}
		}
	}

	return tableMatch
}
