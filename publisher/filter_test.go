package publisher

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewGlobFilter(t *testing.T) {
	filter, err := NewGlobFilter([]string{"users", "orders"}, []string{"production", "staging"})
	require.NoError(t, err)
	require.NotNil(t, filter)

	assert.Len(t, filter.tableGlobs, 2)
	assert.Len(t, filter.databaseGlobs, 2)
}

func TestNewGlobFilterEmptyPatterns(t *testing.T) {
	// Empty patterns should match everything
	filter, err := NewGlobFilter(nil, nil)
	require.NoError(t, err)
	require.NotNil(t, filter)

	// Should match anything
	assert.True(t, filter.Match("any_db", "any_table"))
	assert.True(t, filter.Match("production", "users"))
	assert.True(t, filter.Match("", ""))
}

func TestGlobFilterExactMatch(t *testing.T) {
	filter, err := NewGlobFilter([]string{"users"}, []string{"production"})
	require.NoError(t, err)

	// Should match exact strings
	assert.True(t, filter.Match("production", "users"))

	// Should not match different strings
	assert.False(t, filter.Match("staging", "users"))
	assert.False(t, filter.Match("production", "orders"))
	assert.False(t, filter.Match("staging", "orders"))
}

func TestGlobFilterWildcard(t *testing.T) {
	filter, err := NewGlobFilter([]string{"user*"}, []string{"prod*"})
	require.NoError(t, err)

	// Should match with wildcards
	assert.True(t, filter.Match("production", "users"))
	assert.True(t, filter.Match("prod", "user"))
	assert.True(t, filter.Match("prod_db", "user_accounts"))

	// Should not match
	assert.False(t, filter.Match("staging", "users"))
	assert.False(t, filter.Match("production", "orders"))
}

func TestGlobFilterMultiplePatterns(t *testing.T) {
	filter, err := NewGlobFilter(
		[]string{"users", "orders", "products"},
		[]string{"production", "staging"},
	)
	require.NoError(t, err)

	// Should match any combination
	assert.True(t, filter.Match("production", "users"))
	assert.True(t, filter.Match("staging", "orders"))
	assert.True(t, filter.Match("production", "products"))

	// Should not match non-matching table
	assert.False(t, filter.Match("production", "inventory"))

	// Should not match non-matching database
	assert.False(t, filter.Match("development", "users"))
}

func TestGlobFilterGlobPatterns(t *testing.T) {
	filter, err := NewGlobFilter(
		[]string{"user_*", "order_*"},
		[]string{"*_prod", "*_staging"},
	)
	require.NoError(t, err)

	// Should match glob patterns
	assert.True(t, filter.Match("us_prod", "user_accounts"))
	assert.True(t, filter.Match("eu_staging", "order_items"))
	assert.True(t, filter.Match("asia_prod", "user_preferences"))

	// Should not match
	assert.False(t, filter.Match("dev", "user_accounts"))
	assert.False(t, filter.Match("us_prod", "product_catalog"))
}

func TestGlobFilterOnlyTablePatterns(t *testing.T) {
	// Only table patterns, no database patterns
	filter, err := NewGlobFilter([]string{"users", "orders"}, nil)
	require.NoError(t, err)

	// Should match any database with matching table
	assert.True(t, filter.Match("production", "users"))
	assert.True(t, filter.Match("staging", "users"))
	assert.True(t, filter.Match("development", "orders"))
	assert.True(t, filter.Match("any_db", "users"))

	// Should not match non-matching table
	assert.False(t, filter.Match("production", "products"))
}

func TestGlobFilterOnlyDatabasePatterns(t *testing.T) {
	// Only database patterns, no table patterns
	filter, err := NewGlobFilter(nil, []string{"production", "staging"})
	require.NoError(t, err)

	// Should match matching database with any table
	assert.True(t, filter.Match("production", "users"))
	assert.True(t, filter.Match("staging", "orders"))
	assert.True(t, filter.Match("production", "anything"))

	// Should not match non-matching database
	assert.False(t, filter.Match("development", "users"))
}

func TestGlobFilterComplexGlobs(t *testing.T) {
	filter, err := NewGlobFilter(
		[]string{"user_{accounts,profiles}", "order_*"},
		[]string{"*_prod_*"},
	)
	require.NoError(t, err)

	// Should match complex patterns
	assert.True(t, filter.Match("us_prod_1", "user_accounts"))
	assert.True(t, filter.Match("eu_prod_2", "user_profiles"))
	assert.True(t, filter.Match("asia_prod_main", "order_items"))

	// Should not match
	assert.False(t, filter.Match("us_staging_1", "user_accounts"))
	assert.False(t, filter.Match("us_prod_1", "user_settings"))
}

func TestGlobFilterCaseSensitive(t *testing.T) {
	filter, err := NewGlobFilter([]string{"Users"}, []string{"Production"})
	require.NoError(t, err)

	// Glob matching is case-sensitive by default
	assert.True(t, filter.Match("Production", "Users"))
	assert.False(t, filter.Match("production", "Users"))
	assert.False(t, filter.Match("Production", "users"))
}

func TestGlobFilterInvalidPattern(t *testing.T) {
	// Invalid glob pattern should return error
	_, err := NewGlobFilter([]string{"user["}, nil)
	assert.Error(t, err)

	_, err = NewGlobFilter(nil, []string{"db["})
	assert.Error(t, err)
}

func TestGlobFilterSpecialCharacters(t *testing.T) {
	filter, err := NewGlobFilter(
		[]string{"user-accounts", "order.items"},
		[]string{"db-prod", "db.staging"},
	)
	require.NoError(t, err)

	// Should handle special characters
	assert.True(t, filter.Match("db-prod", "user-accounts"))
	assert.True(t, filter.Match("db.staging", "order.items"))
}

func TestGlobFilterEmptyStrings(t *testing.T) {
	filter, err := NewGlobFilter([]string{"*"}, []string{"*"})
	require.NoError(t, err)

	// Should match empty strings with wildcard
	assert.True(t, filter.Match("", ""))
	assert.True(t, filter.Match("db", ""))
	assert.True(t, filter.Match("", "table"))
}

func TestGlobFilterQuestionMark(t *testing.T) {
	filter, err := NewGlobFilter(
		[]string{"user?"}, // Matches user followed by any single char
		[]string{"db?"},
	)
	require.NoError(t, err)

	// Should match single character
	assert.True(t, filter.Match("db1", "user1"))
	assert.True(t, filter.Match("dba", "userx"))

	// Should not match multiple characters
	assert.False(t, filter.Match("db12", "user1"))
	assert.False(t, filter.Match("db1", "user12"))
}

func TestGlobFilterNoTablePattern(t *testing.T) {
	// Database pattern but no table pattern
	filter, err := NewGlobFilter([]string{}, []string{"production"})
	require.NoError(t, err)

	// Should match all tables in matching database
	assert.True(t, filter.Match("production", "users"))
	assert.True(t, filter.Match("production", "orders"))
	assert.True(t, filter.Match("production", "anything"))

	// Should not match non-matching database
	assert.False(t, filter.Match("staging", "users"))
}

func TestGlobFilterNoDatabasePattern(t *testing.T) {
	// Table pattern but no database pattern
	filter, err := NewGlobFilter([]string{"users"}, []string{})
	require.NoError(t, err)

	// Should match all databases with matching table
	assert.True(t, filter.Match("production", "users"))
	assert.True(t, filter.Match("staging", "users"))
	assert.True(t, filter.Match("anything", "users"))

	// Should not match non-matching table
	assert.False(t, filter.Match("production", "orders"))
}

func TestGlobFilterRanges(t *testing.T) {
	filter, err := NewGlobFilter(
		[]string{"user[0-9]"}, // Match user followed by digit
		[]string{"db[a-z]"},
	)
	require.NoError(t, err)

	// Should match ranges
	assert.True(t, filter.Match("dba", "user1"))
	assert.True(t, filter.Match("dbz", "user9"))

	// Should not match outside range
	assert.False(t, filter.Match("db1", "user1"))
	assert.False(t, filter.Match("dba", "usera"))
}

func BenchmarkGlobFilterMatch(b *testing.B) {
	filter, err := NewGlobFilter(
		[]string{"user*", "order*", "product*"},
		[]string{"production", "staging"},
	)
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		filter.Match("production", "users")
	}
}

func BenchmarkGlobFilterMatchMiss(b *testing.B) {
	filter, err := NewGlobFilter(
		[]string{"user*", "order*"},
		[]string{"production", "staging"},
	)
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		filter.Match("development", "inventory")
	}
}

func BenchmarkGlobFilterMatchWildcard(b *testing.B) {
	filter, err := NewGlobFilter(
		[]string{"*"},
		[]string{"*"},
	)
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		filter.Match("any_database", "any_table")
	}
}
