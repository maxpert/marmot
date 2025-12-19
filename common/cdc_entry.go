package common

// CDCEntry represents a CDC entry for change data capture.
// This type is shared between coordinator and publisher packages.
type CDCEntry struct {
	Table     string
	IntentKey []byte
	OldValues map[string][]byte
	NewValues map[string][]byte
}
