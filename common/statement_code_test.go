package common

import "testing"

func TestVectorIndexStatementCodes(t *testing.T) {
	if !StatementCreateVectorIndex.IsMutation() {
		t.Error("StatementCreateVectorIndex must be mutation")
	}
	if !StatementDropVectorIndex.IsMutation() {
		t.Error("StatementDropVectorIndex must be mutation")
	}
	if StatementCreateVectorIndex.IsReadOnly() {
		t.Error("StatementCreateVectorIndex must NOT be read-only")
	}
}
