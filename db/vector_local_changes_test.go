package db

import (
	"testing"

	"github.com/maxpert/marmot/encoding"
	"github.com/stretchr/testify/require"
)

func TestDecodeCDCBytesFastPathBorrowsMsgpackBinPayload(t *testing.T) {
	raw, err := encoding.Marshal([]byte{1, 2, 3, 4})
	require.NoError(t, err)

	values := map[string][]byte{"embed": raw}
	got, ok, err := decodeCDCBytes(values, "embed")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte{1, 2, 3, 4}, got)

	raw[len(raw)-1] = 9
	require.Equal(t, byte(9), got[len(got)-1])
}

func TestDecodeCDCBytesFallsBackForString(t *testing.T) {
	raw, err := encoding.Marshal("abcd")
	require.NoError(t, err)

	got, ok, err := decodeCDCBytes(map[string][]byte{"embed": raw}, "embed")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("abcd"), got)
}
