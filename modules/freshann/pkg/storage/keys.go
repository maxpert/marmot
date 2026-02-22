package storage

import "encoding/binary"

var (
	keySpec      = []byte("meta/spec")
	keyWatermark = []byte("meta/watermark")
	keyNextDocID = []byte("meta/next_docid")
	keyGraphMeta = []byte("graph/meta")
	keyGraphHead = []byte("graph/head")
)

const (
	prefixVector    = "vec/"
	prefixVectorDoc = "vecdoc/"
	prefixExtToDoc  = "id/e2d/"
	prefixDocToExt  = "id/d2e/"
	prefixApplied   = "applied/"
	prefixPart      = "part/"
	prefixTag       = "tag/"
	prefixIDMap     = "idmap/"
	prefixGraphN    = "graph/n/"
)

func encodeAppliedKey(txnID, seqID uint64) []byte {
	buf := make([]byte, len(prefixApplied)+16)
	copy(buf, []byte(prefixApplied))
	binary.BigEndian.PutUint64(buf[len(prefixApplied):], txnID)
	binary.BigEndian.PutUint64(buf[len(prefixApplied)+8:], seqID)
	return buf
}

func encodeVectorKey(externalID []byte) []byte {
	out := make([]byte, len(prefixVector)+len(externalID))
	copy(out, []byte(prefixVector))
	copy(out[len(prefixVector):], externalID)
	return out
}

func decodeVectorExternalID(key []byte) []byte {
	if len(key) <= len(prefixVector) {
		return nil
	}
	out := make([]byte, len(key)-len(prefixVector))
	copy(out, key[len(prefixVector):])
	return out
}

func encodeVectorDocKey(docID uint64) []byte {
	buf := make([]byte, len(prefixVectorDoc)+8)
	copy(buf, []byte(prefixVectorDoc))
	binary.BigEndian.PutUint64(buf[len(prefixVectorDoc):], docID)
	return buf
}

func decodeVectorDocID(key []byte) uint64 {
	if len(key) < len(prefixVectorDoc)+8 {
		return 0
	}
	return binary.BigEndian.Uint64(key[len(prefixVectorDoc):])
}

func encodeExtToDocKey(externalID []byte) []byte {
	out := make([]byte, len(prefixExtToDoc)+len(externalID))
	copy(out, []byte(prefixExtToDoc))
	copy(out[len(prefixExtToDoc):], externalID)
	return out
}

func encodeDocToExtKey(docID uint64) []byte {
	buf := make([]byte, len(prefixDocToExt)+8)
	copy(buf, []byte(prefixDocToExt))
	binary.BigEndian.PutUint64(buf[len(prefixDocToExt):], docID)
	return buf
}

func encodePartitionKey(partition string) []byte {
	return []byte(prefixPart + partition)
}

func encodeTagKey(k, v string) []byte {
	return []byte(prefixTag + k + "=" + v)
}

func encodeIDMapKey(hash uint64) []byte {
	buf := make([]byte, len(prefixIDMap)+8)
	copy(buf, []byte(prefixIDMap))
	binary.BigEndian.PutUint64(buf[len(prefixIDMap):], hash)
	return buf
}

func encodeGraphNodeKey(nodeID string) []byte {
	return []byte(prefixGraphN + nodeID)
}

func decodeGraphNodeID(key []byte) string {
	if len(key) <= len(prefixGraphN) {
		return ""
	}
	return string(key[len(prefixGraphN):])
}

func prefixBounds(prefix string) (lower []byte, upper []byte) {
	lower = []byte(prefix)
	upper = nextPrefix(lower)
	return
}

func nextPrefix(p []byte) []byte {
	if len(p) == 0 {
		return nil
	}
	out := make([]byte, len(p))
	copy(out, p)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] < 0xFF {
			out[i]++
			return out[:i+1]
		}
	}
	return nil
}
