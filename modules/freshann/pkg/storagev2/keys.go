package storagev2

import "encoding/binary"

var (
	keySpecV2      = []byte("meta/spec/v2")
	keyWatermark   = []byte("meta/watermark")
	keyNextDocID   = []byte("meta/next_docid")
	keyVectorCount = []byte("meta/vector_count")
	keyAppliedCnt  = []byte("meta/applied_count")
	keyGraphHead   = []byte("graph/v2/head")
)

const (
	prefixExtToDoc = "id/e2d/"
	prefixDocToExt = "id/d2e/"
	prefixVectorV2 = "vec/v2/"
	prefixMetaV2   = "meta/v2/"
	prefixApplied  = "tok/applied/"
	prefixPostPart = "post/part/"
	prefixPostTag  = "post/tag/"
	prefixGraphPg  = "graph/v2/page/"
)

func encodeAppliedKey(txnID, seqID uint64) []byte {
	buf := make([]byte, len(prefixApplied)+16)
	copy(buf, []byte(prefixApplied))
	binary.BigEndian.PutUint64(buf[len(prefixApplied):], txnID)
	binary.BigEndian.PutUint64(buf[len(prefixApplied)+8:], seqID)
	return buf
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

func encodeVectorDocKey(docID uint64) []byte {
	buf := make([]byte, len(prefixVectorV2)+8)
	copy(buf, []byte(prefixVectorV2))
	binary.BigEndian.PutUint64(buf[len(prefixVectorV2):], docID)
	return buf
}

func encodeMetaDocKey(docID uint64) []byte {
	buf := make([]byte, len(prefixMetaV2)+8)
	copy(buf, []byte(prefixMetaV2))
	binary.BigEndian.PutUint64(buf[len(prefixMetaV2):], docID)
	return buf
}

func decodeDocIDFromKey(prefix string, key []byte) uint64 {
	if len(key) < len(prefix)+8 {
		return 0
	}
	return binary.BigEndian.Uint64(key[len(prefix):])
}

func encodePostingBase(prefix string, value string) []byte {
	val := []byte(value)
	buf := make([]byte, len(prefix)+2+len(val))
	copy(buf, []byte(prefix))
	binary.BigEndian.PutUint16(buf[len(prefix):], uint16(len(val)))
	copy(buf[len(prefix)+2:], val)
	return buf
}

func encodePostingChunk(prefix string, value string, chunkID uint64) []byte {
	base := encodePostingBase(prefix, value)
	buf := make([]byte, len(base)+8)
	copy(buf, base)
	binary.BigEndian.PutUint64(buf[len(base):], chunkID)
	return buf
}

func encodeGraphPageKey(generation uint64, pageID uint64) []byte {
	buf := make([]byte, len(prefixGraphPg)+16)
	copy(buf, []byte(prefixGraphPg))
	binary.BigEndian.PutUint64(buf[len(prefixGraphPg):], generation)
	binary.BigEndian.PutUint64(buf[len(prefixGraphPg)+8:], pageID)
	return buf
}

func prefixBounds(prefix []byte) (lower []byte, upper []byte) {
	lower = append([]byte(nil), prefix...)
	upper = nextPrefix(lower)
	return
}

func nextPrefix(p []byte) []byte {
	if len(p) == 0 {
		return nil
	}
	out := append([]byte(nil), p...)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] < 0xFF {
			out[i]++
			return out[:i+1]
		}
	}
	return nil
}
