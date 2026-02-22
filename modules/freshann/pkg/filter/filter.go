package filter

import "github.com/maxpert/marmot/modules/freshann/pkg/storage"

func Match(rec storage.VectorRecord, partition string, tags map[string]string) bool {
	if partition != "" && rec.PartitionKey != partition {
		return false
	}
	if len(tags) == 0 {
		return true
	}
	if len(rec.Tags) == 0 {
		return false
	}
	for k, v := range tags {
		if rec.Tags[k] != v {
			return false
		}
	}
	return true
}
