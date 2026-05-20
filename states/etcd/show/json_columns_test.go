package show

import (
	"testing"

	"github.com/milvus-io/birdwatcher/models"
)

func TestCollectPackedChildJSONColumnStats(t *testing.T) {
	fieldStats := map[int64]*JSONColumnStat{
		138: {FieldID: 138, FieldName: "$meta"},
		139: {FieldID: 139, FieldName: "payload"},
	}

	t.Run("matches storage v2 packed child fields", func(t *testing.T) {
		packedChildStats := make(map[int64]*JSONColumnStat)
		collectPackedChildJSONColumnStats(fieldStats, &models.FieldBinlog{
			FieldID:     1,
			ChildFields: []int64{105, 138, 139},
		}, nil, packedChildStats)

		if len(packedChildStats) != 2 {
			t.Fatalf("expected 2 packed JSON child field matches, got %#v", packedChildStats)
		}
		if packedChildStats[138] == nil || packedChildStats[139] == nil {
			t.Fatalf("expected JSON child field matches for 138 and 139, got %#v", packedChildStats)
		}
	})

	t.Run("skips exact field matches", func(t *testing.T) {
		packedChildStats := make(map[int64]*JSONColumnStat)
		exactFieldIDs := map[int64]struct{}{138: {}}
		collectPackedChildJSONColumnStats(fieldStats, &models.FieldBinlog{
			FieldID:     138,
			ChildFields: []int64{138, 139},
		}, exactFieldIDs, packedChildStats)

		if packedChildStats[138] != nil || packedChildStats[139] == nil {
			t.Fatalf("expected exact field 138 to be skipped and 139 to match, got %#v", packedChildStats)
		}
	})
}

func TestSummarizeFieldBinlog(t *testing.T) {
	summary := summarizeFieldBinlog(&models.FieldBinlog{
		FieldID: 1,
		Binlogs: []*models.Binlog{
			{EntriesNum: 10, LogSize: 20, MemSize: 30},
			{EntriesNum: 40, LogSize: 50, MemSize: 60},
		},
	})

	if summary.logCount != 2 ||
		summary.rowCount != 50 ||
		summary.logSizeBytes != 70 ||
		summary.memorySizeBytes != 90 {
		t.Fatalf("unexpected summary: %#v", summary)
	}
}
