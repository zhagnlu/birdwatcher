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
		packedChildSummaries := make(map[int64]insertLogSummary)
		summary := insertLogSummary{
			logCount:        1,
			rowCount:        10,
			logSizeBytes:    20,
			memorySizeBytes: 30,
		}
		collectPackedChildJSONColumnStats(fieldStats, &models.FieldBinlog{
			FieldID:     1,
			ChildFields: []int64{105, 138, 139},
		}, nil, summary, packedChildSummaries)

		if len(packedChildSummaries) != 2 {
			t.Fatalf("expected 2 packed JSON child field matches, got %#v", packedChildSummaries)
		}
		if packedChildSummaries[138] != summary || packedChildSummaries[139] != summary {
			t.Fatalf("expected JSON child field summaries for 138 and 139, got %#v", packedChildSummaries)
		}
	})

	t.Run("skips exact field matches", func(t *testing.T) {
		packedChildSummaries := make(map[int64]insertLogSummary)
		exactFieldIDs := map[int64]struct{}{138: {}}
		summary := insertLogSummary{logCount: 1, rowCount: 10, logSizeBytes: 20, memorySizeBytes: 30}
		collectPackedChildJSONColumnStats(fieldStats, &models.FieldBinlog{
			FieldID:     138,
			ChildFields: []int64{138, 139},
		}, exactFieldIDs, summary, packedChildSummaries)

		if _, ok := packedChildSummaries[138]; ok {
			t.Fatalf("expected exact field 138 to be skipped, got %#v", packedChildSummaries)
		}
		if packedChildSummaries[139] != summary {
			t.Fatalf("expected field 139 to match, got %#v", packedChildSummaries)
		}
	})

	t.Run("accumulates only matching packed binlogs", func(t *testing.T) {
		packedChildSummaries := make(map[int64]insertLogSummary)
		collectPackedChildJSONColumnStats(fieldStats, &models.FieldBinlog{
			FieldID:     1,
			ChildFields: []int64{101, 139},
		}, nil, insertLogSummary{logCount: 1, rowCount: 11, logSizeBytes: 101, memorySizeBytes: 1001}, packedChildSummaries)
		collectPackedChildJSONColumnStats(fieldStats, &models.FieldBinlog{
			FieldID:     113,
			ChildFields: []int64{113},
		}, nil, insertLogSummary{logCount: 1, rowCount: 22, logSizeBytes: 202, memorySizeBytes: 2002}, packedChildSummaries)

		expected := insertLogSummary{logCount: 1, rowCount: 11, logSizeBytes: 101, memorySizeBytes: 1001}
		if packedChildSummaries[139] != expected {
			t.Fatalf("expected field 139 to use only the matching packed field summary, got %#v", packedChildSummaries[139])
		}
		if _, ok := packedChildSummaries[138]; ok {
			t.Fatalf("expected unrelated JSON field 138 to stay empty, got %#v", packedChildSummaries)
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
