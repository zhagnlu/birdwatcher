package show

import (
	"testing"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

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

func TestEnsureJSONGroupStatMergesPackedFields(t *testing.T) {
	collectionStats := &jsonCollectionStats{
		databaseID:      1,
		databaseName:    "default",
		collectionID:    100,
		collectionName:  "posts",
		collectionState: "CollectionCreated",
		fields: map[int64]*schemapb.FieldSchema{
			105: {FieldID: 105, Name: "follower_count_group", DataType: schemapb.DataType_JSON},
			106: {FieldID: 106, Name: "taken_at_bounds", DataType: schemapb.DataType_JSON},
		},
		groups: make(map[string]*JSONColumnStat),
	}

	stat := ensureJSONGroupStat(collectionStats, []int64{106, 105, 106})
	if stat.FieldID != 105 {
		t.Fatalf("expected first field id 105, got %d", stat.FieldID)
	}
	if displayJSONFieldIDs(stat) != "105,106" {
		t.Fatalf("unexpected field ids display: %s", displayJSONFieldIDs(stat))
	}
	if displayJSONFieldNames(stat) != "follower_count_group,taken_at_bounds" {
		t.Fatalf("unexpected field names display: %s", displayJSONFieldNames(stat))
	}
	if stat.StorageMode != "v2" {
		t.Fatalf("expected v2 storage mode, got %s", stat.StorageMode)
	}
	if len(collectionStats.groups) != 1 {
		t.Fatalf("expected one storage group, got %#v", collectionStats.groups)
	}
}

func TestJSONStorageMode(t *testing.T) {
	if got := jsonStorageMode([]int64{105}); got != "v1" {
		t.Fatalf("jsonStorageMode(single) = %s, want v1", got)
	}
	if got := jsonSizeScope([]int64{105}); got != "field" {
		t.Fatalf("jsonSizeScope(single) = %s, want field", got)
	}
	if got := jsonStorageMode([]int64{105, 106}); got != "v2" {
		t.Fatalf("jsonStorageMode(packed) = %s, want v2", got)
	}
	if got := jsonSizeScope([]int64{105, 106}); got != "shared_group" {
		t.Fatalf("jsonSizeScope(packed) = %s, want shared_group", got)
	}
}

func TestPackedJSONChildFieldIDsSkipsExactFields(t *testing.T) {
	jsonFields := map[int64]*schemapb.FieldSchema{
		105: {FieldID: 105, Name: "json_105", DataType: schemapb.DataType_JSON},
		106: {FieldID: 106, Name: "json_106", DataType: schemapb.DataType_JSON},
	}

	got := packedJSONChildFieldIDs(jsonFields, []int64{106, 105, 105, 200}, map[int64]struct{}{106: {}})
	if len(got) != 1 || got[0] != 105 {
		t.Fatalf("packedJSONChildFieldIDs() = %v, want [105]", got)
	}
}

func TestApplyV2JSONSampleEstimatesRawAndAllocatedSizes(t *testing.T) {
	collectionStats := &jsonCollectionStats{
		fields: map[int64]*schemapb.FieldSchema{
			105: {FieldID: 105, Name: "json_a", DataType: schemapb.DataType_JSON},
			106: {FieldID: 106, Name: "json_b", DataType: schemapb.DataType_JSON},
		},
	}
	stat := &JSONColumnStat{
		FieldIDs:        []int64{105, 106},
		RowCount:        100,
		LogSizeBytes:    1000,
		MemorySizeBytes: 2000,
	}
	sample := &v2JSONSample{
		rows:       10,
		totalBytes: 1000,
		fieldBytes: map[int64]int64{
			105: 250,
			106: 750,
		},
	}

	applyV2JSONSample(stat, collectionStats, sample)

	if stat.V2SampleRows != 10 || stat.V2SampleBytes != 1000 {
		t.Fatalf("unexpected sample summary: rows=%d bytes=%d", stat.V2SampleRows, stat.V2SampleBytes)
	}
	if len(stat.V2FieldEstimates) != 2 {
		t.Fatalf("expected 2 estimates, got %d", len(stat.V2FieldEstimates))
	}
	first := stat.V2FieldEstimates[0]
	if first.FieldID != 105 || first.FieldName != "json_a" {
		t.Fatalf("unexpected first estimate field: %#v", first)
	}
	if first.SampleBytes != 250 || first.SampleAvgBytes != 25 {
		t.Fatalf("unexpected first sample bytes: %#v", first)
	}
	if first.RawEstimateBytes != 2500 {
		t.Fatalf("raw estimate = %d, want 2500", first.RawEstimateBytes)
	}
	if first.EstimatedLogSizeBytes != 250 || first.EstimatedMemorySizeBytes != 500 {
		t.Fatalf("allocated sizes = log %d mem %d, want 250/500", first.EstimatedLogSizeBytes, first.EstimatedMemorySizeBytes)
	}

	second := stat.V2FieldEstimates[1]
	if second.RawEstimateBytes != 7500 {
		t.Fatalf("second raw estimate = %d, want 7500", second.RawEstimateBytes)
	}
	if second.EstimatedLogSizeBytes != 750 || second.EstimatedMemorySizeBytes != 1500 {
		t.Fatalf("second allocated sizes = log %d mem %d, want 750/1500", second.EstimatedLogSizeBytes, second.EstimatedMemorySizeBytes)
	}
}
