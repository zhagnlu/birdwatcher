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
	if len(collectionStats.groups) != 1 {
		t.Fatalf("expected one storage group, got %#v", collectionStats.groups)
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
