package show

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/jedib0t/go-pretty/v6/table"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

type JSONColumnsParam struct {
	framework.DataSetParam `use:"show json-columns" desc:"display collections with JSON columns and their data size" alias:"json-fields"`
	CollectionID           int64  `name:"collection" default:"0" desc:"collection id to filter with"`
	CollectionName         string `name:"name" default:"" desc:"collection name to filter with"`
	DatabaseID             int64  `name:"dbid" default:"-1" desc:"database id to filter"`
	CollectionState        string `name:"collection-state" default:"" desc:"collection state to filter"`
	SegmentState           string `name:"segment-state" default:"" desc:"segment state to include; empty means all non-dropped segments"`
	IncludeDropped         bool   `name:"include-dropped" default:"false" desc:"include dropped segments in data size statistics"`
}

type JSONColumnStat struct {
	DatabaseID             int64    `json:"database_id"`
	DatabaseName           string   `json:"database_name,omitempty"`
	CollectionID           int64    `json:"collection_id"`
	CollectionName         string   `json:"collection_name"`
	CollectionState        string   `json:"collection_state"`
	FieldID                int64    `json:"field_id"`
	FieldName              string   `json:"field_name"`
	FieldIDs               []int64  `json:"field_ids,omitempty"`
	FieldNames             []string `json:"field_names,omitempty"`
	IsDynamic              bool     `json:"is_dynamic,omitempty"`
	SegmentCount           int      `json:"segment_count"`
	SegmentRows            int64    `json:"segment_rows"`
	RowCount               int64    `json:"row_count"`
	LogCount               int      `json:"log_count"`
	LogSizeBytes           int64    `json:"log_size_bytes"`
	LogSize                string   `json:"log_size"`
	MemorySizeBytes        int64    `json:"memory_size_bytes"`
	MemorySize             string   `json:"memory_size"`
	JSONStatsBuiltSegments int      `json:"json_stats_built_segments"`
	JSONStatsFileCount     int      `json:"json_stats_file_count"`
	JSONStatsMemoryBytes   int64    `json:"json_stats_memory_bytes"`
	JSONStatsMemory        string   `json:"json_stats_memory"`
}

type JSONColumns struct {
	columns             []*JSONColumnStat
	matchedCollections  int
	jsonColumnCount     int
	segmentCount        int
	segmentRows         int64
	totalLogSizeBytes   int64
	totalMemoryBytes    int64
	totalJSONStatsBytes int64
}

type insertLogSummary struct {
	logCount        int
	rowCount        int64
	logSizeBytes    int64
	memorySizeBytes int64
}

type jsonCollectionStats struct {
	databaseID      int64
	databaseName    string
	collectionID    int64
	collectionName  string
	collectionState string
	fields          map[int64]*schemapb.FieldSchema
	groups          map[string]*JSONColumnStat
}

// JSONColumnsCommand returns show json-columns command.
func (c *ComponentShow) JSONColumnsCommand(ctx context.Context, p *JSONColumnsParam) (*framework.PresetResultSet, error) {
	dbNames := make(map[int64]string)
	if dbs, err := common.ListDatabase(ctx, c.client, c.metaPath); err == nil {
		for _, db := range dbs {
			dbNames[db.GetProto().GetId()] = db.GetProto().GetName()
		}
	}

	collections, err := common.ListCollections(ctx, c.client, c.metaPath, func(info *models.Collection) bool {
		coll := info.GetProto()
		return (p.CollectionID == 0 || coll.GetID() == p.CollectionID) &&
			(p.CollectionName == "" || coll.GetSchema().GetName() == p.CollectionName) &&
			(p.DatabaseID == -1 || coll.GetDbId() == p.DatabaseID) &&
			(p.CollectionState == "" || strings.EqualFold(coll.GetState().String(), p.CollectionState))
	})
	if err != nil {
		return nil, err
	}

	stats := make(map[int64]*jsonCollectionStats)
	collectionIDs := make(map[int64]struct{})
	matchedCollections := 0
	jsonColumnCount := 0
	for _, collection := range collections {
		coll := collection.GetProto()
		var jsonFields []*schemapb.FieldSchema
		for _, field := range coll.GetSchema().GetFields() {
			if field.GetDataType() == schemapb.DataType_JSON {
				jsonFields = append(jsonFields, field)
			}
		}
		if len(jsonFields) == 0 {
			continue
		}

		matchedCollections++
		jsonColumnCount += len(jsonFields)
		collectionIDs[coll.GetID()] = struct{}{}
		stats[coll.GetID()] = &jsonCollectionStats{
			databaseID:      coll.GetDbId(),
			databaseName:    dbNames[coll.GetDbId()],
			collectionID:    coll.GetID(),
			collectionName:  coll.GetSchema().GetName(),
			collectionState: coll.GetState().String(),
			fields:          make(map[int64]*schemapb.FieldSchema),
			groups:          make(map[string]*JSONColumnStat),
		}
		for _, field := range jsonFields {
			stats[coll.GetID()].fields[field.GetFieldID()] = field
		}
	}

	var segments []*models.Segment
	if len(collectionIDs) > 0 {
		segments, err = common.ListSegments(ctx, c.client, c.metaPath, func(seg *models.Segment) bool {
			if _, ok := collectionIDs[seg.GetCollectionID()]; !ok {
				return false
			}
			if !p.IncludeDropped && seg.GetState() == commonpb.SegmentState_Dropped {
				return false
			}
			if p.SegmentState != "" && !strings.EqualFold(seg.GetState().String(), p.SegmentState) {
				return false
			}
			return true
		})
		if err != nil {
			return nil, err
		}
	}

	var segmentRows int64
	for _, seg := range segments {
		collectionStats := stats[seg.GetCollectionID()]
		if collectionStats == nil {
			continue
		}
		segmentRows += seg.GetNumOfRows()

		exactFieldIDs := make(map[int64]struct{})
		for _, fieldBinlog := range seg.GetBinlogs() {
			if fieldBinlog == nil {
				continue
			}
			if _, ok := collectionStats.fields[fieldBinlog.FieldID]; ok {
				exactFieldIDs[fieldBinlog.FieldID] = struct{}{}
			}
		}

		segmentGroups := make(map[string]insertLogSummary)
		segmentGroupFields := make(map[string][]int64)
		fieldGroupKeys := make(map[int64]string)
		for _, fieldBinlog := range seg.GetBinlogs() {
			if fieldBinlog == nil {
				continue
			}
			summary := summarizeFieldBinlog(fieldBinlog)

			if _, ok := collectionStats.fields[fieldBinlog.FieldID]; ok {
				fieldIDs := []int64{fieldBinlog.FieldID}
				key := jsonFieldGroupKey(fieldIDs)
				groupSummary := segmentGroups[key]
				mergeInsertLogSummary(&groupSummary, summary)
				segmentGroups[key] = groupSummary
				segmentGroupFields[key] = fieldIDs
				fieldGroupKeys[fieldBinlog.FieldID] = key
			}

			childFieldIDs := packedJSONChildFieldIDs(collectionStats.fields, fieldBinlog.ChildFields, exactFieldIDs)
			if len(childFieldIDs) == 0 {
				continue
			}
			key := jsonFieldGroupKey(childFieldIDs)
			groupSummary := segmentGroups[key]
			mergeInsertLogSummary(&groupSummary, summary)
			segmentGroups[key] = groupSummary
			segmentGroupFields[key] = childFieldIDs
			for _, fieldID := range childFieldIDs {
				fieldGroupKeys[fieldID] = key
			}
		}

		for key, summary := range segmentGroups {
			stat := ensureJSONGroupStat(collectionStats, segmentGroupFields[key])
			stat.SegmentCount++
			stat.SegmentRows += seg.GetNumOfRows()
			stat.LogCount += summary.logCount
			stat.RowCount += summary.rowCount
			stat.LogSizeBytes += summary.logSizeBytes
			stat.MemorySizeBytes += summary.memorySizeBytes
		}

		type jsonStatsSummary struct {
			fileCount   int
			memoryBytes int64
		}
		segmentJSONStats := make(map[string]jsonStatsSummary)
		for fieldID, keyStats := range seg.GetJsonKeyStats() {
			if _, ok := collectionStats.fields[fieldID]; !ok {
				continue
			}
			key, ok := fieldGroupKeys[fieldID]
			if !ok {
				key = jsonFieldGroupKey([]int64{fieldID})
				segmentGroupFields[key] = []int64{fieldID}
			}
			summary := segmentJSONStats[key]
			summary.fileCount += len(keyStats.GetFiles())
			summary.memoryBytes += keyStats.GetMemorySize()
			segmentJSONStats[key] = summary
		}
		for key, summary := range segmentJSONStats {
			stat := ensureJSONGroupStat(collectionStats, segmentGroupFields[key])
			stat.JSONStatsBuiltSegments++
			stat.JSONStatsFileCount += summary.fileCount
			stat.JSONStatsMemoryBytes += summary.memoryBytes
		}
	}

	columns := make([]*JSONColumnStat, 0)
	var totalLogSizeBytes int64
	var totalMemoryBytes int64
	var totalJSONStatsBytes int64
	for _, collectionStats := range stats {
		ensureMissingJSONFieldRows(collectionStats)
		for _, stat := range collectionStats.groups {
			stat.LogSize = hrSize(stat.LogSizeBytes)
			stat.MemorySize = hrSize(stat.MemorySizeBytes)
			stat.JSONStatsMemory = hrSize(stat.JSONStatsMemoryBytes)
			totalLogSizeBytes += stat.LogSizeBytes
			totalMemoryBytes += stat.MemorySizeBytes
			totalJSONStatsBytes += stat.JSONStatsMemoryBytes
			columns = append(columns, stat)
		}
	}
	sort.Slice(columns, func(i, j int) bool {
		if columns[i].DatabaseID != columns[j].DatabaseID {
			return columns[i].DatabaseID < columns[j].DatabaseID
		}
		if columns[i].CollectionName != columns[j].CollectionName {
			return columns[i].CollectionName < columns[j].CollectionName
		}
		if columns[i].CollectionID != columns[j].CollectionID {
			return columns[i].CollectionID < columns[j].CollectionID
		}
		return columns[i].FieldID < columns[j].FieldID
	})

	return framework.NewPresetResultSet(&JSONColumns{
		columns:             columns,
		matchedCollections:  matchedCollections,
		jsonColumnCount:     jsonColumnCount,
		segmentCount:        len(segments),
		segmentRows:         segmentRows,
		totalLogSizeBytes:   totalLogSizeBytes,
		totalMemoryBytes:    totalMemoryBytes,
		totalJSONStatsBytes: totalJSONStatsBytes,
	}, framework.NameFormat(p.Format)), nil
}

func summarizeFieldBinlog(fieldBinlog *models.FieldBinlog) insertLogSummary {
	if fieldBinlog == nil {
		return insertLogSummary{}
	}

	summary := insertLogSummary{logCount: len(fieldBinlog.Binlogs)}
	for _, binlog := range fieldBinlog.Binlogs {
		summary.rowCount += binlog.EntriesNum
		summary.logSizeBytes += binlog.LogSize
		summary.memorySizeBytes += binlog.MemSize
	}
	return summary
}

func mergeInsertLogSummary(dst *insertLogSummary, src insertLogSummary) {
	dst.logCount += src.logCount
	dst.rowCount += src.rowCount
	dst.logSizeBytes += src.logSizeBytes
	dst.memorySizeBytes += src.memorySizeBytes
}

func ensureJSONGroupStat(collectionStats *jsonCollectionStats, fieldIDs []int64) *JSONColumnStat {
	fieldIDs = normalizeJSONFieldIDs(fieldIDs)
	key := jsonFieldGroupKey(fieldIDs)
	if stat, ok := collectionStats.groups[key]; ok {
		return stat
	}

	fieldNames := make([]string, 0, len(fieldIDs))
	isDynamic := false
	for _, fieldID := range fieldIDs {
		field := collectionStats.fields[fieldID]
		if field == nil {
			continue
		}
		fieldNames = append(fieldNames, field.GetName())
		isDynamic = isDynamic || field.GetIsDynamic()
	}

	stat := &JSONColumnStat{
		DatabaseID:      collectionStats.databaseID,
		DatabaseName:    collectionStats.databaseName,
		CollectionID:    collectionStats.collectionID,
		CollectionName:  collectionStats.collectionName,
		CollectionState: collectionStats.collectionState,
		FieldIDs:        append([]int64(nil), fieldIDs...),
		FieldNames:      fieldNames,
		IsDynamic:       isDynamic,
		LogSize:         hrSize(0),
		MemorySize:      hrSize(0),
		JSONStatsMemory: hrSize(0),
	}
	if len(fieldIDs) > 0 {
		stat.FieldID = fieldIDs[0]
	}
	if len(fieldNames) > 0 {
		stat.FieldName = fieldNames[0]
	}

	collectionStats.groups[key] = stat
	return stat
}

func ensureMissingJSONFieldRows(collectionStats *jsonCollectionStats) {
	represented := make(map[int64]struct{})
	for _, stat := range collectionStats.groups {
		for _, fieldID := range stat.FieldIDs {
			represented[fieldID] = struct{}{}
		}
	}

	for fieldID := range collectionStats.fields {
		if _, ok := represented[fieldID]; ok {
			continue
		}
		ensureJSONGroupStat(collectionStats, []int64{fieldID})
	}
}

func packedJSONChildFieldIDs(
	jsonFields map[int64]*schemapb.FieldSchema,
	childFields []int64,
	exactFieldIDs map[int64]struct{},
) []int64 {
	fieldIDs := make([]int64, 0, len(childFields))
	for _, childFieldID := range childFields {
		if _, ok := exactFieldIDs[childFieldID]; ok {
			continue
		}
		if _, ok := jsonFields[childFieldID]; ok {
			fieldIDs = append(fieldIDs, childFieldID)
		}
	}
	return normalizeJSONFieldIDs(fieldIDs)
}

func normalizeJSONFieldIDs(fieldIDs []int64) []int64 {
	if len(fieldIDs) == 0 {
		return nil
	}
	result := append([]int64(nil), fieldIDs...)
	sort.Slice(result, func(i, j int) bool {
		return result[i] < result[j]
	})
	n := 0
	for _, fieldID := range result {
		if n > 0 && result[n-1] == fieldID {
			continue
		}
		result[n] = fieldID
		n++
	}
	return result[:n]
}

func jsonFieldGroupKey(fieldIDs []int64) string {
	fieldIDs = normalizeJSONFieldIDs(fieldIDs)
	parts := make([]string, 0, len(fieldIDs))
	for _, fieldID := range fieldIDs {
		parts = append(parts, strconv.FormatInt(fieldID, 10))
	}
	return strings.Join(parts, ",")
}

func (rs *JSONColumns) Entities() any {
	return rs.columns
}

func (rs *JSONColumns) PrintAs(format framework.Format) string {
	switch format {
	case framework.FormatJSON:
		return rs.printAsJSON()
	case framework.FormatLine:
		return rs.printAsLine()
	default:
		return rs.printAsTable()
	}
}

func (rs *JSONColumns) printAsTable() string {
	if len(rs.columns) == 0 {
		return "no json columns found\n"
	}

	t := table.NewWriter()
	t.AppendHeader(table.Row{
		"DB", "Collection", "CollectionID", "Field", "FieldID", "Segments", "Rows", "InsertLog", "Mem", "Logs", "JsonStats",
	})
	for _, col := range rs.columns {
		dbName := col.DatabaseName
		if dbName == "" {
			dbName = fmt.Sprintf("%d", col.DatabaseID)
		}
		t.AppendRow(table.Row{
			dbName,
			col.CollectionName,
			col.CollectionID,
			displayJSONFieldNames(col),
			displayJSONFieldIDs(col),
			col.SegmentCount,
			col.RowCount,
			col.LogSize,
			col.MemorySize,
			col.LogCount,
			fmt.Sprintf("%d/%d, %s", col.JSONStatsBuiltSegments, col.SegmentCount, col.JSONStatsMemory),
		})
	}
	return fmt.Sprintf("%s\n--- JSON collections: %d collections, JSON columns: %d columns, storage groups: %d groups, matched segments: %d segments, matched rows: %d rows, insert log size: %s, mem size: %s, json stats size: %s\n",
		t.Render(), rs.matchedCollections, rs.jsonColumnCount, len(rs.columns), rs.segmentCount, rs.segmentRows,
		hrSize(rs.totalLogSizeBytes), hrSize(rs.totalMemoryBytes), hrSize(rs.totalJSONStatsBytes))
}

func (rs *JSONColumns) printAsLine() string {
	sb := &strings.Builder{}
	for _, col := range rs.columns {
		fmt.Fprintf(sb, "collection %s(%d) fields %s(%s) rows %d size %s\n",
			col.CollectionName, col.CollectionID, displayJSONFieldNames(col), displayJSONFieldIDs(col), col.RowCount, col.LogSize)
	}
	fmt.Fprintf(sb, "--- JSON collections: %d collections, JSON columns: %d columns, storage groups: %d groups, matched segments: %d segments, matched rows: %d rows, insert log size: %s, mem size: %s, json stats size: %s\n",
		rs.matchedCollections, rs.jsonColumnCount, len(rs.columns), rs.segmentCount, rs.segmentRows,
		hrSize(rs.totalLogSizeBytes), hrSize(rs.totalMemoryBytes), hrSize(rs.totalJSONStatsBytes))
	return sb.String()
}

func displayJSONFieldNames(col *JSONColumnStat) string {
	if len(col.FieldNames) == 0 {
		return col.FieldName
	}
	return strings.Join(col.FieldNames, ",")
}

func displayJSONFieldIDs(col *JSONColumnStat) string {
	if len(col.FieldIDs) == 0 {
		return strconv.FormatInt(col.FieldID, 10)
	}
	parts := make([]string, 0, len(col.FieldIDs))
	for _, fieldID := range col.FieldIDs {
		parts = append(parts, strconv.FormatInt(fieldID, 10))
	}
	return strings.Join(parts, ",")
}

func (rs *JSONColumns) printAsJSON() string {
	type OutputJSON struct {
		Columns             []*JSONColumnStat `json:"columns"`
		JSONCollections     int               `json:"json_collections"`
		JSONColumns         int               `json:"json_columns"`
		StorageGroups       int               `json:"storage_groups"`
		MatchedSegmentRows  int64             `json:"matched_segment_rows"`
		MatchedSegments     int               `json:"matched_segments"`
		TotalLogSizeBytes   int64             `json:"total_log_size_bytes"`
		TotalLogSize        string            `json:"total_log_size"`
		TotalMemoryBytes    int64             `json:"total_memory_bytes"`
		TotalMemory         string            `json:"total_memory"`
		TotalJSONStatsBytes int64             `json:"total_json_stats_bytes"`
		TotalJSONStats      string            `json:"total_json_stats"`
	}

	return framework.MarshalJSON(OutputJSON{
		Columns:             rs.columns,
		JSONCollections:     rs.matchedCollections,
		JSONColumns:         rs.jsonColumnCount,
		StorageGroups:       len(rs.columns),
		MatchedSegmentRows:  rs.segmentRows,
		MatchedSegments:     rs.segmentCount,
		TotalLogSizeBytes:   rs.totalLogSizeBytes,
		TotalLogSize:        hrSize(rs.totalLogSizeBytes),
		TotalMemoryBytes:    rs.totalMemoryBytes,
		TotalMemory:         hrSize(rs.totalMemoryBytes),
		TotalJSONStatsBytes: rs.totalJSONStatsBytes,
		TotalJSONStats:      hrSize(rs.totalJSONStatsBytes),
	})
}
