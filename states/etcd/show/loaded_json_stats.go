package show

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
)

type LoadedJSONStatsParam struct {
	framework.ParamBase `use:"show loaded-json-stats" desc:"display loaded json stats information"`
	CollectionID        int64 `name:"collection" default:"0" desc:"collection id to filter with"`
	SegmentID           int64 `name:"segment" default:"0" desc:"segment id to filter with"`
	FieldID             int64 `name:"field" default:"0" desc:"field id to filter with"`
	IncludeUnBuilt      bool  `name:"include-un-built" default:"false" desc:"include un built segments"`
}

// LoadedJsonStatsCommand returns show loaded-json-stats command.
func (c *ComponentShow) LoadedJSONStatsCommand(ctx context.Context, p *LoadedJSONStatsParam) error {
	segments, err := common.ListSegments(ctx, c.client, c.metaPath, func(seg *models.Segment) bool {
		if p.CollectionID != 0 && p.CollectionID != seg.CollectionID {
			return false
		}
		if p.SegmentID != 0 && p.SegmentID != seg.ID {
			return false
		}
		if seg.Level == datapb.SegmentLevel_L0 {
			return false
		}

		// only consider flushed segments
		if seg.State != commonpb.SegmentState_Flushed {
			return false
		}

		return true
	})
	if err != nil {
		return err
	}

	collectionIDs := make(map[int64]struct{})
	if p.CollectionID != 0 {
		collectionIDs[p.CollectionID] = struct{}{}
	}
	for _, seg := range segments {
		collectionIDs[seg.CollectionID] = struct{}{}
	}
	collectionJSONFields := c.collectionJSONFields(ctx, collectionIDs)

	// Build expected segment set from etcd meta using the same JSON field filters as show json-stats.
	expected := make(map[int64]struct{})
	for _, seg := range segments {
		jsonFields, ok := collectionJSONFields[seg.CollectionID]
		if !ok {
			continue
		}
		if p.FieldID != 0 {
			if _, ok := jsonFields[p.FieldID]; !ok {
				continue
			}
		}

		if !p.IncludeUnBuilt {
			if p.FieldID != 0 {
				if seg.JsonKeyStats == nil {
					continue
				}
				if _, ok := seg.JsonKeyStats[p.FieldID]; !ok {
					continue
				}
			} else {
				hasJSONStats := false
				for fieldID := range seg.JsonKeyStats {
					if _, ok := jsonFields[fieldID]; ok {
						hasJSONStats = true
						break
					}
				}
				if !hasJSONStats {
					continue
				}
			}
		}

		expected[seg.ID] = struct{}{}
	}

	// 1. get all query nodes
	sessions, err := common.ListServers(ctx, c.client, c.metaPath, "querynode")
	if err != nil {
		return err
	}
	if len(sessions) == 0 {
		fmt.Println("no query nodes found")
		return nil
	}

	loaded := make(map[int64]struct{})
	for _, session := range sessions {
		opts := []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		}

		var conn *grpc.ClientConn
		var err error
		func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			conn, err = grpc.DialContext(ctx, session.Address, opts...)
		}()

		if err != nil {
			fmt.Printf("failed to connect %s(%d), err: %s\n", session.ServerName, session.ServerID, err.Error())
			continue
		}
		clientv2 := querypb.NewQueryNodeClient(conn)
		resp, err := clientv2.GetDataDistribution(context.Background(), &querypb.GetDataDistributionRequest{
			Base: &commonpb.MsgBase{
				SourceID: -1,
				TargetID: session.ServerID,
			},
		})
		if err != nil {
			fmt.Println(err.Error())
			continue
		}
		fmt.Printf("query node %s(%d):\n", session.ServerName, session.ServerID)

		for _, segment := range resp.GetSegments() {
			if p.CollectionID != 0 && p.CollectionID != segment.GetCollection() {
				continue
			}
			if p.SegmentID != 0 && p.SegmentID != segment.GetID() {
				continue
			}
			jsonFields, ok := collectionJSONFields[segment.GetCollection()]
			if !ok {
				continue
			}
			if p.FieldID != 0 {
				if _, ok := jsonFields[p.FieldID]; !ok {
					continue
				}
			}

			jsonStats := segment.GetJsonStatsInfo()
			if len(jsonStats) == 0 {
				continue
			}
			loadedThisSeg := false
			printedHeader := false
			for fieldId, jsonStat := range jsonStats {
				if p.FieldID != 0 && p.FieldID != fieldId {
					continue
				}
				if _, ok := jsonFields[fieldId]; !ok {
					continue
				}
				if !printedHeader {
					fmt.Printf("  collection %d, segment %d:\n", segment.GetCollection(), segment.GetID())
					printedHeader = true
				}
				fmt.Printf("    field [%d]: index stats: %s\n", fieldId, jsonStat)
				loadedThisSeg = true
			}
			if loadedThisSeg {
				loaded[segment.GetID()] = struct{}{}
			}
		}
	}

	// Summary ratio
	if len(expected) > 0 {
		loadedCnt := 0
		missing := make([]int64, 0)
		for segID := range expected {
			if _, ok := loaded[segID]; ok {
				loadedCnt++
			} else {
				missing = append(missing, segID)
			}
		}
		ratio := float64(loadedCnt) * 100.0 / float64(len(expected))
		fmt.Printf("\n--- Loaded ratio: %d/%d (%.2f%%)\n", loadedCnt, len(expected), ratio)
		if len(missing) > 0 {
			fmt.Printf("--- Not loaded segments (%d): %v\n", len(missing), missing)
		}
	}
	return nil
}
