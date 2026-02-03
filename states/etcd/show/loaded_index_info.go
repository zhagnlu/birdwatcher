package show

import (
	"context"
	"fmt"
	"time"

	"github.com/samber/lo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
)

type LoadedIndexInfoParam struct {
	framework.ParamBase `use:"show loaded-index-info" desc:"display loaded index information from query nodes"`
	CollectionID        int64 `name:"collection" default:"0" desc:"collection id to filter with"`
	SegmentID           int64 `name:"segment" default:"0" desc:"segment id to filter with"`
	FieldID             int64 `name:"field" default:"0" desc:"field id to filter with"`
	IndexID             int64 `name:"indexID" default:"0" desc:"index id to filter with"`
}

// LoadedIndexInfoCommand returns show loaded-index-info command.
func (c *ComponentShow) LoadedIndexInfoCommand(ctx context.Context, p *LoadedIndexInfoParam) error {
	// Build expected segment-index set from etcd meta
	segments, err := common.ListSegments(ctx, c.client, c.metaPath, func(seg *models.Segment) bool {
		if p.CollectionID != 0 && p.CollectionID != seg.CollectionID {
			return false
		}
		if p.SegmentID != 0 && p.SegmentID != seg.ID {
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

	segmentIndexes, err := common.ListSegmentIndex(ctx, c.client, c.metaPath, func(segIdx *models.SegmentIndex) bool {
		return (p.CollectionID == 0 || p.CollectionID == segIdx.GetProto().GetCollectionID()) &&
			(p.SegmentID == 0 || p.SegmentID == segIdx.GetProto().GetSegmentID()) &&
			(p.IndexID == 0 || p.IndexID == segIdx.GetProto().GetIndexID())
	})
	if err != nil {
		return err
	}

	indexBuildInfo, err := common.ListIndex(ctx, c.client, c.metaPath, func(index *models.FieldIndex) bool {
		return (p.CollectionID == 0 || p.CollectionID == index.GetProto().GetIndexInfo().GetCollectionID()) &&
			(p.FieldID == 0 || p.FieldID == index.GetProto().GetIndexInfo().GetFieldID()) &&
			(p.IndexID == 0 || p.IndexID == index.GetProto().GetIndexInfo().GetIndexID())
	})
	if err != nil {
		return err
	}

	idIdx := lo.SliceToMap(indexBuildInfo, func(info *models.FieldIndex) (int64, *models.FieldIndex) {
		return info.GetProto().GetIndexInfo().GetIndexID(), info
	})

	// Build expected: segmentID -> set of indexIDs
	segSet := lo.SliceToMap(segments, func(s *models.Segment) (int64, struct{}) {
		return s.ID, struct{}{}
	})
	seg2ExpectedIdx := make(map[int64]map[int64]struct{})
	for _, segIdx := range segmentIndexes {
		segID := segIdx.GetProto().GetSegmentID()
		if _, ok := segSet[segID]; !ok {
			continue
		}
		if p.FieldID != 0 {
			indexID := segIdx.GetProto().GetIndexID()
			if fi, ok := idIdx[indexID]; ok {
				if fi.GetProto().GetIndexInfo().GetFieldID() != p.FieldID {
					continue
				}
			}
		}
		if seg2ExpectedIdx[segID] == nil {
			seg2ExpectedIdx[segID] = make(map[int64]struct{})
		}
		seg2ExpectedIdx[segID][segIdx.GetProto().GetIndexID()] = struct{}{}
	}

	// Get all query nodes
	sessions, err := common.ListServers(ctx, c.client, c.metaPath, "querynode")
	if err != nil {
		return err
	}
	if len(sessions) == 0 {
		fmt.Println("no query nodes found")
		return nil
	}

	// segmentID -> set of loaded indexIDs
	loadedSegIdx := make(map[int64]map[int64]struct{})
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

			indexInfo := segment.GetIndexInfo()
			if len(indexInfo) == 0 {
				continue
			}

			fmt.Printf("  collection %d, segment %d:\n", segment.GetCollection(), segment.GetID())
			for fieldID, fieldIdx := range indexInfo {
				if p.FieldID != 0 && p.FieldID != fieldID {
					continue
				}
				if p.IndexID != 0 && p.IndexID != fieldIdx.GetIndexID() {
					continue
				}

				indexType := common.GetKVPair(fieldIdx.GetIndexParams(), "index_type")
				fmt.Printf("    field [%d]: indexID=%d, indexName=%s, buildID=%d, indexType=%s, indexSize=%d, numRows=%d, indexVersion=%d, currentIndexVersion=%d\n",
					fieldID,
					fieldIdx.GetIndexID(),
					fieldIdx.GetIndexName(),
					fieldIdx.GetBuildID(),
					indexType,
					fieldIdx.GetIndexSize(),
					fieldIdx.GetNumRows(),
					fieldIdx.GetIndexVersion(),
					fieldIdx.GetCurrentIndexVersion(),
				)

				if loadedSegIdx[segment.GetID()] == nil {
					loadedSegIdx[segment.GetID()] = make(map[int64]struct{})
				}
				loadedSegIdx[segment.GetID()][fieldIdx.GetIndexID()] = struct{}{}
			}
		}
	}

	// Summary ratio
	if len(seg2ExpectedIdx) > 0 {
		totalExpected := 0
		totalLoaded := 0
		missingSegments := make([]int64, 0)
		for segID, expectedIdxs := range seg2ExpectedIdx {
			totalExpected += len(expectedIdxs)
			loadedIdxs, ok := loadedSegIdx[segID]
			if !ok {
				missingSegments = append(missingSegments, segID)
				continue
			}
			for idxID := range expectedIdxs {
				if _, ok := loadedIdxs[idxID]; ok {
					totalLoaded++
				}
			}
			if len(loadedIdxs) < len(expectedIdxs) {
				missingSegments = append(missingSegments, segID)
			}
		}
		ratio := float64(totalLoaded) * 100.0 / float64(totalExpected)
		fmt.Printf("\n--- Loaded index ratio: %d/%d (%.2f%%)\n", totalLoaded, totalExpected, ratio)
		if len(missingSegments) > 0 {
			fmt.Printf("--- Segments with missing indexes (%d): %v\n", len(missingSegments), missingSegments)
		}
	}
	return nil
}
