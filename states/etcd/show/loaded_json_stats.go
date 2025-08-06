package show

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
)

type LoadedJSONStatsParam struct {
	framework.ParamBase `use:"show loaded-json-stats" desc:"display loaded json stats information"`
	CollectionID        int64 `name:"collection" default:"0" desc:"collection id to filter with"`
	SegmentID           int64 `name:"segment" default:"0" desc:"segment id to filter with"`
	FieldID             int64 `name:"field" default:"0" desc:"field id to filter with"`
}

// LoadedJsonStatsCommand returns show loaded-json-stats command.
func (c *ComponentShow) LoadedJSONStatsCommand(ctx context.Context, p *LoadedJSONStatsParam) error {
	// 1. get all query nodes
	sessions, err := common.ListServers(ctx, c.client, c.metaPath, "querynode")
	if err != nil {
		return err
	}
	if len(sessions) == 0 {
		fmt.Println("no query nodes found")
		return nil
	}
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

			fmt.Printf("  collection %d, segment %d:\n", segment.GetCollection(), segment.GetID())
			jsonStats := segment.GetJsonStatsInfo()
			if len(jsonStats) == 0 {
				continue
			}
			for fieldId, jsonStat := range jsonStats {
				if p.FieldID != 0 && p.FieldID != fieldId {
					continue
				}
				fmt.Printf("    field [%d]: index stats: %s\n", fieldId, jsonStat)
			}
		}
	}
	return nil
}
