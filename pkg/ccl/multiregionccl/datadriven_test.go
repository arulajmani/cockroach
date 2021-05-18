// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package multiregionccl_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
)

// TestMultiRegionDataDriven is a data-driven test to test various multi-region
// invariants at a high level. This is accomplished by allowing custom cluster
// configurations when creating the test cluster and providing directives to
// assert expectations in query traces.
//
// It offers the following commands:
// "new-cluster localities=<localities>": creates a new cluster with
// len(localities) number of nodes. The set of available localities is
// hard-coded in the localityCfgs map below. A locality entry may be repeated to
// create more than one node in a region. The order in which the localities are
// provided may later be used to index into a particular node to run a query.
//
// "set-server idx=server_number": set the node at server_number (in the order
// specified when creating the cluster) as the server to execute queries on by
// default. Defaults to the first node in the cluster.
//
// "exec-sql [idx=server_number]": executes the input SQL query on the target
// server, if provided. The query is executed on the node that has been set as
// using "set-server".
//
// "query-sql [idx=server_number] [serve-locally] [serve-follower-reads]:
// Similar to exec-sql, but also expects results to be as desired. Provides two
// optional flags for testing:
//    - serve-locally: ensures that the query was routed to the local
//    replica.
//    -	serve-follower-reads: ensures that the query was served using a follower
//    read. Currently, this flag only works when "serve-locally-soon" is provided
//    as well.
// These trace analysis parameters only work for "simple" queries which perform
// a single kv operation.
//
// "wait-for-zone-config-changes" table-name=tbName [num-voters=num]
// [num-non-voters=num]: finds the range belonging to the given tbName's prefix
// key and runs it through the split, replicate, and raftsnapshot queue. If the
// num-voters and num-non-voters arguments are provided, it then makes sure that
// the range conforms to those. All this is done in a succeeds soon as any of
// these steps may error out for completely legitimate reasons.
// It's worth noting that num-voters and num-non-voters must be provided as
// arguments and cannot be returned like the way a lot of data-driven tests
// would ideally do this kind of validation. This is because zone config updates,
// which are gossiped, may not have been seen by the leaseholder node when the
// replica is run through the replicate queue. As such, it is completely
// reasonable to have this operation retry until this has happened and needs to
// be wrapped inside a succeeds soon. The only way to do this is to accept these
// things as a argument, instead of returning the result.
//
// refresh-range-descriptor-cache idx=server_number table-name=tbName: runs the
// given query to refresh the range descriptor cache. Then, using the table-name
// argument, the closed timestamp policy is returned.
//
// "cleanup-cluster": destroys the cluster. Must be done before creating a new
// cluster.
func TestMultiRegionDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	datadriven.Walk(t, "testdata/", func(t *testing.T, path string) {
		ds := datadrivenTestState{}
		defer ds.cleanup(ctx)
		var mu syncutil.Mutex
		var traceStmt string
		var recCh chan tracing.Recording
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "new-cluster":
				if ds.tc != nil {
					return errors.New("cluster already exists, cleanup cluster first").Error()
				}
				var localities string
				if d.HasArg(serverLocalities) {
					d.ScanArgs(t, serverLocalities, &localities)
				}
				if localities == "" {
					return errors.Newf("can't create a cluster without %s", serverLocalities).Error()
				}
				serverArgs := make(map[int]base.TestServerArgs)
				localityNames := strings.Split(localities, ",")
				recCh = make(chan tracing.Recording, 1)
				for i, localityName := range localityNames {
					localityCfg, found := localityCfgs[localityName]
					if !found {
						return errors.Newf("cannot create a server in locality %s", localityName).Error()
					}
					serverArgs[i] = base.TestServerArgs{
						Locality: localityCfg,
						Knobs: base.TestingKnobs{
							SQLExecutor: &sql.ExecutorTestingKnobs{
								WithStatementTrace: func(trace tracing.Recording, stmt string) {
									mu.Lock()
									defer mu.Unlock()
									if stmt == traceStmt {
										recCh <- trace
									}
								},
							},
						},
					}
				}
				numServers := len(localityNames)
				tc := testcluster.StartTestCluster(t, numServers, base.TestClusterArgs{
					ServerArgsPerNode: serverArgs,
				})
				ds.tc = tc
				ds.curDB = tc.ServerConn(0)

			case "cleanup-cluster":
				ds.cleanup(ctx)
			case "exec-sql":
				sqlDB := ds.curDB
				if d.HasArg(serverIdx) {
					var err error
					var idx int
					d.ScanArgs(t, serverIdx, &idx)
					sqlDB, err = ds.getSQLConn(idx)
					if err != nil {
						return err.Error()
					}
				}
				_, err := sqlDB.Exec(d.Input)
				if err != nil {
					return err.Error()
				}

			case "query-sql":
				if d.HasArg(expectFollowerRead) && !d.HasArg(expectServeLocally) {
					return errors.Newf(
						"%q is only allowed in conjunction with %q currently",
						expectFollowerRead,
						expectServeLocally,
					).Error()
				}
				var rows *gosql.Rows
				queryFunc := func() error {
					sqlDB := ds.curDB
					if d.HasArg(serverIdx) {
						var err error
						var idx int
						d.ScanArgs(t, serverIdx, &idx)
						sqlDB, err = ds.getSQLConn(idx)
						if err != nil {
							return err
						}
					}
					if d.HasArg(expectServeLocally) {
						mu.Lock()
						traceStmt = d.Input
						mu.Unlock()
						defer func() {
							mu.Lock()
							traceStmt = ""
							mu.Unlock()
						}()
					}
					var err error
					rows, err = sqlDB.Query(d.Input)
					if err != nil {
						return err
					}
					if d.HasArg(expectServeLocally) {
						rec := <-recCh
						localRead, followerRead, err := checkReadServedLocallyInSimpleRecording(rec)
						if err != nil {
							return err
						}
						if !localRead {
							return errors.New("did not serve locally")
						}
						if d.HasArg(expectFollowerRead) && !followerRead {
							return errors.New("did not serve follower read")
						}
					}
					return nil
				}

				err := queryFunc()
				if err != nil {
					return err.Error()
				}
				cols, err := rows.Columns()
				elemsI := make([]interface{}, len(cols))
				for i := range elemsI {
					elemsI[i] = new(interface{})
				}
				elems := make([]string, len(cols))
				// Build string output of the row data.
				var output strings.Builder
				for rows.Next() {
					if err := rows.Scan(elemsI...); err != nil {
						t.Fatal(err)
					}
					for i, elem := range elemsI {
						val := *(elem.(*interface{}))
						elems[i] = fmt.Sprintf("%v", val)
					}
					output.WriteString(strings.Join(elems, " "))
					output.WriteString("\n")
					if err := rows.Err(); err != nil {
						t.Fatal(err)
					}
				}
				return output.String()

			case "refresh-range-descriptor-cache":
				if !d.HasArg(tableName) {
					return errors.Newf("no %q provided", tableName).Error()
				}
				if !d.HasArg(serverIdx) {
					return errors.Newf("no %s provided", serverIdx).Error()
				}
				var tbName string
				d.ScanArgs(t, tableName, &tbName)
				var idx int
				d.ScanArgs(t, serverIdx, &idx)
				sqlDB, err := ds.getSQLConn(idx)
				if err != nil {
					return err.Error()
				}
				// Execute the query that's supposed to populate the range descriptor
				// cache.
				_, err = sqlDB.Exec(d.Input)
				if err != nil {
					return err.Error()
				}
				// Ensure the range descriptor cache was indeed populated.
				var tableID uint32
				err = ds.curDB.QueryRow(fmt.Sprintf(`SELECT id from system.namespace WHERE name='%s'`, tbName)).Scan(&tableID)
				if err != nil {
					return err.Error()
				}
				cache := ds.tc.Server(idx).DistSenderI().(*kvcoord.DistSender).RangeDescriptorCache()
				tablePrefix := keys.MustAddr(keys.SystemSQLCodec.TablePrefix(tableID))
				entry := cache.GetCached(ctx, tablePrefix, false /* inverted */)
				if entry == nil {
					return errors.Newf("no entry found for %s in cache", tbName).Error()
				}
				return entry.ClosedTimestampPolicy().String()

			case "wait-for-zone-config-changes":
				if !d.HasArg(tableName) {
					return errors.Newf("no %s provided", tableName).Error()
				}
				var tbName string
				d.ScanArgs(t, tableName, &tbName)
				var tableID uint32
				err := ds.curDB.QueryRow(fmt.Sprintf(`SELECT id from system.namespace WHERE name='%s'`, tbName)).Scan(&tableID)
				if err != nil {
					return err.Error()
				}
				tablePrefix := keys.MustAddr(keys.SystemSQLCodec.TablePrefix(tableID))
				var repl *kvserver.Replica
				// There's a lot going on here and things can fail at various steps, for
				// completely legitimate reasons, which is why this thing needs to be
				// wrapped in a succeeds soon.
				if err := testutils.SucceedsSoonError(func() error {
					leaseHolder := ds.tc.Server(0)
					found := false
					desc, err := ds.tc.LookupRange(tablePrefix.AsRawKey())
					leaseHolderInfo, err := ds.tc.FindRangeLeaseHolder(desc, nil)
					if err != nil {
						return err
					}
					for i := 0; i < ds.tc.NumServers(); i++ {
						if ds.tc.Server(i).NodeID() == leaseHolderInfo.NodeID {
							leaseHolder = ds.tc.Server(i)
							found = true
							break
						}
					}
					if !found {
						errors.Newf("did not find leaseholder for %s", tbName)
					}

					store, err := leaseHolder.GetStores().(*kvserver.Stores).GetStore(leaseHolder.GetFirstStoreID())
					if err != nil {
						return err
					}
					repl = store.LookupReplica(tablePrefix)
					if repl == nil {
						return errors.New(`could not find replica`)
					}
					for _, queueName := range []string{"split", "replicate", "raftsnapshot"} {
						_, processErr, err := store.ManuallyEnqueue(ctx, queueName, repl, true /* skipShouldQueue */)
						if processErr != nil {
							return processErr
						}
						if err != nil {
							return err
						}
					}
					desc, err = ds.tc.LookupRange(tablePrefix.AsRawKey())
					if err != nil {
						return err
					}
					if d.HasArg(numVoters) {
						var voters int
						d.ScanArgs(t, numVoters, &voters)
						if len(desc.Replicas().VoterDescriptors()) != voters {
							return errors.Newf("unexpected number of voters %d, expected %d",
								len(desc.Replicas().VoterDescriptors()), voters)
						}
					}
					if d.HasArg(numNonVoters) {
						var nonVoters int
						d.ScanArgs(t, numNonVoters, &nonVoters)
						if len(desc.Replicas().NonVoterDescriptors()) != nonVoters {
							return errors.Newf("unexpected number of non-voters %d, expected %d",
								len(desc.Replicas().NonVoterDescriptors()), nonVoters)
						}
					}
					return nil
				}); err != nil {
					return err.Error()
				}

			case "set-server":
				idx := -1
				if d.HasArg(serverIdx) {
					d.ScanArgs(t, serverIdx, &idx)
				}
				sqlDB, err := ds.getSQLConn(idx)
				if err != nil {
					return err.Error()
				}
				ds.curDB = sqlDB
			default:
				return errors.New("unknown command").Error()
			}
			return ""
		})
	})
}

// Constants corresponding to command-options accepted by the data-driven test.
const (
	expectFollowerRead = "serve-follower-read"
	expectServeLocally = "serve-locally"
	serverIdx          = "idx"
	serverLocalities   = "localities"
	tableName          = "table-name"
	numVoters          = "num-voters"
	numNonVoters       = "num-non-voters"
)

type datadrivenTestState struct {
	tc    serverutils.TestClusterInterface
	curDB *gosql.DB
}

// Set of localities to choose from for the data-driven test.
var localityCfgs = map[string]roachpb.Locality{
	"us-east-1": {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "us-east-1"},
		},
	},
	"us-central-1": {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "us-central-1"},
		},
	},
	"us-west-1": {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "us-west-1"},
		},
	},
	"eu-east-1": {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "eu-east-1"},
		},
	},
	"eu-central-1": {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "eu-central-1"},
		},
	},
	"eu-west-1": {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "eu-west-1"},
		},
	},
}

func (d *datadrivenTestState) cleanup(ctx context.Context) {
	if d.tc != nil {
		d.tc.Stopper().Stop(ctx)
	}
	d.tc = nil
}

func (d *datadrivenTestState) getSQLConn(idx int) (*gosql.DB, error) {
	if d.tc == nil {
		return nil, errors.New("no cluster exists")
	}
	if idx < 0 || idx >= d.tc.NumServers() {
		return nil, errors.Newf("invalid idx, must be in range [0, %d)", d.tc.NumServers())
	}
	return d.tc.ServerConn(idx), nil
}

// checkReadServedLocallyInSimpleRecording looks at a "simple" trace and returns
// if the query for served locally, and if it was, via a follower read. A
// "simple" trace is defined as one that contains a single "dist sender send"
// message. An error is returned if more than one (or no) "dist sender send"
// messages are found in the recording.
func checkReadServedLocallyInSimpleRecording(
	rec tracing.Recording,
) (servedLocally bool, servedUsingFollowerReads bool, err error) {
	foundDistSenderSend := false
	for _, sp := range rec {
		if sp.Operation == "dist sender send" {
			if foundDistSenderSend {
				return false, false, errors.New("recording contains > 1 dist sender send messages")
			}
			foundDistSenderSend = true
			localRead := tracing.LogsContainMsg(sp, kvbase.RoutingRequestLocallyMsg)
			if !localRead {
				return false, false, nil
			}
			// Check the child span to find out if the query was served using a
			// follower read.
			for _, span := range rec {
				if span.ParentSpanID == sp.SpanID {
					if tracing.LogsContainMsg(span, kvbase.FollowerReadServingMsg) {
						servedUsingFollowerReads = true
					}
				}
			}
		}
	}
	if !foundDistSenderSend {
		return false, false, errors.New("recording contains no dist sender send messages")
	}
	return true, servedUsingFollowerReads, nil
}
