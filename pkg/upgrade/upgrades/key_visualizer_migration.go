// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	// Import keyvisjob so that its init function will be called.
	_ "github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvisjob"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

// keyVisualizerTablesMigration creates the system.span_stats_unique_keys, system.span_stats_buckets,
// system.span_stats_samples, and system.span_stats_tenant_boundaries tables.
func keyVisualizerTablesMigration(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.SystemDeps,
) error {
	return nil
}
