// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfig

import (
	"math/rand"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// TestEncodeDecodeSystemTarget ensures that encoding/decoding a SystemTarget
// is roundtripable.
func TestEncodeDecodeSystemTarget(t *testing.T) {
	for _, testTarget := range []SystemTarget{
		// Tenant targeting its logical cluster.
		makeTenantTargetOrFatal(t, roachpb.MakeTenantID(10), roachpb.MakeTenantID(10)),
		// System tenant targeting its logical cluster.
		makeTenantTargetOrFatal(t, roachpb.SystemTenantID, roachpb.SystemTenantID),
		// System tenant targeting a secondary tenant.
		makeTenantTargetOrFatal(t, roachpb.SystemTenantID, roachpb.MakeTenantID(10)),
		// System tenant targeting the entire cluster.
		MakeClusterTarget(),
	} {
		systemTarget, err := decodeSystemTarget(testTarget.encode())
		require.NoError(t, err)
		require.Equal(t, testTarget, systemTarget)

		// Next, we encode/decode a spanconfig.Target that wraps a SystemTarget.
		target := MakeTargetFromSystemTarget(systemTarget)
		decodedTarget := DecodeTarget(target.Encode())
		require.Equal(t, target, decodedTarget)
	}
}

// TestDecodeInvalidSpanAsSystemTarget ensures that decoding an invalid span
// as a system target fails.
func TestDecodeInvalidSpanAsSystemTarget(t *testing.T) {
	for _, tc := range []struct {
		span        roachpb.Span
		expectedErr string
	}{
		{
			span:        roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
			expectedErr: "span .* did not conform to SystemTarget encoding",
		},
		{
			// No end key.
			span:        roachpb.Span{Key: keys.SystemSpanConfigEntireKeyspace},
			expectedErr: "invalid end key in span",
		},
		{
			// Invalid end key.
			span: roachpb.Span{
				Key:    keys.SystemSpanConfigEntireKeyspace,
				EndKey: append(keys.SystemSpanConfigEntireKeyspace, byte('a')).PrefixEnd(),
			},
			expectedErr: "invalid end key in span",
		},
		{
			// Sentinel key for SystemSpanConfigEntireKeyspace should not have a
			// suffix.
			span: roachpb.Span{
				Key:    append(keys.SystemSpanConfigEntireKeyspace, byte('a')),
				EndKey: append(keys.SystemSpanConfigEntireKeyspace, byte('a')).PrefixEnd(),
			},
			expectedErr: "span .* did not conform to SystemTarget encoding",
		},
	} {
		_, err := decodeSystemTarget(tc.span)
		require.Error(t, err)
		require.True(t, testutils.IsError(err, tc.expectedErr))
	}
}

// TestSystemTargetValidation ensures target.validate() works as expected.
func TestSystemTargetValidation(t *testing.T) {
	for _, tc := range []struct {
		sourceTenantID roachpb.TenantID
		targetTenantID roachpb.TenantID
		expErr         string
	}{
		{
			// Secondary tenants cannot target the system tenant.
			sourceTenantID: roachpb.MakeTenantID(10),
			targetTenantID: roachpb.SystemTenantID,
			expErr:         "secondary tenant 10 cannot target another tenant with ID",
		},
		{
			// Secondary tenants cannot target other secondary tenants.
			sourceTenantID: roachpb.MakeTenantID(10),
			targetTenantID: roachpb.MakeTenantID(20),
			expErr:         "secondary tenant 10 cannot target another tenant with ID",
		},
		// Test some valid targets.
		{
			// System tenant targeting secondary tenant is allowed.
			sourceTenantID: roachpb.SystemTenantID,
			targetTenantID: roachpb.MakeTenantID(20),
		},
		{
			// System tenant targeting itself is allowed.
			sourceTenantID: roachpb.SystemTenantID,
			targetTenantID: roachpb.SystemTenantID,
		},
		{
			// Secondary tenant targeting itself is allowed.
			sourceTenantID: roachpb.MakeTenantID(10),
			targetTenantID: roachpb.MakeTenantID(10),
		},
	} {
		target := SystemTarget{
			SourceTenantID: tc.sourceTenantID,
			TargetTenantID: &tc.targetTenantID,
		}
		require.True(t, testutils.IsError(target.validate(), tc.expErr))
	}
}

// TestTargetSortingRandomized ensures we sort targets correctly.
func TestTargetSortingRandomized(t *testing.T) {
	// Construct a set of sorted targets.
	sortedTargets := Targets{
		MakeTargetFromSystemTarget(MakeClusterTarget()),
		MakeTargetFromSystemTarget(makeTenantTargetOrFatal(t, roachpb.SystemTenantID, roachpb.SystemTenantID)),
		MakeTargetFromSystemTarget(makeTenantTargetOrFatal(t, roachpb.SystemTenantID, roachpb.MakeTenantID(10))),
		MakeTargetFromSystemTarget(makeTenantTargetOrFatal(t, roachpb.SystemTenantID, roachpb.MakeTenantID(20))),
		MakeTargetFromSystemTarget(makeTenantTargetOrFatal(t, roachpb.MakeTenantID(5), roachpb.MakeTenantID(5))),
		MakeTargetFromSystemTarget(makeTenantTargetOrFatal(t, roachpb.MakeTenantID(10), roachpb.MakeTenantID(10))),
		MakeTargetFromSpan(roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}),
		MakeTargetFromSpan(roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("d")}),
		MakeTargetFromSpan(roachpb.Span{Key: roachpb.Key("y"), EndKey: roachpb.Key("z")}),
	}

	const numOps = 20
	for i := 0; i < numOps; i++ {
		tc := make(Targets, len(sortedTargets))
		copy(tc, sortedTargets)

		rand.Shuffle(len(tc), func(i, j int) {
			tc[i], tc[j] = tc[j], tc[i]
		})

		sort.Sort(tc)
		require.Equal(t, sortedTargets, tc)
	}
}

// TestSpanTargetsConstructedInSystemSpanConfigKeyspace ensures that
// constructing span targets
func TestSpanTargetsConstructedInSystemSpanConfigKeyspace(t *testing.T) {
	for _, tc := range []roachpb.Span{
		MakeClusterTarget().encode(),
		makeTenantTargetOrFatal(t, roachpb.MakeTenantID(10), roachpb.MakeTenantID(10)).encode(),
		makeTenantTargetOrFatal(t, roachpb.SystemTenantID, roachpb.SystemTenantID).encode(),
		makeTenantTargetOrFatal(t, roachpb.SystemTenantID, roachpb.MakeTenantID(10)).encode(),
		{
			// Extends into from the left
			Key:    keys.TimeseriesKeyMax,
			EndKey: keys.SystemSpanConfigPrefix.Next(), // End Key isn't inclusive.
		},
		{
			// Entirely contained.
			Key:    keys.SystemSpanConfigPrefix.Next(),
			EndKey: keys.SystemSpanConfigPrefix.Next().PrefixEnd(),
		},
		{
			// Extends beyond on the right.
			Key:    keys.SystemSpanConfigPrefix.Next().PrefixEnd(),
			EndKey: keys.SystemSpanConfigKeyMax.Next().Next(),
		},
	} {
		require.Panics(t, func() { MakeTargetFromSpan(tc) })
	}
}

func makeTenantTargetOrFatal(
	t *testing.T, sourceID roachpb.TenantID, targetID roachpb.TenantID,
) SystemTarget {
	target, err := MakeTenantTarget(sourceID, targetID)
	require.NoError(t, err)
	return target
}
