// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigreconciler

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

var _ spanconfig.Reconciler = &Reconciler{}

// Reconciler is the concrete implementation of the Reconciler interface.
type Reconciler struct {
	codec            keys.SQLCodec
	DB               *kv.DB
	settings         *cluster.Settings
	rangeFeedFactory *rangefeed.Factory
	clock            *hlc.Clock
	ie               sqlutil.InternalExecutor
	leaseManager     *lease.Manager
	stopper          *stop.Stopper
}

// NewReconciler returns a Reconciler.
func New(
	codec keys.SQLCodec,
	DB *kv.DB,
	settings *cluster.Settings,
	rangeFeedFactory *rangefeed.Factory,
	clock *hlc.Clock,
	ie sqlutil.InternalExecutor,
	leaseManager *lease.Manager,
	stopper *stop.Stopper,
) *Reconciler {
	return &Reconciler{
		codec:            codec,
		DB:               DB,
		settings:         settings,
		rangeFeedFactory: rangeFeedFactory,
		clock:            clock,
		ie:               ie,
		leaseManager:     leaseManager,
		stopper:          stopper,
	}
}

func (r *Reconciler) Reconcile(ctx context.Context) (<-chan spanconfig.Update, error) {

	updatesCh := make(chan spanconfig.Update)

	if err := r.stopper.RunAsyncTask(ctx, "full-reconcile", func(ctx context.Context) {
		entries, err := r.fullReconcile(ctx, r.leaseManager)
		if err != nil {
			log.Errorf(ctx, "error running full reconcile: %v", err)
		}
		for _, entry := range entries {
			update := spanconfig.Update{
				Deleted: false,
				Entry:   entry,
			}
			updatesCh <- update
		}
	}); err != nil {
		return nil, err
	}
	return updatesCh, nil
}

func (r *Reconciler) fullReconcile(
	ctx context.Context, leaseManager *lease.Manager,
) (entries []roachpb.SpanConfigEntry, err error) {
	if err = descs.Txn(
		ctx,
		r.settings,
		leaseManager,
		r.ie,
		r.DB,
		func(ctx context.Context, txn *kv.Txn, descsCol *descs.Collection) error {
			descs, err := descsCol.GetAllDescriptors(ctx, txn)
			ids := make(descpb.IDs, 0, len(descs))
			for _, desc := range descs {
				if desc.DescriptorType() == catalog.Table {
					ids = append(ids, desc.GetID())
				}
			}
			if err != nil {
				return err
			}
			entries, err = r.generateSpanConfigurations(ctx, txn, ids)
			return err
		}); err != nil {
		return nil, err
	}
	return entries, nil
}

func (r *Reconciler) generateSpanConfigurations(
	ctx context.Context, txn *kv.Txn, ids descpb.IDs,
) ([]roachpb.SpanConfigEntry, error) {
	// TODO(arul): Check if there is a utility for this/if this can go in a
	// utility.
	copyKey := func(k roachpb.Key) roachpb.Key {
		k2 := make([]byte, len(k))
		copy(k2, k)
		return k2
	}

	ret := make([]roachpb.SpanConfigEntry, 0, len(ids))
	for _, id := range ids {
		zone, err := sql.GetHydratedZoneConfigForTable(ctx, txn, r.codec, id)
		if err != nil {
			return nil, err
		}
		spanConfig, err := zone.ToSpanConfig()
		if err != nil {
			return nil, err
		}

		tablePrefix := r.codec.TablePrefix(uint32(id))
		prev := tablePrefix
		for i := range zone.SubzoneSpans {
			// We need to prepend the tablePrefix to the spans stored inside the
			// SubzoneSpans field because we store the stripped version here for
			// historical reasons.
			span := roachpb.Span{
				Key:    append(tablePrefix, zone.SubzoneSpans[i].Key...),
				EndKey: append(tablePrefix, zone.SubzoneSpans[i].EndKey...),
			}

			{
				// TODO(arul): Seems like the zone config code sets the EndKey to be nil
				// if the EndKey is Key.PrefixEnd() -- I'm not exactly sure why and still
				// need to investigate.
				if zone.SubzoneSpans[i].EndKey == nil {
					span.EndKey = span.Key.PrefixEnd()
				}
			}

			// If there is a "hole" in the spans covered by the subzones array we fill
			// it using the parent zone configuration.
			if !prev.Equal(span.Key) {
				ret = append(ret,
					roachpb.SpanConfigEntry{
						Span:   roachpb.Span{Key: copyKey(prev), EndKey: copyKey(span.Key)},
						Config: spanConfig,
					},
				)
			}

			// Add an entry for the subzone.
			subzoneSpanConfig, err := zone.Subzones[zone.SubzoneSpans[i].SubzoneIndex].Config.ToSpanConfig()
			if err != nil {
				return nil, err
			}
			ret = append(ret,
				roachpb.SpanConfigEntry{
					Span:   roachpb.Span{Key: copyKey(span.Key), EndKey: copyKey(span.EndKey)},
					Config: subzoneSpanConfig,
				},
			)

			prev = copyKey(span.EndKey)
		}

		if !prev.Equal(tablePrefix.PrefixEnd()) {
			ret = append(ret,
				roachpb.SpanConfigEntry{
					Span:   roachpb.Span{Key: prev, EndKey: tablePrefix.PrefixEnd()},
					Config: spanConfig,
				},
			)
		}
	}

	return ret, nil
}
