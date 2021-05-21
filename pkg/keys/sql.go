// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package keys

import (
	"bytes"
	"math"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
)

// MakeTenantPrefix creates the key prefix associated with the specified tenant.
func MakeTenantPrefix(tenID roachpb.TenantID) roachpb.Key {
	if tenID == roachpb.SystemTenantID {
		return nil
	}
	return encoding.EncodeUvarintAscending(TenantPrefix, tenID.ToUint64())
}

// DecodeTenantPrefix determines the tenant ID from the key prefix, returning
// the remainder of the key (with the prefix removed) and the decoded tenant ID.
func DecodeTenantPrefix(key roachpb.Key) ([]byte, roachpb.TenantID, error) {
	if len(key) == 0 { // key.Equal(roachpb.RKeyMin)
		return nil, roachpb.SystemTenantID, nil
	}
	if key[0] != tenantPrefixByte {
		return key, roachpb.SystemTenantID, nil
	}
	rem, tenID, err := encoding.DecodeUvarintAscending(key[1:])
	if err != nil {
		return nil, roachpb.TenantID{}, err
	}
	return rem, roachpb.MakeTenantID(tenID), nil
}

// SQLCodec provides methods for encoding SQL table keys bound to a given
// tenant. The generator also provides methods for efficiently decoding keys
// previously generated by it. The generated keys are safe to use indefinitely
// and the generator is safe to use concurrently.
type SQLCodec struct {
	sqlEncoder
	sqlDecoder
	_ func() // incomparable
}

// sqlEncoder implements the encoding logic for SQL keys.
//
// The type is expressed as a pointer to a slice instead of a slice directly so
// that its zero value is not usable. Any attempt to use the methods on the zero
// value of a sqlEncoder will panic.
type sqlEncoder struct {
	buf *roachpb.Key
}

// sqlEncoder implements the decoding logic for SQL keys.
//
// The type is expressed as a pointer to a slice instead of a slice directly so
// that its zero value is not usable. Any attempt to use the methods on the zero
// value of a sqlDecoder will panic.
type sqlDecoder struct {
	buf *roachpb.Key
}

// MakeSQLCodec creates a new  SQLCodec suitable for manipulating SQL keys.
func MakeSQLCodec(tenID roachpb.TenantID) SQLCodec {
	k := MakeTenantPrefix(tenID)
	k = k[:len(k):len(k)] // bound capacity, avoid aliasing
	return SQLCodec{
		sqlEncoder: sqlEncoder{&k},
		sqlDecoder: sqlDecoder{&k},
	}
}

// SystemSQLCodec is a SQL key codec for the system tenant.
var SystemSQLCodec = MakeSQLCodec(roachpb.SystemTenantID)

// TODOSQLCodec is a SQL key codec. It is equivalent to SystemSQLCodec, but
// should be used when it is unclear which tenant should be referenced by the
// surrounding context.
var TODOSQLCodec = MakeSQLCodec(roachpb.SystemTenantID)

// ForSystemTenant returns whether the encoder is bound to the system tenant.
func (e sqlEncoder) ForSystemTenant() bool {
	return len(e.TenantPrefix()) == 0
}

// TenantPrefix returns the key prefix used for the tenants's data.
func (e sqlEncoder) TenantPrefix() roachpb.Key {
	return *e.buf
}

// TablePrefix returns the key prefix used for the table's data.
func (e sqlEncoder) TablePrefix(tableID uint32) roachpb.Key {
	k := e.TenantPrefix()
	return encoding.EncodeUvarintAscending(k, uint64(tableID))
}

// IndexPrefix returns the key prefix used for the index's data.
func (e sqlEncoder) IndexPrefix(tableID, indexID uint32) roachpb.Key {
	k := e.TablePrefix(tableID)
	return encoding.EncodeUvarintAscending(k, uint64(indexID))
}

// DescMetadataPrefix returns the key prefix for all descriptors in the
// system.descriptor table.
func (e sqlEncoder) DescMetadataPrefix() roachpb.Key {
	return e.IndexPrefix(DescriptorTableID, DescriptorTablePrimaryKeyIndexID)
}

// DescMetadataKey returns the key for the descriptor in the system.descriptor
// table.
func (e sqlEncoder) DescMetadataKey(descID uint32) roachpb.Key {
	k := e.DescMetadataPrefix()
	k = encoding.EncodeUvarintAscending(k, uint64(descID))
	return MakeFamilyKey(k, DescriptorTableDescriptorColFamID)
}

// TenantMetadataKey returns the key for the tenant metadata in the
// system.tenants table.
func (e sqlEncoder) TenantMetadataKey(tenID roachpb.TenantID) roachpb.Key {
	k := e.IndexPrefix(TenantsTableID, TenantsTablePrimaryKeyIndexID)
	k = encoding.EncodeUvarintAscending(k, tenID.ToUint64())
	return MakeFamilyKey(k, 0)
}

// SequenceKey returns the key used to store the value of a sequence.
func (e sqlEncoder) SequenceKey(tableID uint32) roachpb.Key {
	k := e.IndexPrefix(tableID, SequenceIndexID)
	k = encoding.EncodeUvarintAscending(k, 0)    // Primary key value
	k = MakeFamilyKey(k, SequenceColumnFamilyID) // Column family
	return k
}

// DescIDSequenceKey returns the key used for the descriptor ID sequence.
func (e sqlEncoder) DescIDSequenceKey() roachpb.Key {
	if e.ForSystemTenant() {
		// To maintain backwards compatibility, the system tenant uses a
		// separate, non-SQL, key to store its descriptor ID sequence.
		return descIDGenerator
	}
	return e.SequenceKey(DescIDSequenceID)
}

func (e sqlEncoder) ZoneConfigIDKey() roachpb.Key {
	// TODO(arul): make this a per-tenant thing.
	return zoneIDGenerator
}

// ZoneKeyPrefix returns the key prefix for id's row in the system.zones table.
func (e sqlEncoder) ZoneKeyPrefix(id uint32) roachpb.Key {
	if !e.ForSystemTenant() {
		panic("zone keys only exist in the system tenant's keyspace")
	}
	k := e.IndexPrefix(ZonesTableID, ZonesTablePrimaryIndexID)
	return encoding.EncodeUvarintAscending(k, uint64(id))
}

// ZoneKey returns the key for id's entry in the system.zones table.
func (e sqlEncoder) ZoneKey(id uint32) roachpb.Key {
	if !e.ForSystemTenant() {
		panic("zone keys only exist in the system tenant's keyspace")
	}
	k := e.ZoneKeyPrefix(id)
	return MakeFamilyKey(k, uint32(ZonesTableConfigColumnID))
}

// MigrationKeyPrefix returns the key prefix to store all migration details.
func (e sqlEncoder) MigrationKeyPrefix() roachpb.Key {
	return append(e.TenantPrefix(), MigrationPrefix...)
}

// MigrationLeaseKey returns the key that nodes must take a lease on in order to
// run system migrations on the cluster.
func (e sqlEncoder) MigrationLeaseKey() roachpb.Key {
	return append(e.TenantPrefix(), MigrationLease...)
}

// unexpected to avoid colliding with sqlEncoder.tenantPrefix.
func (d sqlDecoder) tenantPrefix() roachpb.Key {
	return *d.buf
}

// StripTenantPrefix validates that the given key has the proper tenant ID
// prefix, returning the remainder of the key with the prefix removed. The
// method returns an error if the key has a different tenant ID prefix than
// would be generated by the generator.
func (d sqlDecoder) StripTenantPrefix(key roachpb.Key) ([]byte, error) {
	tenPrefix := d.tenantPrefix()
	if !bytes.HasPrefix(key, tenPrefix) {
		return nil, errors.Errorf("invalid tenant id prefix: %q", key)
	}
	return key[len(tenPrefix):], nil
}

// DecodeTablePrefix validates that the given key has a table prefix, returning
// the remainder of the key (with the prefix removed) and the decoded descriptor
// ID of the table.
func (d sqlDecoder) DecodeTablePrefix(key roachpb.Key) ([]byte, uint32, error) {
	key, err := d.StripTenantPrefix(key)
	if err != nil {
		return nil, 0, err
	}
	if encoding.PeekType(key) != encoding.Int {
		return nil, 0, errors.Errorf("invalid key prefix: %q", key)
	}
	key, tableID, err := encoding.DecodeUvarintAscending(key)
	return key, uint32(tableID), err
}

// DecodeIndexPrefix validates that the given key has a table ID followed by an
// index ID, returning the remainder of the key (with the table and index prefix
// removed) and the decoded IDs of the table and index, respectively.
func (d sqlDecoder) DecodeIndexPrefix(key roachpb.Key) ([]byte, uint32, uint32, error) {
	key, tableID, err := d.DecodeTablePrefix(key)
	if err != nil {
		return nil, 0, 0, err
	}
	if encoding.PeekType(key) != encoding.Int {
		return nil, 0, 0, errors.Errorf("invalid key prefix: %q", key)
	}
	key, indexID, err := encoding.DecodeUvarintAscending(key)
	return key, tableID, uint32(indexID), err
}

// DecodeDescMetadataID decodes a descriptor ID from a descriptor metadata key.
func (d sqlDecoder) DecodeDescMetadataID(key roachpb.Key) (uint32, error) {
	// Extract table and index ID from key.
	remaining, tableID, _, err := d.DecodeIndexPrefix(key)
	if err != nil {
		return 0, err
	}
	if tableID != DescriptorTableID {
		return 0, errors.Errorf("key is not a descriptor table entry: %v", key)
	}
	// Extract the descriptor ID.
	_, id, err := encoding.DecodeUvarintAscending(remaining)
	if err != nil {
		return 0, err
	}
	if id > math.MaxUint32 {
		return 0, errors.Errorf("descriptor ID %d exceeds uint32 bounds", id)
	}
	return uint32(id), nil
}

// DecodeTenantMetadataID decodes a tenant ID from a tenant metadata key.
func (d sqlDecoder) DecodeTenantMetadataID(key roachpb.Key) (roachpb.TenantID, error) {
	// Extract table and index ID from key.
	remaining, tableID, _, err := d.DecodeIndexPrefix(key)
	if err != nil {
		return roachpb.TenantID{}, err
	}
	if tableID != TenantsTableID {
		return roachpb.TenantID{}, errors.Errorf("key is not a tenant table entry: %v", key)
	}
	// Extract the tenant ID.
	_, id, err := encoding.DecodeUvarintAscending(remaining)
	if err != nil {
		return roachpb.TenantID{}, err
	}
	return roachpb.MakeTenantID(id), nil
}
