// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests;

/// <summary>
/// Wrapper that allows for reusing test logic for either a
/// <see cref="ListShardMap{TKey}"/> or <see cref="RangeShardMap{TKey}"/>.
/// </summary>
/// <typeparam name="TKey"></typeparam>
internal class TestShardMap<TKey>
{
    private readonly RangeShardMap<TKey> _rsm;

    private readonly ListShardMap<TKey> _lsm;

    public TestShardMap(ListShardMap<TKey> lsm) => _lsm = lsm;

    public TestShardMap(RangeShardMap<TKey> rsm) => _rsm = rsm;

    public ShardMap ShardMap => (ShardMap)_rsm ?? _lsm;

    protected TResult Do<TResult>(
        Func<ListShardMap<TKey>, TResult> lsmFunc,
        Func<RangeShardMap<TKey>, TResult> rsmFunc) => _lsm != null
            ? lsmFunc(_lsm)
            : _rsm != null ? rsmFunc(_rsm) : throw new InvalidOperationException("Both lsm and rsm are null!");

    protected void Do<TResult>(
        Action<ListShardMap<TKey>> lsmFunc,
        Action<RangeShardMap<TKey>> rsmFunc)
    {
        if (_lsm != null)
        {
            lsmFunc(_lsm);
            return;
        }

        if (_rsm != null)
        {
            rsmFunc(_rsm);
            return;
        }

        throw new InvalidOperationException("Both lsm and rsm are null!");
    }

    public IMappingInfoProvider CreateMapping(TKey value, TKey nextValue, Shard shard) => Do<IMappingInfoProvider>(
            lsm => lsm.CreatePointMapping(value, shard),
            rsm => rsm.CreateRangeMapping(new Range<TKey>(value, nextValue), shard));

    public IMappingInfoProvider GetMappingForKey(TKey value) => Do<IMappingInfoProvider>(
            lsm => lsm.GetMappingForKey(value),
            rsm => rsm.GetMappingForKey(value));

    public IMappingInfoProvider GetMappingForKey(TKey value, LookupOptions lookupOptions) => Do<IMappingInfoProvider>(
            lsm => lsm.GetMappingForKey(value, lookupOptions),
            rsm => rsm.GetMappingForKey(value, lookupOptions));

    public bool TryGetMappingForKey(TKey value, out IMappingInfoProvider mapping)
    {
        if (_lsm != null)
        {
            var ret = _lsm.TryGetMappingForKey(value, out var x);
            mapping = x;
            return ret;
        }

        if (_rsm != null)
        {
            var ret = _rsm.TryGetMappingForKey(value, out var x);
            mapping = x;
            return ret;
        }

        throw new InvalidOperationException("Both lsm and rsm are null!");
    }

    public bool TryGetMappingForKey(TKey value, LookupOptions lookupOptions, out IMappingInfoProvider mapping)
    {
        if (_lsm != null)
        {
            var ret = _lsm.TryGetMappingForKey(value, lookupOptions, out var x);
            mapping = x;
            return ret;
        }

        if (_rsm != null)
        {
            var ret = _rsm.TryGetMappingForKey(value, lookupOptions, out var x);
            mapping = x;
            return ret;
        }

        throw new InvalidOperationException("Both lsm and rsm are null!");
    }

    public IMappingInfoProvider MarkMappingOffline(IMappingInfoProvider mapping) => Do<IMappingInfoProvider>(
            lsm => lsm.MarkMappingOffline((PointMapping<TKey>)mapping),
            rsm => rsm.MarkMappingOffline((RangeMapping<TKey>)mapping));

    public void DeleteMapping(IMappingInfoProvider mapping) => Do<IMappingInfoProvider>(
            lsm => lsm.DeleteMapping((PointMapping<TKey>)mapping),
            rsm => rsm.DeleteMapping((RangeMapping<TKey>)mapping));
}
