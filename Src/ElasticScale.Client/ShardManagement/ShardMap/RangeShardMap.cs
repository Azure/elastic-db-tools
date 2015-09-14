// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Represents a shard map of ranges.
    /// </summary>
    /// <typeparam name="TKey">Key type.</typeparam>
    public sealed class RangeShardMap<TKey> : ShardMap, ICloneable<ShardMap>, ICloneable<RangeShardMap<TKey>>
    {
        /// <summary>
        /// Mapping b/w key ranges and shards.
        /// </summary>
        internal RangeShardMapper<TKey> rsm;

        /// <summary>
        /// Constructs a new instance.
        /// </summary>
        /// <param name="manager">Reference to ShardMapManager.</param>
        /// <param name="ssm">Storage representation.</param>
        internal RangeShardMap(ShardMapManager manager, IStoreShardMap ssm) : base(manager, ssm)
        {
            Debug.Assert(manager != null);
            Debug.Assert(ssm != null);
            this.rsm = new RangeShardMapper<TKey>(this.Manager, this);
        }

        #region Sync OpenConnection methods

        /// <summary>
        /// Opens a regular <see cref="SqlConnection"/> to the shard 
        /// to which the specified key value is mapped, with <see cref="ConnectionOptions.Validate"/>.
        /// </summary>
        /// <param name="key">Input key value.</param>
        /// <param name="connectionString">
        /// Connection string with credential information such as SQL Server credentials or Integrated Security settings. 
        /// The hostname of the server and the database name for the shard are obtained from the lookup operation for key.
        /// </param>
        /// <returns>An opened SqlConnection.</returns>
        /// <remarks>
        /// Note that the <see cref="SqlConnection"/> object returned by this call is not protected against transient faults. 
        /// Callers should follow best practices to protect the connection against transient faults 
        /// in their application code, e.g., by using the transient fault handling 
        /// functionality in the Enterprise Library from Microsoft Patterns and Practices team.
        /// </remarks>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1061:DoNotHideBaseClassMethods")]
        public SqlConnection OpenConnectionForKey(TKey key, string connectionString)
        {
            return this.OpenConnectionForKey(key, connectionString, ConnectionOptions.Validate);
        }

        /// <summary>
        /// Opens a regular <see cref="SqlConnection"/> to the shard 
        /// to which the specified key value is mapped.
        /// </summary>
        /// <param name="key">Input key value.</param>
        /// <param name="connectionString">
        /// Connection string with credential information such as SQL Server credentials or Integrated Security settings. 
        /// The hostname of the server and the database name for the shard are obtained from the lookup operation for key.
        /// </param>
        /// <param name="options">Options for validation operations to perform on opened connection.</param>
        /// <returns>An opened SqlConnection.</returns>
        /// <remarks>
        /// Note that the <see cref="SqlConnection"/> object returned by this call is not protected against transient faults. 
        /// Callers should follow best practices to protect the connection against transient faults 
        /// in their application code, e.g., by using the transient fault handling 
        /// functionality in the Enterprise Library from Microsoft Patterns and Practices team.
        /// </remarks>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1061:DoNotHideBaseClassMethods")]
        public SqlConnection OpenConnectionForKey(TKey key, string connectionString, ConnectionOptions options)
        {
            ExceptionUtils.DisallowNullArgument(connectionString, "connectionString");

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                return this.rsm.OpenConnectionForKey(key, connectionString, options);
            }
        }

        #endregion

        #region Async OpenConnection methods

        /// <summary>
        /// Asynchronously opens a regular <see cref="SqlConnection"/> to the shard 
        /// to which the specified key value is mapped, with <see cref="ConnectionOptions.Validate"/>.
        /// </summary>
        /// <param name="key">Input key value.</param>
        /// <param name="connectionString">
        /// Connection string with credential information such as SQL Server credentials or Integrated Security settings. 
        /// The hostname of the server and the database name for the shard are obtained from the lookup operation for key.
        /// </param>
        /// <returns>A Task encapsulating an opened SqlConnection.</returns>
        /// <remarks>
        /// Note that the <see cref="SqlConnection"/> object returned by this call is not protected against transient faults. 
        /// Callers should follow best practices to protect the connection against transient faults 
        /// in their application code, e.g., by using the transient fault handling 
        /// functionality in the Enterprise Library from Microsoft Patterns and Practices team.
        /// All non-usage errors will be propagated via the returned Task.
        /// </remarks>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1061:DoNotHideBaseClassMethods")]
        public Task<SqlConnection> OpenConnectionForKeyAsync(TKey key, string connectionString)
        {
            return this.OpenConnectionForKeyAsync(key, connectionString, ConnectionOptions.Validate);
        }

        /// <summary>
        /// Asynchronously opens a regular <see cref="SqlConnection"/> to the shard 
        /// to which the specified key value is mapped.
        /// </summary>
        /// <param name="key">Input key value.</param>
        /// <param name="connectionString">
        /// Connection string with credential information such as SQL Server credentials or Integrated Security settings. 
        /// The hostname of the server and the database name for the shard are obtained from the lookup operation for key.
        /// </param>
        /// <param name="options">Options for validation operations to perform on opened connection.</param>
        /// <returns>A Task encapsulating an opened SqlConnection.</returns>
        /// <remarks>
        /// Note that the <see cref="SqlConnection"/> object returned by this call is not protected against transient faults. 
        /// Callers should follow best practices to protect the connection against transient faults 
        /// in their application code, e.g., by using the transient fault handling 
        /// functionality in the Enterprise Library from Microsoft Patterns and Practices team.
        /// All non-usage errors will be propagated via the returned Task.
        /// </remarks>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1061:DoNotHideBaseClassMethods")]
        public Task<SqlConnection> OpenConnectionForKeyAsync(TKey key, string connectionString, ConnectionOptions options)
        {
            ExceptionUtils.DisallowNullArgument(connectionString, "connectionString");

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                return this.rsm.OpenConnectionForKeyAsync(key, connectionString, options);
            }
        }

        #endregion

        /// <summary>
        /// Creates and adds a range mapping to ShardMap.
        /// </summary>
        /// <param name="creationInfo">Information about mapping to be added.</param>
        /// <returns>Newly created mapping.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods",
            MessageId = "0")]
        public RangeMapping<TKey> CreateRangeMapping(RangeMappingCreationInfo<TKey> creationInfo)
        {
            ExceptionUtils.DisallowNullArgument(creationInfo, "args");

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap,
                    "CreateRangeMapping", "Start; Shard: {0}", creationInfo.Shard.Location);

                Stopwatch stopwatch = Stopwatch.StartNew();

                RangeMapping<TKey> rangeMapping = this.rsm.Add(new RangeMapping<TKey>(this.Manager, creationInfo));

                stopwatch.Stop();

                Tracer.TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap,
                    "CreateRangeMapping", "Complete; Shard: {0}; Duration: {1}",
                    creationInfo.Shard.Location, stopwatch.Elapsed);

                return rangeMapping;
            }
        }

        /// <summary>
        /// Creates and adds a range mapping to ShardMap.
        /// </summary>
        /// <param name="range">Range for which to create the mapping.</param>
        /// <param name="shard">Shard associated with the range mapping.</param>
        /// <returns>Newly created mapping.</returns>
        public RangeMapping<TKey> CreateRangeMapping(Range<TKey> range, Shard shard)
        {
            ExceptionUtils.DisallowNullArgument(range, "range");
            ExceptionUtils.DisallowNullArgument(shard, "shard");

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                RangeMappingCreationInfo<TKey> args = new RangeMappingCreationInfo<TKey>(range, shard, MappingStatus.Online);

                Tracer.TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap,
                    "CreateRangeMapping", "Start; Shard: {0}", shard.Location);

                Stopwatch stopwatch = Stopwatch.StartNew();

                RangeMapping<TKey> rangeMapping = this.rsm.Add(new RangeMapping<TKey>(this.Manager, args));

                stopwatch.Stop();

                Tracer.TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap,
                    "CreateRangeMapping", "Complete; Shard: {0}; Duration: {1}", shard.Location, stopwatch.Elapsed);

                return rangeMapping;
            }
        }

        /// <summary>
        /// Removes a range mapping.
        /// </summary>
        /// <param name="mapping">Mapping being removed.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods",
            MessageId = "0")]
        public void DeleteMapping(RangeMapping<TKey> mapping)
        {
            this.DeleteMapping(mapping, MappingLockToken.NoLock);
        }

        /// <summary>
        /// Removes a range mapping.
        /// </summary>
        /// <param name="mapping">Mapping being removed.</param>
        /// <param name="mappingLockToken">An instance of <see cref="MappingLockToken"/></param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "1"),
            System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods",
            MessageId = "0")]
        public void DeleteMapping(RangeMapping<TKey> mapping, MappingLockToken mappingLockToken)
        {
            ExceptionUtils.DisallowNullArgument(mapping, "mapping");
            ExceptionUtils.DisallowNullArgument(mappingLockToken, "mappingLockToken");

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap,
                    "DeleteMapping", "Start; Shard: {0}", mapping.Shard.Location);

                Stopwatch stopwatch = Stopwatch.StartNew();

                this.rsm.Remove(mapping, mappingLockToken.LockOwnerId);

                stopwatch.Stop();

                Tracer.TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap,
                    "DeleteMapping", "Complete; Shard: {0}; Duration: {1}",
                    mapping.Shard.Location, stopwatch.Elapsed);
            }
        }

        /// <summary>
        /// Looks up the key value and returns the corresponding mapping.
        /// </summary>
        /// <param name="key">Input key value.</param>
        /// <returns>Mapping that contains the key value.</returns>
        public RangeMapping<TKey> GetMappingForKey(TKey key)
        {
            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap,
                    "GetMapping", "Start; Range Mapping Key Type: {0}", typeof(TKey));

                Stopwatch stopwatch = Stopwatch.StartNew();

                RangeMapping<TKey> rangeMapping = this.rsm.Lookup(key, false);

                stopwatch.Stop();

                Tracer.TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap,
                    "GetMapping", "Complete; Range Mapping Key Type: {0} Duration: {1}",
                    typeof(TKey), stopwatch.Elapsed);

                return rangeMapping;
            }
        }

        /// <summary>
        /// Tries to looks up the key value and place the corresponding mapping in <paramref name="rangeMapping"/>.
        /// </summary>
        /// <param name="key">Input key value.</param>
        /// <param name="rangeMapping">Mapping that contains the key value.</param>
        /// <returns><c>true</c> if mapping is found, <c>false</c> otherwise.</returns>
        public bool TryGetMappingForKey(TKey key, out RangeMapping<TKey> rangeMapping)
        {
            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap,
                    "TryLookupRangeMapping", "Start; ShardMap name: {0}; Range Mapping Key Type: {1}",
                    this.Name, typeof(TKey));

                Stopwatch stopwatch = Stopwatch.StartNew();

                bool result = this.rsm.TryLookup(key, false, out rangeMapping);

                stopwatch.Stop();

                Tracer.TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap,
                    "TryLookupRangeMapping", "Complete; ShardMap name: {0}; Range Mapping Key Type: {1}; Duration: {2}",
                    this.Name, typeof(TKey), stopwatch.Elapsed);

                return result;
            }
        }

        /// <summary>
        /// Gets all the range mappings for the shard map.
        /// </summary>
        /// <returns>Read-only collection of all range mappings on the shard map.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1006:DoNotNestGenericTypesInMemberSignatures",
            Justification = "Necessary to allow different types of keys")]
        public IReadOnlyList<RangeMapping<TKey>> GetMappings()
        {
            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.RangeShardMap,
                    "GetMappings", "Start;");

                Stopwatch stopwatch = Stopwatch.StartNew();

                IReadOnlyList<RangeMapping<TKey>> rangeMappings = this.rsm.GetMappingsForRange(null, null);

                stopwatch.Stop();

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.RangeShardMap,
                    "GetMappings", "Complete; Duration: {0}",
                    stopwatch.Elapsed);

                return rangeMappings;
            }
        }

        /// <summary>
        /// Gets all the range mappings that exist within given range.
        /// </summary>
        /// <param name="range">Range value, any mapping overlapping with the range will be returned.</param>
        /// <returns>Read-only collection of mappings that satisfy the given range constraint.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1006:DoNotNestGenericTypesInMemberSignatures",
            Justification = "Necessary to allow different types of keys")]
        public IReadOnlyList<RangeMapping<TKey>> GetMappings(Range<TKey> range)
        {
            ExceptionUtils.DisallowNullArgument(range, "range");

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.RangeShardMap,
                    "GetMappings", "Start; Range: {0}",
                    range);

                Stopwatch stopwatch = Stopwatch.StartNew();

                IReadOnlyList<RangeMapping<TKey>> rangeMappings = this.rsm.GetMappingsForRange(range, null);

                stopwatch.Stop();

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.RangeShardMap,
                    "GetMappings",
                    "Complete; Range: {0}; Duration: {1}",
                    range, stopwatch.Elapsed);

                return rangeMappings;
            }
        }

        /// <summary>
        /// Gets all the range mappings that exist for the given shard.
        /// </summary>
        /// <param name="shard">Shard for which the mappings will be returned.</param>
        /// <returns>Read-only collection of mappings that satisfy the given shard constraint.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0"),
         System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1006:DoNotNestGenericTypesInMemberSignatures",
             Justification = "Necessary to allow different types of keys")]
        public IReadOnlyList<RangeMapping<TKey>> GetMappings(Shard shard)
        {
            ExceptionUtils.DisallowNullArgument(shard, "shard");

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.RangeShardMap,
                    "GetMappings", "Start; Shard: {0}",
                    shard.Location);

                Stopwatch stopwatch = Stopwatch.StartNew();

                IReadOnlyList<RangeMapping<TKey>> rangeMappings = this.rsm.GetMappingsForRange(null, shard);

                stopwatch.Stop();

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.RangeShardMap,
                    "GetMappings", "Complete; Shard: {0}; Duration: {1}",
                    shard.Location, stopwatch.Elapsed);

                return rangeMappings;
            }
        }

        /// <summary>
        /// Gets all the range mappings that exist within given range and given shard.
        /// </summary>
        /// <param name="range">Range value, any mapping overlapping with the range will be returned.</param>
        /// <param name="shard">Shard for which the mappings will be returned.</param>
        /// <returns>Read-only collection of mappings that satisfy the given range and shard constraints.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "1"),
         System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1006:DoNotNestGenericTypesInMemberSignatures",
             Justification = "Necessary to allow different types of keys")]
        public IReadOnlyList<RangeMapping<TKey>> GetMappings(Range<TKey> range, Shard shard)
        {
            ExceptionUtils.DisallowNullArgument(range, "range");
            ExceptionUtils.DisallowNullArgument(shard, "shard");

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.RangeShardMap,
                    "GetMappings", "Start; Shard: {0}; Range: {1}",
                    shard.Location, range);

                Stopwatch stopwatch = Stopwatch.StartNew();

                IReadOnlyList<RangeMapping<TKey>> rangeMappings = this.rsm.GetMappingsForRange(range, shard);

                stopwatch.Stop();

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.RangeShardMap,
                    "GetMappings", "Complete; Shard: {0}; Duration: {1}",
                    shard.Location, stopwatch.Elapsed);

                return rangeMappings;
            }
        }

        /// <summary>
        /// Marks the specified mapping offline.
        /// </summary>
        /// <param name="mapping">Input range mapping.</param>
        /// <returns>An offline mapping.</returns>
        public RangeMapping<TKey> MarkMappingOffline(RangeMapping<TKey> mapping)
        {
            return this.MarkMappingOffline(mapping, MappingLockToken.NoLock);
        }

        /// <summary>
        /// Marks the specified mapping offline.
        /// </summary>
        /// <param name="mapping">Input range mapping.</param>
        /// <param name="mappingLockToken">An instance of <see cref="MappingLockToken"/></param>
        /// <returns>An offline mapping.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "1")]
        public RangeMapping<TKey> MarkMappingOffline(RangeMapping<TKey> mapping, MappingLockToken mappingLockToken)
        {
            ExceptionUtils.DisallowNullArgument(mapping, "mapping");
            ExceptionUtils.DisallowNullArgument(mappingLockToken, "mappingLockToken");

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.RangeShardMap,
                    "MarkMappingOffline", "Start; ");

                Stopwatch stopwatch = Stopwatch.StartNew();

                RangeMapping<TKey> result = this.rsm.MarkMappingOffline(mapping, mappingLockToken.LockOwnerId);

                stopwatch.Stop();

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.RangeShardMap,
                    "MarkMappingOffline", "Complete; Duration: {0}",
                    stopwatch.Elapsed);

                return result;
            }
        }

        /// <summary>
        /// Marks the specified mapping online.
        /// </summary>
        /// <param name="mapping">Input range mapping.</param>
        /// <returns>An online mapping.</returns>
        public RangeMapping<TKey> MarkMappingOnline(RangeMapping<TKey> mapping)
        {
            return this.MarkMappingOnline(mapping, MappingLockToken.NoLock);
        }

        /// <summary>
        /// Marks the specified mapping online.
        /// </summary>
        /// <param name="mapping">Input range mapping.</param>
        /// <param name="mappingLockToken">An instance of <see cref="MappingLockToken"/></param>
        /// <returns>An online mapping.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "1")]
        public RangeMapping<TKey> MarkMappingOnline(RangeMapping<TKey> mapping, MappingLockToken mappingLockToken)
        {
            ExceptionUtils.DisallowNullArgument(mapping, "mapping");
            ExceptionUtils.DisallowNullArgument(mappingLockToken, "mappingLockToken");

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.RangeShardMap,
                    "MarkMappingOnline", "Start; ");

                Stopwatch stopwatch = Stopwatch.StartNew();

                RangeMapping<TKey> result = this.rsm.MarkMappingOnline(mapping, mappingLockToken.LockOwnerId);

                stopwatch.Stop();

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.RangeShardMap,
                    "MarkMappingOnline", "Complete; Duration: {0}",
                    stopwatch.Elapsed);

                return result;
            }
        }

        /// <summary>
        /// Gets the lock owner id of the specified mapping.
        /// </summary>
        /// <param name="mapping">Input range mapping.</param>
        /// <returns>An instance of <see cref="MappingLockToken"/></returns>
        public MappingLockToken GetMappingLockOwner(RangeMapping<TKey> mapping)
        {
            ExceptionUtils.DisallowNullArgument(mapping, "mapping");

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.RangeShardMap,
                    "LookupLockOwner", "Start");

                Stopwatch stopwatch = Stopwatch.StartNew();

                Guid storeLockOwnerId = this.rsm.GetLockOwnerForMapping(mapping);

                stopwatch.Stop();

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.RangeShardMap,
                    "LookupLockOwner", "Complete; Duration: {0}; StoreLockOwnerId: {1}",
                    stopwatch.Elapsed, storeLockOwnerId);

                return new MappingLockToken(storeLockOwnerId);
            }
        }

        /// <summary>
        /// Locks the mapping for the specified owner
        /// The state of a locked mapping can only be modified by the lock owner.
        /// </summary>
        /// <param name="mapping">Input range mapping.</param>
        /// <param name="mappingLockToken">An instance of <see cref="MappingLockToken"/></param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "1")]
        public void LockMapping(RangeMapping<TKey> mapping, MappingLockToken mappingLockToken)
        {
            ExceptionUtils.DisallowNullArgument(mapping, "mapping");
            ExceptionUtils.DisallowNullArgument(mappingLockToken, "mappingLockToken");

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                // Generate a lock owner id
                Guid lockOwnerId = mappingLockToken.LockOwnerId;

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.RangeShardMap,
                    "Lock", "Start; LockOwnerId: {0}", lockOwnerId);

                Stopwatch stopwatch = Stopwatch.StartNew();

                this.rsm.LockOrUnlockMappings(mapping, lockOwnerId, LockOwnerIdOpType.Lock);

                stopwatch.Stop();

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.RangeShardMap,
                    "Lock", "Complete; Duration: {0}; StoreLockOwnerId: {1}",
                    stopwatch.Elapsed, lockOwnerId);
            }
        }

        /// <summary>
        /// Unlocks the specified mapping
        /// </summary>
        /// <param name="mapping">Input range mapping.</param>
        /// <param name="mappingLockToken">An instance of <see cref="MappingLockToken"/></param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "1")]
        public void UnlockMapping(RangeMapping<TKey> mapping, MappingLockToken mappingLockToken)
        {
            ExceptionUtils.DisallowNullArgument(mapping, "mapping");
            ExceptionUtils.DisallowNullArgument(mappingLockToken, "mappingLockToken");

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Guid lockOwnerId = mappingLockToken.LockOwnerId;
                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.RangeShardMap,
                    "Unlock", "Start; LockOwnerId: {0}", lockOwnerId);

                Stopwatch stopwatch = Stopwatch.StartNew();

                this.rsm.LockOrUnlockMappings(mapping, lockOwnerId, LockOwnerIdOpType.UnlockMappingForId);

                stopwatch.Stop();

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.RangeShardMap,
                    "UnLock", "Complete; Duration: {0}; StoreLockOwnerId: {1}",
                    stopwatch.Elapsed, lockOwnerId);
            }
        }

        /// <summary>
        /// Unlocks all mappings in this map that belong to the given <see cref="MappingLockToken"/>
        /// </summary>
        /// <param name="mappingLockToken">An instance of <see cref="MappingLockToken"/></param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        public void UnlockMapping(MappingLockToken mappingLockToken)
        {
            ExceptionUtils.DisallowNullArgument(mappingLockToken, "mappingLockToken");

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Guid lockOwnerId = mappingLockToken.LockOwnerId;
                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.RangeShardMap,
                    "UnlockAllMappingsWithLockOwnerId", "Start; LockOwnerId: {0}", lockOwnerId);

                Stopwatch stopwatch = Stopwatch.StartNew();

                this.rsm.LockOrUnlockMappings(null, lockOwnerId, LockOwnerIdOpType.UnlockAllMappingsForId);

                stopwatch.Stop();

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.RangeShardMap,
                    "UnlockAllMappingsWithLockOwnerId", "Complete; Duration: {0}",
                    stopwatch.Elapsed);
            }
        }

        /// <summary>
        /// Updates a <see cref="RangeMapping{TKey}"/> with the updates provided in 
        /// the <paramref name="update"/> parameter.
        /// </summary>
        /// <param name="currentMapping">Mapping being updated.</param>
        /// <param name="update">Updated properties of the mapping.</param>
        /// <returns>New instance of mapping with updated information.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods",
            MessageId = "0")]
        public RangeMapping<TKey> UpdateMapping(RangeMapping<TKey> currentMapping, RangeMappingUpdate update)
        {
            return this.UpdateMapping(currentMapping, update, MappingLockToken.NoLock);
        }

        /// <summary>
        /// Updates a <see cref="RangeMapping{TKey}"/> with the updates provided in 
        /// the <paramref name="update"/> parameter.
        /// </summary>
        /// <param name="currentMapping">Mapping being updated.</param>
        /// <param name="update">Updated properties of the mapping.</param>
        /// <param name="mappingLockToken">An instance of <see cref="MappingLockToken"/></param>
        /// <returns>New instance of mapping with updated information.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "2"), System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods",
            MessageId = "0")]
        public RangeMapping<TKey> UpdateMapping(RangeMapping<TKey> currentMapping, RangeMappingUpdate update,
            MappingLockToken mappingLockToken)
        {
            ExceptionUtils.DisallowNullArgument(currentMapping, "currentMapping");
            ExceptionUtils.DisallowNullArgument(update, "update");
            ExceptionUtils.DisallowNullArgument(mappingLockToken, "mappingLockToken");

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap,
                    "UpdateMapping", "Start; Current mapping shard: {0}",
                    currentMapping.Shard.Location);

                Stopwatch stopwatch = Stopwatch.StartNew();

                RangeMapping<TKey> rangeMapping = this.rsm.Update(currentMapping, update, mappingLockToken.LockOwnerId);

                stopwatch.Stop();

                Tracer.TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap,
                    "UpdateMapping", "Complete; Current mapping shard: {0}; Duration: {1}",
                    currentMapping.Shard.Location, stopwatch.Elapsed);

                return rangeMapping;
            }
        }

        /// <summary>
        /// Splits the specified mapping into two new mappings using the specified key as boundary. 
        /// The new mappings point to the same shard as the existing mapping.
        /// </summary>
        /// <param name="existingMapping">Existing mapping.</param>
        /// <param name="splitAt">Split point.</param>
        /// <returns>Read-only collection of two new mappings that were created.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1006:DoNotNestGenericTypesInMemberSignatures"),
            System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods",
            MessageId = "0", Justification = "Necessary to allow different types of keys")]
        public IReadOnlyList<RangeMapping<TKey>> SplitMapping(RangeMapping<TKey> existingMapping, TKey splitAt)
        {
            return this.SplitMapping(existingMapping, splitAt, MappingLockToken.NoLock);
        }

        /// <summary>
        /// Splits the specified mapping into two new mappings using the specified key as boundary. 
        /// The new mappings point to the same shard as the existing mapping.
        /// </summary>
        /// <param name="existingMapping">Existing mapping.</param>
        /// <param name="splitAt">Split point.</param>
        /// <param name="mappingLockToken">An instance of <see cref="MappingLockToken"/></param>
        /// <returns>Read-only collection of two new mappings that were created.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "2"), System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1006:DoNotNestGenericTypesInMemberSignatures"),
            System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods",
            MessageId = "0", Justification = "Necessary to allow different types of keys")]
        public IReadOnlyList<RangeMapping<TKey>> SplitMapping(RangeMapping<TKey> existingMapping, TKey splitAt,
            MappingLockToken mappingLockToken)
        {
            ExceptionUtils.DisallowNullArgument(existingMapping, "existingMapping");
            ExceptionUtils.DisallowNullArgument(mappingLockToken, "mappingLockToken");

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap,
                    "SplitMapping", "Start; Shard: {0}", existingMapping.Shard.Location);

                Stopwatch stopwatch = Stopwatch.StartNew();

                IReadOnlyList<RangeMapping<TKey>> rangeMapping = this.rsm.Split(existingMapping, splitAt, mappingLockToken.LockOwnerId);

                stopwatch.Stop();

                Tracer.TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap,
                    "SplitMapping", "Complete; Shard: {0}; Duration: {1}",
                    existingMapping.Shard.Location, stopwatch.Elapsed);

                return rangeMapping;
            }
        }

        /// <summary>
        /// Merges 2 contiguous mappings into a single mapping. Both left and right mappings should point
        /// to the same location and must be contiguous.
        /// </summary>
        /// <param name="left">Left mapping.</param>
        /// <param name="right">Right mapping.</param>
        /// <returns>Mapping that results from the merge operation.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods",
            MessageId = "0"),
        System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods",
            MessageId = "1")]
        public RangeMapping<TKey> MergeMappings(RangeMapping<TKey> left, RangeMapping<TKey> right)
        {
            return this.MergeMappings(left, right, MappingLockToken.NoLock, MappingLockToken.NoLock);
        }

        /// <summary>
        /// Merges 2 contiguous mappings into a single mapping. Both left and right mappings should point
        /// to the same location and must be contiguous.
        /// </summary>
        /// <param name="left">Left mapping.</param>
        /// <param name="right">Right mapping.</param>
        /// <param name="leftMappingLockToken">An instance of <see cref="MappingLockToken"/> for the left mapping</param>
        /// <param name="rightMappingLockToken">An instance of <see cref="MappingLockToken"/> for the right mapping</param>
        /// <returns>Mapping that results from the merge operation.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "2"), System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "3"), System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods",
            MessageId = "0"),
        System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods",
            MessageId = "1")]
        public RangeMapping<TKey> MergeMappings(RangeMapping<TKey> left, RangeMapping<TKey> right,
            MappingLockToken leftMappingLockToken, MappingLockToken rightMappingLockToken)
        {
            ExceptionUtils.DisallowNullArgument(left, "left");
            ExceptionUtils.DisallowNullArgument(right, "right");
            ExceptionUtils.DisallowNullArgument(leftMappingLockToken, "leftMappingLockToken");
            ExceptionUtils.DisallowNullArgument(rightMappingLockToken, "rightMappingLockToken");

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap,
                    "SplitMapping", "Start; Left Shard: {0}; Right Shard: {1}",
                    left.Shard.Location, right.Shard.Location);

                Stopwatch stopwatch = Stopwatch.StartNew();

                RangeMapping<TKey> rangeMapping = this.rsm.Merge(left, right, leftMappingLockToken.LockOwnerId, rightMappingLockToken.LockOwnerId);

                stopwatch.Stop();

                Tracer.TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap,
                    "SplitMapping", "Complete; Duration: {0}", stopwatch.Elapsed);

                return rangeMapping;
            }
        }

        /// <summary>
        /// Gets the mapper. This method is used by OpenConnection/Lookup of V.
        /// </summary>
        /// <typeparam name="V">Shard provider type.</typeparam>
        /// <returns>RangeShardMapper for given key type.</returns>
        internal override IShardMapper<V> GetMapper<V>()
        {
            return this.rsm as IShardMapper<V>;
        }

        /// <summary>
        /// Clones the given range shard map.
        /// </summary>
        /// <returns>A cloned instance of the range shard map.</returns>
        public RangeShardMap<TKey> Clone()
        {
            return this.CloneCore() as RangeShardMap<TKey>;
        }

        #region ICloneable<ShardMap>

        /// <summary>
        /// Clones the given shard map.
        /// </summary>
        /// <returns>A cloned instance of the shard map.</returns>
        ShardMap ICloneable<ShardMap>.Clone()
        {
            return this.CloneCore();
        }

        /// <summary>
        /// Clones the current shard map instance.
        /// </summary>
        /// <returns>Cloned shard map instance.</returns>
        protected override ShardMap CloneCore()
        {
            return new RangeShardMap<TKey>(
                this.Manager,
                this.StoreShardMap);
        }

        #endregion ICloneable<ShardMap>

        /// <summary>
        /// The Tracer
        /// </summary>
        private static ILogger Tracer
        {
            get
            {
                return TraceHelper.Tracer;
            }
        }
    }
}
