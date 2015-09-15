// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Represents a shard map of points where points are of the specified key.
    /// </summary>
    /// <typeparam name="TKey">Key type.</typeparam>
    public sealed class ListShardMap<TKey> : ShardMap, ICloneable<ShardMap>, ICloneable<ListShardMap<TKey>>
    {
        /// <summary>
        /// Mapper b/w points and shards.
        /// </summary>
        private ListShardMapper<TKey> _lsm;

        /// <summary>
        /// Constructs a new instance.
        /// </summary>
        /// <param name="manager">Reference to ShardMapManager.</param>
        /// <param name="ssm">Storage representation.</param>
        internal ListShardMap(ShardMapManager manager, IStoreShardMap ssm) : base(manager, ssm)
        {
            Debug.Assert(manager != null);
            Debug.Assert(ssm != null);

            _lsm = new ListShardMapper<TKey>(this.Manager, this);
        }

        #region Sync OpenConnection Methods

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
                return _lsm.OpenConnectionForKey(key, connectionString, options);
            }
        }

        #endregion

        #region Async OpenConnection Methods

        /// <summary>
        /// Asynchronously opens a regular <see cref="SqlConnection"/> to the shard 
        /// to which the specified key value is mapped, with <see cref="ConnectionOptions.Validate"/>.
        /// </summary>
        /// <param name="key">Input key value.</param>
        /// <param name="connectionString">
        /// Connection string with credential information such as SQL Server credentials or Integrated Security settings. 
        /// The hostname of the server and the database name for the shard are obtained from the lookup operation for key.
        /// </param>
        /// <returns>A Task encapsulating an open SqlConnection as the result</returns>
        /// <remarks>
        /// Note that the <see cref="SqlConnection"/> object returned by this call is not protected against transient faults. 
        /// Callers should follow best practices to protect the connection against transient faults 
        /// in their application code, e.g., by using the transient fault handling 
        /// functionality in the Enterprise Library from Microsoft Patterns and Practices team.
        /// All non-usage error related exceptions are reported via the returned Task.
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
        /// All non-usage error related exceptions are reported via the returned Task.
        /// </remarks>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1061:DoNotHideBaseClassMethods")]
        public Task<SqlConnection> OpenConnectionForKeyAsync(TKey key, string connectionString, ConnectionOptions options)
        {
            ExceptionUtils.DisallowNullArgument(connectionString, "connectionString");

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                return _lsm.OpenConnectionForKeyAsync(key, connectionString, options);
            }
        }

        #endregion

#if FUTUREWORK
        /// <summary>
        /// Creates and adds many point mappings to ShardMap.
        /// </summary>
        /// <param name="argsList">List of objects containing information about mappings to be added.</param>
        public IEnumerable<PointMapping<TKey>> CreateFromPointMappings(IEnumerable<PointMappingCreationArgs<TKey>> argsList)
        {
            ExceptionUtils.DisallowNullArgument(argsList, "argsList");

            // Partition the mappings by shardlocation.
            IDictionary<ShardLocation,IList<PointMapping<TKey>>> pointMappings = new Dictionary<ShardLocation,IList<PointMapping<TKey>>>();
            foreach (PointMappingCreationArgs<TKey> args in argsList)
            {
                ExceptionUtils.DisallowNullArgument(args, "args");
                if (!pointMappings.ContainsKey(args.Shard.Location))
                {
                    pointMappings[args.Shard.Location] = new List<PointMapping<TKey>>();
                }
                pointMappings[args.Shard.Location].Add(new PointMapping<TKey>(this.Manager, this.Id, args));
            }

            // For each shardlocation bulk add all the mappings to local only.
            ConcurrentQueue<Exception> exceptions = new ConcurrentQueue<Exception>();
            Parallel.ForEach(pointMappings, (kvp) =>
            {
                try
                {
                    this.lsm.AddLocals(kvp.Value, kvp.Key);
                }
                catch (Exception e)
                {
                    exceptions.Enqueue(e);
                }
            });

            if (exceptions.Count > 0)
            {
                throw new AggregateException(exceptions);
            }

            // Rebuild the global from locals.
            RecoveryManager recoveryManager = this.Manager.GetRecoveryManager();
            recoveryManager.RebuildShardMapManager(pointMappings.Keys);
            return pointMappings.Values.SelectMany(x => x.AsEnumerable());
        }
#endif

        /// <summary>
        /// Creates and adds a point mapping to ShardMap.
        /// </summary>
        /// <param name="creationInfo">Information about mapping to be added.</param>
        /// <returns>Newly created mapping.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods",
            MessageId = "0")]
        public PointMapping<TKey> CreatePointMapping(PointMappingCreationInfo<TKey> creationInfo)
        {
            ExceptionUtils.DisallowNullArgument(creationInfo, "args");

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Stopwatch stopwatch = Stopwatch.StartNew();

                string mappingKey = BitConverter.ToString(creationInfo.Key.RawValue);
                Tracer.TraceInfo(TraceSourceConstants.ComponentNames.ListShardMap,
                    "CreatePointMapping", "Start; ShardMap name: {0}; Point Mapping: {1} ",
                    this.Name, mappingKey);

                PointMapping<TKey> pointMapping = _lsm.Add(new PointMapping<TKey>(this.Manager, creationInfo));

                stopwatch.Stop();

                Tracer.TraceInfo(TraceSourceConstants.ComponentNames.ListShardMap,
                    "CreatePointMapping", "Complete; ShardMap name: {0}; Point Mapping: {1}; Duration: {2}",
                    this.Name, mappingKey, stopwatch.Elapsed);

                return pointMapping;
            }
        }

        /// <summary>
        /// Creates and adds a point mapping to ShardMap.
        /// </summary>
        /// <param name="point">Point for which to create the mapping.</param>
        /// <param name="shard">Shard associated with the point mapping.</param>
        /// <returns>Newly created mapping.</returns>
        public PointMapping<TKey> CreatePointMapping(TKey point, Shard shard)
        {
            ExceptionUtils.DisallowNullArgument(shard, "shard");

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                PointMappingCreationInfo<TKey> args = new PointMappingCreationInfo<TKey>(point, shard, MappingStatus.Online);

                string mappingKey = BitConverter.ToString(args.Key.RawValue);
                Tracer.TraceInfo(TraceSourceConstants.ComponentNames.ListShardMap,
                    "CreatePointMapping", "Start; ShardMap name: {0}; Point Mapping: {1}",
                    this.Name, mappingKey);

                Stopwatch stopwatch = Stopwatch.StartNew();

                PointMapping<TKey> pointMapping = _lsm.Add(new PointMapping<TKey>(this.Manager, args));

                stopwatch.Stop();

                Tracer.TraceInfo(TraceSourceConstants.ComponentNames.ListShardMap,
                    "CreatePointMapping", "Complete; ShardMap name: {0}; Point Mapping: {1}; Duration: {2}",
                    this.Name, mappingKey, stopwatch.Elapsed);

                return pointMapping;
            }
        }

        /// <summary>
        /// Removes a point mapping.
        /// </summary>
        /// <param name="mapping">Mapping being removed.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods",
            MessageId = "0")]
        public void DeleteMapping(PointMapping<TKey> mapping)
        {
            ExceptionUtils.DisallowNullArgument(mapping, "mapping");

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                string mappingKey = BitConverter.ToString(mapping.Key.RawValue);
                Tracer.TraceInfo(TraceSourceConstants.ComponentNames.ListShardMap,
                    "DeletePointMapping", "Start; ShardMap name: {0}; Point Mapping: {1}",
                    this.Name, mappingKey);

                Stopwatch stopwatch = Stopwatch.StartNew();

                _lsm.Remove(mapping);

                stopwatch.Stop();

                Tracer.TraceInfo(TraceSourceConstants.ComponentNames.ListShardMap,
                    "DeletePointMapping", "Completed; ShardMap name: {0}; Point Mapping: {1}; Duration: {2}",
                    this.Name, mappingKey, stopwatch.Elapsed);
            }
        }

        /// <summary>
        /// Looks up the key value and returns the corresponding mapping.
        /// </summary>
        /// <param name="key">Input key value.</param>
        /// <returns>Mapping that contains the key value.</returns>
        public PointMapping<TKey> GetMappingForKey(TKey key)
        {
            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceInfo(TraceSourceConstants.ComponentNames.ListShardMap,
                    "LookupPointMapping", "Start; ShardMap name: {0}; Point Mapping Key Type: {1}",
                    this.Name, typeof(TKey));

                Stopwatch stopwatch = Stopwatch.StartNew();

                PointMapping<TKey> pointMapping = _lsm.Lookup(key, false);

                stopwatch.Stop();

                Tracer.TraceInfo(TraceSourceConstants.ComponentNames.ListShardMap,
                    "LookupPointMapping", "Complete; ShardMap name: {0}; Point Mapping Key Type: {1}; Duration: {2}",
                    this.Name, typeof(TKey), stopwatch.Elapsed);

                return pointMapping;
            }
        }

        /// <summary>
        /// Tries to looks up the key value and place the corresponding mapping in <paramref name="pointMapping"/>.
        /// </summary>
        /// <param name="key">Input key value.</param>
        /// <param name="pointMapping">Mapping that contains the key value.</param>
        /// <returns><c>true</c> if mapping is found, <c>false</c> otherwise.</returns>
        public bool TryGetMappingForKey(TKey key, out PointMapping<TKey> pointMapping)
        {
            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceInfo(TraceSourceConstants.ComponentNames.ListShardMap,
                    "TryLookupPointMapping", "Start; ShardMap name: {0}; Point Mapping Key Type: {1}",
                    this.Name, typeof(TKey));

                Stopwatch stopwatch = Stopwatch.StartNew();

                bool result = _lsm.TryLookup(key, false, out pointMapping);

                stopwatch.Stop();

                Tracer.TraceInfo(TraceSourceConstants.ComponentNames.ListShardMap,
                    "TryLookupPointMapping", "Complete; ShardMap name: {0}; Point Mapping Key Type: {1}; Duration: {2}",
                    this.Name, typeof(TKey), stopwatch.Elapsed);

                return result;
            }
        }

        /// <summary>
        /// Gets all the point mappings for the shard map.
        /// </summary>
        /// <returns>Read-only collection of all point mappings on the shard map.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1006:DoNotNestGenericTypesInMemberSignatures",
            Justification = "Necessary to allow different types of keys")]
        public IReadOnlyList<PointMapping<TKey>> GetMappings()
        {
            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ListShardMap,
                    "GetPointMappings", "Start;");

                Stopwatch stopwatch = Stopwatch.StartNew();

                IReadOnlyList<PointMapping<TKey>> pointMappings = _lsm.GetMappingsForRange(null, null);

                stopwatch.Stop();

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ListShardMap,
                    "GetPointMappings", "Complete; Duration: {0}",
                    stopwatch.Elapsed);

                return pointMappings;
            }
        }

        /// <summary>
        /// Gets all the mappings that exist within given range.
        /// </summary>
        /// <param name="range">Point value, any mapping overlapping with the range will be returned.</param>
        /// <returns>Read-only collection of mappings that satisfy the given range constraint.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1006:DoNotNestGenericTypesInMemberSignatures",
            Justification = "Necessary to allow different types of keys")]
        public IReadOnlyList<PointMapping<TKey>> GetMappings(Range<TKey> range)
        {
            ExceptionUtils.DisallowNullArgument(range, "range");

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ListShardMap,
                    "GetPointMappings", "Start; Range: {0}",
                    range);

                Stopwatch stopwatch = Stopwatch.StartNew();

                IReadOnlyList<PointMapping<TKey>> pointMappings = _lsm.GetMappingsForRange(range, null);

                stopwatch.Stop();

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ListShardMap,
                    "GetPointMappings",
                    "Complete; Range: {0}; Duration: {1}",
                    range, stopwatch.Elapsed);

                return pointMappings;
            }
        }

        /// <summary>
        /// Gets all the mappings that exist for the given shard.
        /// </summary>
        /// <param name="shard">Shard for which the mappings will be returned.</param>
        /// <returns>Read-only collection of mappings that satisfy the given shard constraint.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0"),
         System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1006:DoNotNestGenericTypesInMemberSignatures",
             Justification = "Necessary to allow different types of keys")]
        public IReadOnlyList<PointMapping<TKey>> GetMappings(Shard shard)
        {
            ExceptionUtils.DisallowNullArgument(shard, "shard");

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ListShardMap,
                    "GetPointMappings", "Start; Shard: {0}",
                    shard.Location);

                Stopwatch stopwatch = Stopwatch.StartNew();

                IReadOnlyList<PointMapping<TKey>> pointMappings = _lsm.GetMappingsForRange(null, shard);

                stopwatch.Stop();

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ListShardMap,
                    "GetPointMappings", "Complete; Shard: {0}; Duration: {1}",
                    shard.Location, stopwatch.Elapsed);

                return pointMappings;
            }
        }

        /// <summary>
        /// Gets all the mappings that exist within given range and given shard.
        /// </summary>
        /// <param name="range">Point value, any mapping overlapping with the range will be returned.</param>
        /// <param name="shard">Shard for which the mappings will be returned.</param>
        /// <returns>Read-only collection of mappings that satisfy the given range and shard constraints.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "1"),
         System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1006:DoNotNestGenericTypesInMemberSignatures",
            Justification = "Necessary to allow different types of keys")]
        public IReadOnlyList<PointMapping<TKey>> GetMappings(Range<TKey> range, Shard shard)
        {
            ExceptionUtils.DisallowNullArgument(range, "range");
            ExceptionUtils.DisallowNullArgument(shard, "shard");

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ListShardMap,
                    "GetPointMappings", "Start; Shard: {0}; Range: {1}",
                    shard.Location, range);

                Stopwatch stopwatch = Stopwatch.StartNew();

                IReadOnlyList<PointMapping<TKey>> pointMappings = _lsm.GetMappingsForRange(range, shard);

                stopwatch.Stop();

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ListShardMap,
                    "GetPointMappings", "Complete; Shard: {0}; Duration: {1}",
                    shard.Location, stopwatch.Elapsed);

                return pointMappings;
            }
        }

        /// <summary>
        /// Marks the specified mapping offline.
        /// </summary>
        /// <param name="mapping">Input point mapping.</param>
        /// <returns>An offline mapping.</returns>
        public PointMapping<TKey> MarkMappingOffline(PointMapping<TKey> mapping)
        {
            ExceptionUtils.DisallowNullArgument(mapping, "mapping");

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ListShardMap,
                    "MarkMappingOffline", "Start; ");

                Stopwatch stopwatch = Stopwatch.StartNew();

                PointMapping<TKey> result = _lsm.MarkMappingOffline(mapping);

                stopwatch.Stop();

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ListShardMap,
                    "MarkMappingOffline", "Complete; Duration: {0}",
                    stopwatch.Elapsed);

                return result;
            }
        }

        /// <summary>
        /// Marks the specified mapping online.
        /// </summary>
        /// <param name="mapping">Input point mapping.</param>
        /// <returns>An online mapping.</returns>
        public PointMapping<TKey> MarkMappingOnline(PointMapping<TKey> mapping)
        {
            ExceptionUtils.DisallowNullArgument(mapping, "mapping");

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ListShardMap,
                    "MarkMappingOnline", "Start; ");

                Stopwatch stopwatch = Stopwatch.StartNew();

                PointMapping<TKey> result = _lsm.MarkMappingOnline(mapping);

                stopwatch.Stop();

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ListShardMap,
                    "MarkMappingOnline", "Complete; Duration: {0}",
                    stopwatch.Elapsed);

                return result;
            }
        }

        /// <summary>
        /// Updates a <see cref="PointMapping{TKey}"/> with the updates provided in 
        /// the <paramref name="update"/> parameter.
        /// </summary>
        /// <param name="currentMapping">Mapping being updated.</param>
        /// <param name="update">Updated properties of the mapping.</param>
        /// <returns>New instance of mapping with updated information.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods",
            MessageId = "0")]
        public PointMapping<TKey> UpdateMapping(PointMapping<TKey> currentMapping, PointMappingUpdate update)
        {
            return this.UpdateMapping(currentMapping, update, MappingLockToken.NoLock);
        }

        /// <summary>
        /// Updates a point mapping with the changes provided in 
        /// the <paramref name="update"/> parameter.
        /// </summary>
        /// <param name="currentMapping">Mapping being updated.</param>
        /// <param name="update">Updated properties of the Shard.</param>
        /// <param name="mappingLockToken">An instance of <see cref="MappingLockToken"/></param>
        /// <returns>New instance of mapping with updated information.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0"),
         System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "2")]
        public PointMapping<TKey> UpdateMapping(PointMapping<TKey> currentMapping, PointMappingUpdate update,
            MappingLockToken mappingLockToken)
        {
            ExceptionUtils.DisallowNullArgument(currentMapping, "currentMapping");
            ExceptionUtils.DisallowNullArgument(update, "update");
            ExceptionUtils.DisallowNullArgument(mappingLockToken, "mappingLockToken");

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                string mappingKey = BitConverter.ToString(currentMapping.Key.RawValue);
                Tracer.TraceInfo(TraceSourceConstants.ComponentNames.ListShardMap,
                    "UpdatePointMapping", "Start; ShardMap name: {0}; Current Point Mapping: {1}",
                    this.Name, mappingKey);

                Stopwatch stopwatch = Stopwatch.StartNew();

                PointMapping<TKey> pointMapping = _lsm.Update(currentMapping, update, mappingLockToken.LockOwnerId);

                stopwatch.Stop();

                Tracer.TraceInfo(TraceSourceConstants.ComponentNames.ListShardMap,
                    "UpdatePointMapping", "Complete; ShardMap name: {0}; Current Point Mapping: {1}; Duration: {2}",
                    this.Name, mappingKey, stopwatch.Elapsed);

                return pointMapping;
            }
        }

        /// <summary>
        /// Gets the lock owner id of the specified mapping.
        /// </summary>
        /// <param name="mapping">Input range mapping.</param>
        /// <returns>An instance of <see cref="MappingLockToken"/></returns>
        public MappingLockToken GetMappingLockOwner(PointMapping<TKey> mapping)
        {
            ExceptionUtils.DisallowNullArgument(mapping, "mapping");

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ListShardMap,
                    "LookupLockOwner", "Start");

                Stopwatch stopwatch = Stopwatch.StartNew();

                Guid storeLockOwnerId = _lsm.GetLockOwnerForMapping(mapping);

                stopwatch.Stop();

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ListShardMap,
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
        public void LockMapping(PointMapping<TKey> mapping, MappingLockToken mappingLockToken)
        {
            ExceptionUtils.DisallowNullArgument(mapping, "mapping");
            ExceptionUtils.DisallowNullArgument(mappingLockToken, "mappingLockToken");

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                // Generate a lock owner id
                Guid lockOwnerId = mappingLockToken.LockOwnerId;

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ListShardMap,
                    "Lock", "Start; LockOwnerId: {0}", lockOwnerId);

                Stopwatch stopwatch = Stopwatch.StartNew();

                _lsm.LockOrUnlockMappings(mapping, lockOwnerId, LockOwnerIdOpType.Lock);

                stopwatch.Stop();

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ListShardMap,
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
        public void UnlockMapping(PointMapping<TKey> mapping, MappingLockToken mappingLockToken)
        {
            ExceptionUtils.DisallowNullArgument(mapping, "mapping");
            ExceptionUtils.DisallowNullArgument(mappingLockToken, "mappingLockToken");

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Guid lockOwnerId = mappingLockToken.LockOwnerId;
                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ListShardMap,
                    "Unlock", "Start; LockOwnerId: {0}", lockOwnerId);

                Stopwatch stopwatch = Stopwatch.StartNew();

                _lsm.LockOrUnlockMappings(mapping, lockOwnerId, LockOwnerIdOpType.UnlockMappingForId);

                stopwatch.Stop();

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ListShardMap,
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
                    TraceSourceConstants.ComponentNames.ListShardMap,
                    "UnlockAllMappingsWithLockOwnerId", "Start; LockOwnerId: {0}", lockOwnerId);

                Stopwatch stopwatch = Stopwatch.StartNew();

                _lsm.LockOrUnlockMappings(null, lockOwnerId, LockOwnerIdOpType.UnlockAllMappingsForId);

                stopwatch.Stop();

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ListShardMap,
                    "UnlockAllMappingsWithLockOwnerId", "Complete; Duration: {0}",
                    stopwatch.Elapsed);
            }
        }

        /// <summary>
        /// Gets the mapper. This method is used by OpenConnection/Lookup of V.
        /// </summary>
        /// <typeparam name="V">Shard provider type.</typeparam>
        /// <returns>ListShardMapper for given key type.</returns>
        internal override IShardMapper<V> GetMapper<V>()
        {
            return _lsm as IShardMapper<V>;
        }

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


        /// <summary>
        /// Clones the specified list shard map.
        /// </summary>
        /// <returns>A cloned instance of the list shard map.</returns>
        public ListShardMap<TKey> Clone()
        {
            return this.CloneCore() as ListShardMap<TKey>;
        }

        #region ICloneable<ShardMap>

        /// <summary>
        /// Clones the specified shard map.
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
            return new ListShardMap<TKey>(
                this.Manager,
                this.StoreShardMap);
        }

        #endregion ICloneable<ShardMap>
    }
}
