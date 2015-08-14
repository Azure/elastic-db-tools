// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.Recovery
{
    /// <summary>
    /// Manages various recovery related tasks for a shard map manager. It helps 
    /// resolving data corruption issues between shard map information stored
    /// locally on the shards and in the global shard map manager database. 
    /// It also helps with certain 'oops' recovery scenarios where reconstruction
    /// of shard maps from database backups or database copies is necessary.
    /// </summary>
    /// <remarks>
    /// Note that some of the recovery methods can cause unrecoverable data loss when not used 
    /// properly. It is recommend to take backups or copies of all databases that participate 
    /// in recovery operations. 
    /// </remarks>
    public sealed class RecoveryManager
    {
        #region Constructors

        /// <summary>
        /// Constructs an instance of the recovery manager for given shard map manager.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager being recovered.</param>
        internal RecoveryManager(ShardMapManager shardMapManager)
        {
            Debug.Assert(shardMapManager != null);
            this.Manager = shardMapManager;
            this.Inconsistencies = new Dictionary<RecoveryToken, IDictionary<ShardRange, MappingDifference>>();
            this.StoreShardMaps = new Dictionary<RecoveryToken, Tuple<IStoreShardMap, IStoreShard>>();
            this.Locations = new Dictionary<RecoveryToken, ShardLocation>();
        }

        #endregion Constructors

        #region Private Properties

        /// <summary>
        /// Cached list of inconsistencies so user can resolve without knowing the data format.
        /// </summary>
        private IDictionary<RecoveryToken, IDictionary<ShardRange, MappingDifference>> Inconsistencies
        {
            get;
            set;
        }

        /// <summary>
        /// Cached list of IStoreShardMaps so user can reconstruct shards based on a token.
        /// </summary>
        private IDictionary<RecoveryToken, Tuple<IStoreShardMap, IStoreShard>> StoreShardMaps
        {
            get;
            set;
        }

        /// <summary>
        /// Cached list of ShardLocations so user can determine Shardlocation based on a token.
        /// </summary>
        private IDictionary<RecoveryToken, ShardLocation> Locations
        {
            get;
            set;
        }

        /// <summary>
        /// Reference to the associated shard map manager.
        /// </summary>
        private ShardMapManager Manager
        {
            get;
            set;
        }

        #endregion

        #region Public API

        /// <summary>
        /// Attaches a shard to the shard map manager. Earlier versions
        /// of mappings for the same shard map will automatically be updated 
        /// if more recent versions are found on the shard to be attached.
        /// After attaching a shard, <see cref="DetectMappingDifferences(ShardLocation, string)"/>
        /// should be called to check for any inconsistencies that warrant
        /// manual conflict resolution.
        /// </summary>
        /// <param name="location">Location of the shard being attached.</param>
        /// <remarks>
        /// Note that this method can cause unrecoverable data loss. Make sure you have taken backups or copies 
        /// of your databases and only then proceed with great care.
        /// </remarks>        
        public void AttachShard(ShardLocation location)
        {
            this.AttachShard(location, null);
        }

        /// <summary>
        /// Attaches a shard to the shard map manager. Earlier versions
        /// of mappings for the same shard map will automatically be updated 
        /// if more recent versions are found on the shard to be attached.
        /// Shard location will be upgraded to latest version of local store as part of this operation.
        /// After attaching a shard, <see cref="DetectMappingDifferences(ShardLocation, string)"/>
        /// should be called to check for any inconsistencies that warrant
        /// manual conflict resolution.
        /// </summary>
        /// <param name="location">Location of the shard being attached.</param>
        /// <param name="shardMapName">Optional string to filter on the shard map name.</param>
        /// <remarks>
        /// Note that this method can cause unrecoverable data loss. Make sure you have taken backups or copies 
        /// of your databases and only then proceed with great care.
        /// </remarks>        
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Maintainability", "CA1502:AvoidExcessiveComplexity")]
        public void AttachShard(ShardLocation location, string shardMapName)
        {
            ExceptionUtils.DisallowNullArgument(location, "location");

            IStoreResults result;

            using (IStoreOperationLocal op = this.Manager.StoreOperationFactory.CreateGetShardsLocalOperation(
                this.Manager,
                location,
                "AttachShard"))
            {
                result = op.Do();
            }

            Debug.Assert(result.Result == StoreResult.Success);

            IEnumerable<IStoreShardMap> shardMaps =
                shardMapName == null ?
                result.StoreShardMaps :
                result.StoreShardMaps.Where(s => s.Name == shardMapName);

            shardMaps.ToList<IStoreShardMap>().ForEach((sm) =>
            {
                IStoreShard shard = result.StoreShards.SingleOrDefault(s => s.ShardMapId == sm.Id);

                // construct a new store shard with correct location
                DefaultStoreShard sNew = new DefaultStoreShard(
                    shard.Id,
                    shard.Version,
                    shard.ShardMapId,
                    location,
                    shard.Status);

                using (IStoreOperation op = this.Manager.StoreOperationFactory.CreateAttachShardOperation(
                this.Manager,
                sm,
                sNew))
                {
                    op.Do();
                }
            });
        }

        /// <summary>
        /// Detaches the given shard from the shard map manager. Mappings pointing to the 
        /// shard to be deleted will automatically be removed by this method.
        /// </summary>
        /// <param name="location">Location of the shard being detached.</param>
        /// <remarks>
        /// Note that this method can cause unrecoverable data loss. Make sure you have taken backups or copies 
        /// of your databases and only then proceed with great care.
        /// </remarks>
        public void DetachShard(ShardLocation location)
        {
            this.DetachShard(location, null);
        }

        /// <summary>
        /// Detaches the given shard from the shard map manager. Mappings pointing to the 
        /// shard to be deleted will automatically be removed by this method.
        /// </summary>
        /// <param name="location">Location of the shard being detached.</param>
        /// <param name="shardMapName">Optional string to filter on shard map name.</param>
        /// <remarks>
        /// Note that this method can cause unrecoverable data loss. Make sure you have taken backups or copies 
        /// of your databases and only then proceed with great care.
        /// </remarks>
        public void DetachShard(ShardLocation location, string shardMapName)
        {
            ExceptionUtils.DisallowNullArgument(location, "location");

            using (IStoreOperationGlobal op = this.Manager.StoreOperationFactory.CreateDetachShardGlobalOperation(
                this.Manager,
                "DetachShard",
                location,
                shardMapName))
            {
                op.Do();
            }
        }

        #region Token information Getters

        /// <summary>
        /// Returns a dictionary of range-to-location key-value pairs. The location returned is an enumerator stating 
        /// whether a given range (or point) is present only in the local shard map, only in the global shard map, or both. 
        /// Ranges not contained in either shard map cannot contain differences so those ranges are not shown.
        /// </summary>
        /// <param name="token">Recovery token returned from <see cref="DetectMappingDifferences(ShardLocation, string)"/>.</param>
        /// <returns>The set of ranges and their corresponding <see cref="MappingLocation"/>.</returns>
        /// <remarks>
        /// This method assumes a previous call to <see cref="DetectMappingDifferences(ShardLocation, string)"/> that provides the recovery token parameter.
        /// The result of this method is typically used in subsequent calls to resolve inconsistencies such as 
        /// <see cref="ResolveMappingDifferences"/> or <see cref="RebuildMappingsOnShard"/>. 
        /// </remarks>
        public IDictionary<ShardRange, MappingLocation> GetMappingDifferences(RecoveryToken token)
        {
            ExceptionUtils.DisallowNullArgument(token, "token");

            if (!this.Inconsistencies.ContainsKey(token))
            {
                throw new ArgumentException(
                    StringUtils.FormatInvariant(
                        Errors._Recovery_InvalidRecoveryToken,
                        token),
                    "token");
            }

            return this.Inconsistencies[token].ToDictionary(i => i.Key, i => i.Value.Location);
        }

        /// <summary>
        /// Retrieves shard map type, name and shard location based on the token returned from <see cref="DetectMappingDifferences(ShardLocation, string)"/>.
        /// </summary>
        /// <param name="token">Recovery token returned from <see cref="DetectMappingDifferences(ShardLocation, string)"/>.</param>
        /// <param name="mapType">Outputs shard map type (Range or List).</param>
        /// <param name="shardMapName">Outputs shard map name.</param>
        /// <param name="shardLocation">Outputs shard location</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1021:AvoidOutParameters", MessageId = "1#"),
         System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1021:AvoidOutParameters", MessageId = "2#"),
         System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1021:AvoidOutParameters", MessageId = "3#")]
        public void GetShardInfo(RecoveryToken token, out ShardMapType mapType, out string shardMapName, out ShardLocation shardLocation)
        {
            ExceptionUtils.DisallowNullArgument(token, "token");

            Tuple<IStoreShardMap, IStoreShard> shardInfoLocal;

            if (!this.StoreShardMaps.TryGetValue(token, out shardInfoLocal))
            {
                throw new ArgumentException(
                    StringUtils.FormatInvariant(
                        Errors._Recovery_InvalidRecoveryToken,
                        token),
                    "token");
            }

            mapType = shardInfoLocal.Item1.MapType;
            shardMapName = shardInfoLocal.Item1.Name;

            if (!this.Locations.TryGetValue(token, out shardLocation))
            {
                throw new ArgumentException(
                    StringUtils.FormatInvariant(
                        Errors._Recovery_InvalidRecoveryToken,
                        token),
                    "token");
            }
        }

        /// <summary>
        /// Retrieves shard map type and name based on the token returned from <see cref="DetectMappingDifferences(ShardLocation, string)"/>.
        /// </summary>
        /// <param name="token">Recovery token returned from <see cref="DetectMappingDifferences(ShardLocation, string)"/>.</param>
        /// <param name="mapType">Output Shardmap type (Range or List).</param>
        /// <param name="shardMapName">Output name of shard map.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1021:AvoidOutParameters", MessageId = "1#"),
         System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1021:AvoidOutParameters", MessageId = "2#")]
        public void GetShardInfo(RecoveryToken token, out ShardMapType mapType, out string shardMapName)
        {
            ExceptionUtils.DisallowNullArgument(token, "token");

            Tuple<IStoreShardMap, IStoreShard> shardInfoLocal;

            if (!this.StoreShardMaps.TryGetValue(token, out shardInfoLocal))
            {
                throw new ArgumentException(
                    StringUtils.FormatInvariant(
                        Errors._Recovery_InvalidRecoveryToken,
                        token),
                    "token");
            }

            mapType = shardInfoLocal.Item1.MapType;
            shardMapName = shardInfoLocal.Item1.Name;
        }

        /// <summary>
        /// Returns the shard map type of the shard map processed by <see cref="DetectMappingDifferences(ShardLocation, string)"/>.
        /// </summary>
        /// <param name="token">Recovery token returned from <see cref="DetectMappingDifferences(ShardLocation, string)"/>.</param>
        /// <returns>The type of shard map (list, range, etc...) corresponding to the recovery token.</returns>
        public ShardMapType GetShardMapType(RecoveryToken token)
        {
            ExceptionUtils.DisallowNullArgument(token, "token");

            Tuple<IStoreShardMap, IStoreShard> shardInfoLocal;

            if (!this.StoreShardMaps.TryGetValue(token, out shardInfoLocal))
            {
                throw new ArgumentException(
                    StringUtils.FormatInvariant(
                        Errors._Recovery_InvalidRecoveryToken,
                        token),
                    "token");
            }

            return shardInfoLocal.Item1.MapType;
        }

        /// <summary>
        /// Returns the shard map name of the shard map processed by <see cref="DetectMappingDifferences(ShardLocation, string)"/>.
        /// </summary>
        /// <param name="token">Recovery token returned from <see cref="DetectMappingDifferences(ShardLocation, string)"/>.</param>
        /// <returns>The name of the shard map for the given recovery token.</returns>
        public string GetShardMapName(RecoveryToken token)
        {
            ExceptionUtils.DisallowNullArgument(token, "token");

            Tuple<IStoreShardMap, IStoreShard> shardInfoLocal;

            if (!this.StoreShardMaps.TryGetValue(token, out shardInfoLocal))
            {
                throw new ArgumentException(
                    StringUtils.FormatInvariant(
                        Errors._Recovery_InvalidRecoveryToken,
                        token),
                    "token");
            }

            return shardInfoLocal.Item1.Name;
        }

        /// <summary>
        /// Returns the shard location of the local shard map processed by <see cref="DetectMappingDifferences(ShardLocation, string)"/>.
        /// </summary>
        /// <param name="token">Recovery token returned from <see cref="DetectMappingDifferences(ShardLocation, string)"/></param>
        /// <returns>Location of the shard corresponding to the set of mapping differences detected in <see cref="DetectMappingDifferences(ShardLocation, string)"/>.</returns>
        public ShardLocation GetShardLocation(RecoveryToken token)
        {
            ExceptionUtils.DisallowNullArgument(token, "token");

            ShardLocation shardLocation;

            if (!this.Locations.TryGetValue(token, out shardLocation))
            {
                throw new ArgumentException(
                    StringUtils.FormatInvariant(
                        Errors._Recovery_InvalidRecoveryToken,
                        token),
                    "token");
            }

            return shardLocation;
        }

        #endregion Token Information Getters

        /// <summary>
        /// Given a collection of shard locations, reconstructs local shard maps based 
        /// on the mapping information stored in the global shard map. The specified
        /// shards need to be registered already in the global shard map. This method only 
        /// rebuilds mappings. It does not rebuild shard membership within the global shard map.
        /// </summary>
        /// <param name="shardLocations">Collection of shard locations.</param>
        /// <remarks>
        /// Note that this method can cause unrecoverable data loss. Make sure you have taken backups or copies 
        /// of your databases and only then proceed with great care.
        /// </remarks>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        public void RebuildMappingsOnShardsFromShardMapManager(IEnumerable<ShardLocation> shardLocations)
        {
            this.RebuildMappingsOnShardsFromShardMapManager(shardLocations, null);
        }

        /// <summary>
        /// Given a collection of shard locations, reconstructs local shard maps based 
        /// on the mapping information stored in the global shard map. The specified
        /// shards need to be registered already in the global shard map. This method only 
        /// rebuilds mappings. It does not rebuild shard membership within the global shard map.
        /// </summary>
        /// <param name="shardLocations">Collection of shard locations.</param>
        /// <param name="shardMapName">Optional parameter to filter by shard map name. If omitted, all shard maps will be rebuilt.</param>
        /// <remarks>
        /// Note that this method can cause unrecoverable data loss. Make sure you have taken backups or copies 
        /// of your databases and only then proceed with great care.
        /// </remarks>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        public void RebuildMappingsOnShardsFromShardMapManager(IEnumerable<ShardLocation> shardLocations, string shardMapName)
        {
            ExceptionUtils.DisallowNullArgument(shardLocations, "shardLocations");

            this.RebuildMappingsHelper(
                "RebuildMappingsOnShardsFromShardMapManager",
                shardLocations,
                MappingDifferenceResolution.KeepShardMapMapping,
                shardMapName);
        }

        /// <summary>
        /// Given a collection of shard locations, reconstructs the shard map manager based on mapping information
        /// stored in the individual shards. The specified
        /// shards need to be registered already in the global shard map. This method only 
        /// rebuilds mappings. It does not rebuild shard membership within the global shard map.
        /// If the information in the individual shard maps is or becomes inconsistent, the behavior is undefined.
        /// No cross shard locks are taken, so if any shards become inconsistent during the execution of this
        /// method, the final state of the global shard map may be corrupt.
        /// </summary>
        /// <param name="shardLocations">Collection of shard locations.</param>
        /// <remarks>
        /// Note that this method can cause unrecoverable data loss. Make sure you have taken backups or copies 
        /// of your databases and only then proceed with great care.
        /// </remarks>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        public void RebuildMappingsOnShardMapManagerFromShards(IEnumerable<ShardLocation> shardLocations)
        {
            RebuildMappingsOnShardMapManagerFromShards(shardLocations, null);
        }

        /// <summary>
        /// Given a collection of shard locations, reconstructs the shard map manager based on mapping information
        /// stored in the individual shards. The specified
        /// shards need to be registered already in the global shard map. This method only 
        /// rebuilds mappings. It does not rebuild shard membership within the global shard map.
        /// If the information in the individual shard maps is or becomes inconsistent, the behavior is undefined.
        /// No cross shard locks are taken, so if any shards become inconsistent during the execution of this
        /// method, the final state of the global shard map may be corrupt.
        /// </summary>
        /// <param name="shardLocations">Collection of shard locations.</param>
        /// <param name="shardMapName">Optional name of shard map. If omitted, will attempt to recover from all shard maps present on each shard.</param>
        /// <remarks>
        /// Note that this method can cause unrecoverable data loss. Make sure you have taken backups or copies 
        /// of your databases and only then proceed with great care.
        /// </remarks>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        public void RebuildMappingsOnShardMapManagerFromShards(IEnumerable<ShardLocation> shardLocations, string shardMapName)
        {
            ExceptionUtils.DisallowNullArgument(shardLocations, "shardLocations");

            this.RebuildMappingsHelper(
                "RebuildMappingsOnShardMapManagerFromShards",
                shardLocations,
                MappingDifferenceResolution.KeepShardMapping,
                shardMapName);
        }

        /// <summary>
        /// Rebuilds a local range shard map from a list of inconsistent shard ranges
        /// detected by <see cref="DetectMappingDifferences(ShardLocation, string)"/> and then accessed by <see cref="GetMappingDifferences"/>.
        /// The resulting local range shard map will always still be inconsistent with 
        /// the global shard map in the shard map manager database. A subsequent call to <see cref="ResolveMappingDifferences"/>
        /// is necessary to bring the system back to a healthy state.
        /// </summary>
        /// <param name="token">The recovery token from a previous call to <see cref="DetectMappingDifferences(ShardLocation, string)"/>.</param>
        /// <param name="ranges">The set of ranges to keep on the local shard when rebuilding the local shard map.</param>
        /// <remarks>
        /// Note that this method can cause unrecoverable data loss. Make sure you have taken backups or copies 
        /// of your databases and only then proceed with great care.
        ///
        /// Only shard ranges with inconsistencies can be rebuilt using this method. All ranges with no inconsistencies between
        /// the local shard and the global shard map will be kept intact on the local shard and are not affected by this call.
        /// Subsequent changes to the non-conflicting mappings can be made later using the regular interfaces in the shard map manager. 
        /// It is not necessary to use the recovery manager to change non-conflicting mappings.
        /// </remarks>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "1")]
        public void RebuildMappingsOnShard(RecoveryToken token, IEnumerable<ShardRange> ranges)
        {
            ExceptionUtils.DisallowNullArgument(token, "token");
            ExceptionUtils.DisallowNullArgument(ranges, "ranges");

            ShardLocation location = this.GetShardLocation(token);

            if (!this.Inconsistencies.ContainsKey(token))
            {
                throw new ArgumentException(
                    StringUtils.FormatInvariant(
                        Errors._Recovery_InvalidRecoveryToken,
                        token),
                    "token");
            }

            IStoreShardMap ssmLocal;

            DefaultStoreShard dss = this.GetStoreShardFromToken("RebuildMappingsOnShard", token, out ssmLocal);

            IList<IStoreMapping> mappingsToAdd = new List<IStoreMapping>();

            // Determine the ranges we want to keep based on input keeps list.
            foreach (ShardRange range in ranges)
            {
                MappingDifference difference;

                if (!this.Inconsistencies[token].TryGetValue(range, out difference))
                {
                    throw new ArgumentException(
                        StringUtils.FormatInvariant(
                            Errors._Recovery_InvalidRebuildShardSpecification,
                            range,
                            location),
                        "ranges");
                }

                // The storeMapping we will use as a template.
                IStoreMapping storeMappingTemplate = difference.Location == MappingLocation.MappingInShardMapOnly ?
                    difference.MappingForShardMap :
                    difference.MappingForShard;

                IStoreMapping storeMappingToAdd = new DefaultStoreMapping(
                    Guid.NewGuid(),
                    storeMappingTemplate.ShardMapId,
                    dss,
                    range.Low.RawValue,
                    range.High.RawValue,
                    storeMappingTemplate.Status,
                    default(Guid)
                    );

                mappingsToAdd.Add(storeMappingToAdd);
            }

            using (IStoreOperationLocal op = this.Manager.StoreOperationFactory.CreateReplaceMappingsLocalOperation(
                this.Manager,
                location,
                "RebuildMappingsOnShard",
                ssmLocal,
                dss,
                this.Inconsistencies[token].Keys,
                mappingsToAdd))
            {
                op.Do();
            }

            this.StoreShardMaps.Remove(token);
            this.Locations.Remove(token);
            this.Inconsistencies.Remove(token);
        }

        /// <summary>
        /// Enumerates differences in the mappings between the global shard map manager database and the local shard 
        /// database in the specified shard location.
        /// </summary>
        /// <param name="location">Location of shard for which to detect inconsistencies.</param>
        /// <returns>Collection of tokens to be used for further resolution tasks (see <see cref="ResolveMappingDifferences"/>).</returns>
        public IEnumerable<RecoveryToken> DetectMappingDifferences(ShardLocation location)
        {
            return this.DetectMappingDifferences(location, null);
        }

        /// <summary>
        /// Enumerates differences in the mappings between the global shard map manager database and the local shard 
        /// database in the specified shard location.
        /// </summary>
        /// <param name="location">Location of shard for which to detect inconsistencies.</param>
        /// <param name="shardMapName">Optional parameter to specify a particular shard map.</param>
        /// <returns>Collection of tokens to be used for further resolution tasks (see <see cref="ResolveMappingDifferences"/>).</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Maintainability", "CA1502:AvoidExcessiveComplexity"),
         System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Maintainability", "CA1506:AvoidExcessiveClassCoupling")]
        public IEnumerable<RecoveryToken> DetectMappingDifferences(ShardLocation location, string shardMapName)
        {
            ExceptionUtils.DisallowNullArgument(location, "location");

            IList<RecoveryToken> listOfTokens = new List<RecoveryToken>();

            IStoreResults getShardsLocalResult;

            using (IStoreOperationLocal op = this.Manager.StoreOperationFactory.CreateGetShardsLocalOperation(
                this.Manager,
                location,
                "DetectMappingDifferences"))
            {
                getShardsLocalResult = op.Do();
            }

            Debug.Assert(getShardsLocalResult.Result == StoreResult.Success);

            IEnumerable<IStoreShardMap> shardMaps =
                shardMapName == null ?
                getShardsLocalResult.StoreShardMaps :
                getShardsLocalResult.StoreShardMaps.Where(s => s.Name == shardMapName);

            IEnumerable<Tuple<IStoreShardMap, IStoreShard>> shardInfos =
                shardMaps
                    .Select(sm => new Tuple<IStoreShardMap, IStoreShard>(
                        sm,
                        getShardsLocalResult.StoreShards.SingleOrDefault(s => s.ShardMapId == sm.Id)));

            foreach (Tuple<IStoreShardMap, IStoreShard> shardInfo in shardInfos)
            {
                IStoreShardMap ssmLocal = shardInfo.Item1;
                IStoreShard ssLocal = shardInfo.Item2;

                RecoveryToken token = new RecoveryToken();

                listOfTokens.Add(token);
                this.StoreShardMaps[token] = shardInfo;
                this.Locations[token] = location;

                this.Inconsistencies[token] = new Dictionary<ShardRange, MappingDifference>();

                DefaultStoreShard dss = new DefaultStoreShard(
                    ssLocal.Id,
                    ssLocal.Version,
                    ssLocal.ShardMapId,
                    ssLocal.Location,
                    ssLocal.Status);

                // First get all local mappings.
                IStoreResults lsmMappings;

                using (IStoreOperationLocal op = this.Manager.StoreOperationFactory.CreateGetMappingsByRangeLocalOperation(
                    this.Manager,
                    location,
                    "DetectMappingDifferences",
                    ssmLocal,
                    dss,
                    null,
                    true))
                {
                    lsmMappings = op.Do();

                    if (lsmMappings.Result == StoreResult.ShardMapDoesNotExist)
                    {
                        // The shard needs to be re-attached. We are ignoring these errors in 
                        // DetectMappingDifferences, since corruption is more profound than 
                        // just inconsistent mappings.
                        // Alternatively, this shard belongs to a different shard map manager.
                        // Either way, we can't do anything about it here.
                        continue;
                    }
                }

                // Next build up a set of relevant global mappings.
                // This is the union of those mappings that are associated with this local shard
                // and those mappings which intersect with mappings found in the local shard.
                // We will partition these mappings based on ranges.
                IDictionary<ShardRange, IStoreMapping> relevantGsmMappings = new Dictionary<ShardRange, IStoreMapping>();

                IStoreResults gsmMappingsByMap;

                using (IStoreOperationGlobal op = this.Manager.StoreOperationFactory.CreateGetMappingsByRangeGlobalOperation(
                    this.Manager,
                    "DetectMappingDifferences",
                    ssmLocal,
                    dss,
                    null,
                    ShardManagementErrorCategory.Recovery,
                    false,
                    true/* ignore failures */))
                {
                    gsmMappingsByMap = op.Do();
                }

                if (gsmMappingsByMap.Result == StoreResult.ShardMapDoesNotExist)
                {
                    // The shard map is not properly attached to this GSM.
                    // This is beyond what we can handle resolving mappings.
                    continue;
                }

                foreach (IStoreMapping gsmMapping in gsmMappingsByMap.StoreMappings)
                {
                    ShardKey min = ShardKey.FromRawValue(ssmLocal.KeyType, gsmMapping.MinValue);

                    ShardKey max = null;

                    switch (ssmLocal.MapType)
                    {
                        case ShardMapType.Range:
                            max = ShardKey.FromRawValue(ssmLocal.KeyType, gsmMapping.MaxValue);
                            break;

                        default:
                            Debug.Assert(ssmLocal.MapType == ShardMapType.List);
                            max = ShardKey.FromRawValue(ssmLocal.KeyType, gsmMapping.MinValue).GetNextKey();
                            break;
                    }

                    ShardRange range = new ShardRange(min, max);

                    relevantGsmMappings[range] = gsmMapping;
                }

                // Next, for each of the mappings in lsmMappings, we need to augment 
                // the gsmMappingsByMap by intersecting ranges.
                foreach (IStoreMapping lsmMapping in lsmMappings.StoreMappings)
                {
                    ShardKey min = ShardKey.FromRawValue(ssmLocal.KeyType, lsmMapping.MinValue);

                    IStoreResults gsmMappingsByRange;

                    if (ssmLocal.MapType == ShardMapType.Range)
                    {
                        ShardKey max = ShardKey.FromRawValue(ssmLocal.KeyType, lsmMapping.MaxValue);

                        ShardRange range = new ShardRange(min, max);

                        using (IStoreOperationGlobal op = this.Manager.StoreOperationFactory.CreateGetMappingsByRangeGlobalOperation(
                            this.Manager,
                            "DetectMappingDifferences",
                            ssmLocal,
                            null,
                            range,
                            ShardManagementErrorCategory.Recovery,
                            false,
                            true/* ignore failures */))
                        {
                            gsmMappingsByRange = op.Do();
                        }

                        if (gsmMappingsByRange.Result == StoreResult.ShardMapDoesNotExist)
                        {
                            // The shard was not properly attached. 
                            // This is more than we can deal with in mapping resolution.
                            continue;
                        }
                    }
                    else
                    {
                        Debug.Assert(ssmLocal.MapType == ShardMapType.List);
                        using (IStoreOperationGlobal op = this.Manager.StoreOperationFactory.CreateFindMappingByKeyGlobalOperation(
                            this.Manager,
                            "DetectMappingDifferences",
                            ssmLocal,
                            min,
                            CacheStoreMappingUpdatePolicy.OverwriteExisting,
                            ShardManagementErrorCategory.Recovery,
                            false,
                            true/* ignore failures */))
                        {
                            gsmMappingsByRange = op.Do();

                            if (gsmMappingsByRange.Result == StoreResult.MappingNotFoundForKey ||
                                gsmMappingsByRange.Result == StoreResult.ShardMapDoesNotExist)
                            {
                                // * No intersections being found is fine. Skip to the next mapping.
                                // * The shard was not properly attached. 
                                // This is more than we can deal with in mapping resolution.
                                continue;
                            }
                        }
                    }

                    foreach (IStoreMapping gsmMapping in gsmMappingsByRange.StoreMappings)
                    {
                        ShardKey retrievedMin = ShardKey.FromRawValue(ssmLocal.KeyType, gsmMapping.MinValue);

                        ShardRange retrievedRange = null;

                        switch (ssmLocal.MapType)
                        {
                            case ShardMapType.Range:
                                ShardKey retrievedMax = ShardKey.FromRawValue(ssmLocal.KeyType, gsmMapping.MaxValue);
                                retrievedRange = new ShardRange(retrievedMin, retrievedMax);
                                break;

                            default:
                                Debug.Assert(ssmLocal.MapType == ShardMapType.List);
                                retrievedMax = ShardKey.FromRawValue(ssmLocal.KeyType, gsmMapping.MinValue).GetNextKey();
                                retrievedRange = new ShardRange(retrievedMin, retrievedMax);
                                break;
                        }

                        relevantGsmMappings[retrievedRange] = gsmMapping;
                    }
                }

                IList<MappingComparisonResult> comparisonResults = null;

                switch (ssmLocal.MapType)
                {
                    case ShardMapType.Range:
                        comparisonResults = MappingComparisonUtils.CompareRangeMappings(
                            ssmLocal,
                            relevantGsmMappings.Values,
                            lsmMappings.StoreMappings);
                        break;

                    default:
                        Debug.Assert(ssmLocal.MapType == ShardMapType.List);
                        comparisonResults = MappingComparisonUtils.ComparePointMappings(
                            ssmLocal,
                            relevantGsmMappings.Values,
                            lsmMappings.StoreMappings);
                        break;
                }

                // Now we have 2 sets of mappings. Each submapping generated from this function is
                //  1.) in the GSM only: report.
                //  2.) in the LSM only: report.
                //  3.) in both but with different version number: report.
                //  4.) in both with the same version number: skip.
                foreach (MappingComparisonResult r in comparisonResults)
                {
                    switch (r.MappingLocation)
                    {
                        case MappingLocation.MappingInShardMapOnly:
                        case MappingLocation.MappingInShardOnly:
                            break;
                        default:
                            Debug.Assert(r.MappingLocation == MappingLocation.MappingInShardMapAndShard);

                            if (r.ShardMapManagerMapping.Id == r.ShardMapping.Id)
                            {
                                // No conflict found, skip to the next range.
                                continue;
                            }
                            break;
                    }

                    // Store the inconsistency for later reporting.
                    this.Inconsistencies[token][r.Range] = new MappingDifference(
                        type: MappingDifferenceType.Range,
                        location: r.MappingLocation,
                        shardMap: r.ShardMap,
                        mappingForShard: r.ShardMapping,
                        mappingForShardMap: r.ShardMapManagerMapping);
                }
            }

            return listOfTokens;
        }

        /// <summary>
        /// Selects one of the shard maps (either local or global) as a source of truth and brings 
        /// mappings on both shard maps in sync.
        /// </summary>
        /// <param name="token">Recovery token returned from <see cref="DetectMappingDifferences(ShardLocation, string)"/>.</param>
        /// <param name="resolution">The resolution strategy to be used for resolution.</param>
        /// <remarks>
        /// Note that this method can cause unrecoverable data loss. Make sure you have taken backups or copies 
        /// of your databases and only then proceed with great care.
        /// </remarks>
        public void ResolveMappingDifferences(
            RecoveryToken token,
            MappingDifferenceResolution resolution)
        {
            switch (resolution)
            {
                case MappingDifferenceResolution.KeepShardMapMapping:
                    this.RestoreShardFromShardmap(token);
                    break;
                case MappingDifferenceResolution.KeepShardMapping:
                    this.RestoreShardMapFromShard(token);
                    break;
                case MappingDifferenceResolution.Ignore:
                    break;
                default:
                    Debug.Fail("Unexpected value for MappingDifferenceResolution.");
                    return;
            }

            this.StoreShardMaps.Remove(token);
            this.Locations.Remove(token);
            this.Inconsistencies.Remove(token);
        }

        #endregion

        #region Private Helper Methods

        /// <summary>
        /// Given a collection of shard locations, reconstructs the shard map manager based on information
        /// stored in the individual shards.
        /// If the information in the individual shard maps is or becomes inconsistent, behavior is undefined.
        /// No cross shard locks are taken, so if any shards become inconsistent during the execution of this
        /// method, the final state of the global shard map may be corrupt.
        /// </summary>
        /// <param name="operationName">Operation name.</param>
        /// <param name="shardLocations">Collection of shard locations.</param>
        /// <param name="resolutionStrategy">Strategy for resolving the mapping differences.</param>
        /// <param name="shardMapName">Optional name of shard map. If omitted, will attempt to recover from all shard maps present on each shard.</param>
        private void RebuildMappingsHelper(
            string operationName,
            IEnumerable<ShardLocation> shardLocations,
            MappingDifferenceResolution resolutionStrategy,
            string shardMapName = null)
        {
            Debug.Assert(shardLocations != null);

            IList<RecoveryToken> idsToProcess = new List<RecoveryToken>();

            // Collect the shard map-shard pairings to recover. Give each of these pairings a token.
            foreach (ShardLocation shardLocation in shardLocations)
            {
                IStoreResults getShardsLocalResult;

                using (IStoreOperationLocal op = this.Manager.StoreOperationFactory.CreateGetShardsLocalOperation(
                    this.Manager,
                    shardLocation,
                    operationName))
                {
                    getShardsLocalResult = op.Do();
                }

                Debug.Assert(getShardsLocalResult.Result == StoreResult.Success);

                IEnumerable<IStoreShardMap> shardMaps =
                    shardMapName == null ?
                    getShardsLocalResult.StoreShardMaps :
                    getShardsLocalResult.StoreShardMaps.Where(s => s.Name == shardMapName);

                IEnumerable<Tuple<IStoreShardMap, IStoreShard>> shardInfos =
                    shardMaps
                        .Select(sm => new Tuple<IStoreShardMap, IStoreShard>(
                            sm,
                            getShardsLocalResult.StoreShards.SingleOrDefault(s => s.ShardMapId == sm.Id)));

                foreach (Tuple<IStoreShardMap, IStoreShard> shardInfo in shardInfos)
                {
                    RecoveryToken token = new RecoveryToken();

                    idsToProcess.Add(token);

                    this.StoreShardMaps[token] = shardInfo;
                    this.Locations[token] = shardLocation;
                }
            }

            // Recover from the shard map-shard pairing corresponding to the collected token.
            foreach (RecoveryToken token in idsToProcess)
            {
                this.ResolveMappingDifferences(token, resolutionStrategy);

                this.StoreShardMaps.Remove(token);
                this.Locations.Remove(token);
            }
        }

        /// <summary>
        /// Attaches a shard to the shard map manager.
        /// </summary>
        /// <param name="token">Token from DetectMappingDifferences.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Maintainability", "CA1502:AvoidExcessiveComplexity")]
        private void RestoreShardMapFromShard(RecoveryToken token)
        {
            IStoreShardMap ssmLocal;

            DefaultStoreShard dss = this.GetStoreShardFromToken("ResolveMappingDifferences", token, out ssmLocal);

            IStoreResults lsmMappingsToRemove;

            using (IStoreOperationLocal op = this.Manager.StoreOperationFactory.CreateGetMappingsByRangeLocalOperation(
                this.Manager,
                dss.Location,
                "ResolveMappingDifferences",
                ssmLocal,
                dss,
                null,
                false))
            {
                lsmMappingsToRemove = op.Do();
            }

            IEnumerable<IStoreMapping> gsmMappingsToAdd = lsmMappingsToRemove.StoreMappings.Select(
                mapping => new DefaultStoreMapping(
                            mapping.Id,
                            mapping.ShardMapId,
                            dss,
                            mapping.MinValue,
                            mapping.MaxValue,
                            mapping.Status,
                            default(Guid)));

            using (IStoreOperationGlobal op = this.Manager.StoreOperationFactory.CreateReplaceMappingsGlobalOperation(
                this.Manager,
                "ResolveMappingDifferences",
                ssmLocal,
                dss,
                lsmMappingsToRemove.StoreMappings,
                gsmMappingsToAdd))
            {
                op.Do();
            }
        }

        /// <summary>
        /// Helper function to bring a Shard into a consistent state with a ShardMap.
        /// </summary>
        /// <param name="token">Token from DetectMappingDifferences</param>
        private void RestoreShardFromShardmap(RecoveryToken token)
        {
            IStoreShardMap ssmLocal;

            DefaultStoreShard dss = this.GetStoreShardFromToken("ResolveMappingDifferences", token, out ssmLocal);

            IStoreResults gsmMappings;

            using (IStoreOperationGlobal op = this.Manager.StoreOperationFactory.CreateGetMappingsByRangeGlobalOperation(
                this.Manager,
                "ResolveMappingDifferences",
                ssmLocal,
                dss,
                null,
                ShardManagementErrorCategory.Recovery,
                false,
                false))
            {
                gsmMappings = op.Do();
            }

            using (IStoreOperationLocal op = this.Manager.StoreOperationFactory.CreateReplaceMappingsLocalOperation(
                this.Manager,
                dss.Location,
                "ResolveMappingDifferences",
                ssmLocal,
                dss,
                null,
                gsmMappings.StoreMappings))
            {
                op.Do();
            }
        }

        /// <summary>
        /// Helper function to obtain a store shard object from given recovery token.
        /// </summary>
        /// <param name="operationName">Operation name.</param>
        /// <param name="token">Token from DetectMappingDifferences.</param>
        /// <param name="ssmLocal">Reference to store shard map corresponding to the token.</param>
        /// <returns>Store shard object corresponding to given token, or null if shard map is default shard map.</returns>
        private DefaultStoreShard GetStoreShardFromToken(string operationName, RecoveryToken token, out IStoreShardMap ssmLocal)
        {
            Tuple<IStoreShardMap, IStoreShard> shardInfoLocal;

            if (!this.StoreShardMaps.TryGetValue(token, out shardInfoLocal))
            {
                throw new ArgumentException(
                    StringUtils.FormatInvariant(
                        Errors._Recovery_InvalidRecoveryToken,
                        token),
                    "token");
            }

            ssmLocal = shardInfoLocal.Item1;

            IStoreShard ssLocal = shardInfoLocal.Item2;

            ShardLocation location = this.GetShardLocation(token);

            using (IStoreOperationLocal op = this.Manager.StoreOperationFactory.CreateCheckShardLocalOperation(
                operationName,
                this.Manager,
                location))
            {
                op.Do();
            }

            return new DefaultStoreShard(
                ssLocal.Id,
                ssLocal.Version,
                ssLocal.ShardMapId,
                ssLocal.Location,
                ssLocal.Status);
        }

        #endregion
    }
}
