// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Collections.Generic;
using System.Collections;
using System.Linq;
using System.Diagnostics;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.Recovery
{
    /// <summary>
    /// Contains utility methods for performing comparisons among collections of 
    /// mappings of either list or range shard maps.
    /// </summary>
    internal static class MappingComparisonUtils
    {
        #region Static internal methods

        /// <summary>
        /// Helper function that produces a list of MappingComparisonResults from union of range boundaries in the gsmMappings and lsmMappings.
        /// </summary>
        /// <param name="ssm">StoreShardmap to be referenced in produced MappingComparisonResults</param>
        /// <param name="gsmMappings">List of mappings from the GSM.</param>
        /// <param name="lsmMappings">List of mappings from the LSM.</param>
        /// <returns>List of mappingcomparisonresults: one for each range arising from the union of boundaries in gsmMappings and lsmMappings.</returns>
        internal static List<MappingComparisonResult> CompareRangeMappings(
            IStoreShardMap ssm,
            IEnumerable<IStoreMapping> gsmMappings,
            IEnumerable<IStoreMapping> lsmMappings)
        {
            // Detect if these are point mappings and call the ComparePointMappings function below.

            List<MappingComparisonResult> result = new List<MappingComparisonResult>();

            // Identify the type of keys.
            ShardKeyType keyType = ssm.KeyType;

            using (IEnumerator<IStoreMapping> gsmMappingIterator = gsmMappings.GetEnumerator())
            using (IEnumerator<IStoreMapping> lsmMappingIterator = lsmMappings.GetEnumerator())
            {
                IStoreMapping gsmMappingCurrent;
                ShardRange gsmRangeCurrent;
                ShardKey gsmMinKeyCurrent;

                IStoreMapping lsmMappingCurrent;
                ShardRange lsmRangeCurrent;
                ShardKey lsmMinKeyCurrent;

                MoveToNextMapping(
                    gsmMappingIterator,
                    keyType,
                    out gsmMappingCurrent,
                    out gsmRangeCurrent,
                    out gsmMinKeyCurrent);

                MoveToNextMapping(
                    lsmMappingIterator,
                    keyType,
                    out lsmMappingCurrent,
                    out lsmRangeCurrent,
                    out lsmMinKeyCurrent);

                while (gsmMinKeyCurrent != null)
                {
                    // If there is something in LSM, consider the following 6 possibilities.
                    if (lsmMinKeyCurrent != null)
                    {
                        if (lsmMinKeyCurrent <= gsmMinKeyCurrent)
                        {
                            // Case 1. LSM starts to the left of or exactly at GSM.

                            if (lsmRangeCurrent.High <= gsmMinKeyCurrent)
                            {
                                // Case 1.1: LSM is entirely to the left of Left.

                                // Add the LSM only entry.
                                result.Add(
                                    new MappingComparisonResult(
                                        ssm,
                                        new ShardRange(lsmMinKeyCurrent, lsmRangeCurrent.High),
                                        MappingLocation.MappingInShardOnly,
                                        null,
                                        lsmMappingCurrent));

                                // LSM range exhausted for current iteration.
                                MoveToNextMapping(
                                    lsmMappingIterator,
                                    keyType,
                                    out lsmMappingCurrent,
                                    out lsmRangeCurrent,
                                    out lsmMinKeyCurrent);
                            }
                            else
                                if (lsmRangeCurrent.High <= gsmRangeCurrent.High)
                            {
                                // Case 1.2: LSM overlaps with GSM, with extra values to the left and finishing before GSM.
                                if (lsmMinKeyCurrent != gsmMinKeyCurrent)
                                {
                                    // Add the LSM only entry.
                                    result.Add(
                                        new MappingComparisonResult(
                                            ssm,
                                            new ShardRange(lsmMinKeyCurrent, gsmMinKeyCurrent),
                                            MappingLocation.MappingInShardOnly,
                                            null,
                                            lsmMappingCurrent));
                                }

                                // Add common entry.
                                result.Add(
                                    new MappingComparisonResult(
                                        ssm,
                                        new ShardRange(gsmMinKeyCurrent, lsmRangeCurrent.High),
                                        MappingLocation.MappingInShardMapAndShard,
                                        gsmMappingCurrent,
                                        lsmMappingCurrent));

                                gsmMinKeyCurrent = lsmRangeCurrent.High;

                                // LSM range exhausted for current iteration.
                                MoveToNextMapping(
                                    lsmMappingIterator,
                                    keyType,
                                    out lsmMappingCurrent,
                                    out lsmRangeCurrent,
                                    out lsmMinKeyCurrent);

                                // Detect if GSM range exhausted for current iteration.
                                if (gsmMinKeyCurrent == gsmRangeCurrent.High)
                                {
                                    MoveToNextMapping(
                                        gsmMappingIterator,
                                        keyType,
                                        out gsmMappingCurrent,
                                        out gsmRangeCurrent,
                                        out gsmMinKeyCurrent);
                                }
                            }
                            else // lsmRangeCurrent.High > gsmRangeCurrent.High
                            {
                                // Case 1.3: LSM encompasses GSM.

                                // Add the LSM only entry.
                                if (lsmMinKeyCurrent != gsmMinKeyCurrent)
                                {
                                    result.Add(
                                        new MappingComparisonResult(
                                            ssm,
                                            new ShardRange(lsmMinKeyCurrent, gsmMinKeyCurrent),
                                            MappingLocation.MappingInShardOnly,
                                            null,
                                            lsmMappingCurrent));
                                }

                                // Add common entry.
                                result.Add(
                                    new MappingComparisonResult(
                                        ssm,
                                        new ShardRange(gsmMinKeyCurrent, gsmRangeCurrent.High),
                                        MappingLocation.MappingInShardMapAndShard,
                                        gsmMappingCurrent,
                                        lsmMappingCurrent));

                                lsmMinKeyCurrent = gsmRangeCurrent.High;

                                // GSM range exhausted for current iteration.
                                MoveToNextMapping(
                                    gsmMappingIterator,
                                    keyType,
                                    out gsmMappingCurrent,
                                    out gsmRangeCurrent,
                                    out gsmMinKeyCurrent);
                            }
                        }
                        else
                        {
                            // Case 2. LSM starts to the right of GSM.

                            if (lsmRangeCurrent.High <= gsmRangeCurrent.High)
                            {
                                // Case 2.1: GSM encompasses LSM.
                                Debug.Assert(lsmMinKeyCurrent != gsmMinKeyCurrent, "Must have been handled by Case 1.3");

                                // Add the GSM only entry.
                                result.Add(
                                    new MappingComparisonResult(
                                        ssm,
                                        new ShardRange(gsmMinKeyCurrent, lsmMinKeyCurrent),
                                        MappingLocation.MappingInShardMapOnly,
                                        gsmMappingCurrent,
                                        null));

                                gsmMinKeyCurrent = lsmRangeCurrent.Low;

                                // Add common entry.
                                result.Add(
                                    new MappingComparisonResult(
                                        ssm,
                                        new ShardRange(gsmMinKeyCurrent, gsmRangeCurrent.High),
                                        MappingLocation.MappingInShardMapAndShard,
                                        gsmMappingCurrent,
                                        lsmMappingCurrent));

                                gsmMinKeyCurrent = lsmRangeCurrent.High;

                                // LSM range exhausted for current iteration.
                                MoveToNextMapping(
                                    lsmMappingIterator,
                                    keyType,
                                    out lsmMappingCurrent,
                                    out lsmRangeCurrent,
                                    out lsmMinKeyCurrent);

                                // Detect if GSM range exhausted for current iteration.
                                if (gsmMinKeyCurrent == gsmRangeCurrent.High)
                                {
                                    MoveToNextMapping(
                                        gsmMappingIterator,
                                        keyType,
                                        out gsmMappingCurrent,
                                        out gsmRangeCurrent,
                                        out gsmMinKeyCurrent);
                                }
                            }
                            else
                                if (lsmRangeCurrent.Low < gsmRangeCurrent.High)
                            {
                                // Case 2.2: LSM overlaps with GSM, with extra values to the right and finishing after GSM.
                                Debug.Assert(lsmMinKeyCurrent != gsmMinKeyCurrent, "Must have been handled by Case 1.3");

                                // Add the GSM only entry.
                                result.Add(
                                    new MappingComparisonResult(
                                        ssm,
                                        new ShardRange(gsmMinKeyCurrent, lsmMinKeyCurrent),
                                        MappingLocation.MappingInShardMapOnly,
                                        gsmMappingCurrent,
                                        null));

                                // Add common entry.
                                result.Add(
                                    new MappingComparisonResult(
                                        ssm,
                                        new ShardRange(lsmMinKeyCurrent, gsmRangeCurrent.High),
                                        MappingLocation.MappingInShardMapAndShard,
                                        gsmMappingCurrent,
                                        lsmMappingCurrent));

                                lsmMinKeyCurrent = gsmRangeCurrent.High;

                                // GSM range exhausted for current iteration.
                                MoveToNextMapping(
                                    gsmMappingIterator,
                                    keyType,
                                    out gsmMappingCurrent,
                                    out gsmRangeCurrent,
                                    out gsmMinKeyCurrent);
                            }
                            else // lsmRangeCurrent.Low >= gsmRangeCurrent.High
                            {
                                // Case 2.3: LSM is entirely to the right of GSM.

                                // Add the GSM only entry.
                                result.Add(
                                    new MappingComparisonResult(
                                        ssm,
                                        new ShardRange(gsmMinKeyCurrent, gsmRangeCurrent.High),
                                        MappingLocation.MappingInShardMapOnly,
                                        gsmMappingCurrent,
                                        null));

                                // GSM range exhausted for current iteration.
                                MoveToNextMapping(
                                    gsmMappingIterator,
                                    keyType,
                                    out gsmMappingCurrent,
                                    out gsmRangeCurrent,
                                    out gsmMinKeyCurrent);
                            }
                        }
                    }
                    else
                    {
                        // Nothing in LSM, we just keep going over the GSM entries.

                        // Add the GSM only entry.
                        result.Add(
                            new MappingComparisonResult(
                                ssm,
                                new ShardRange(gsmMinKeyCurrent, gsmRangeCurrent.High),
                                MappingLocation.MappingInShardMapOnly,
                                gsmMappingCurrent,
                                null));

                        // GSM range exhausted for current iteration.
                        MoveToNextMapping(
                            gsmMappingIterator,
                            keyType,
                            out gsmMappingCurrent,
                            out gsmRangeCurrent,
                            out gsmMinKeyCurrent);
                    }
                }

                // Go over the partial remainder of LSM entry if any.
                if (lsmRangeCurrent != null && lsmMinKeyCurrent > lsmRangeCurrent.Low)
                {
                    // Add the LSM only entry.
                    result.Add(
                        new MappingComparisonResult(
                            ssm,
                            new ShardRange(lsmMinKeyCurrent, lsmRangeCurrent.High),
                            MappingLocation.MappingInShardOnly,
                            null,
                            lsmMappingCurrent));

                    // LSM range exhausted for current iteration.
                    MoveToNextMapping(
                        lsmMappingIterator,
                        keyType,
                        out lsmMappingCurrent,
                        out lsmRangeCurrent,
                        out lsmMinKeyCurrent);
                }

                // Go over remaining Right entries if any which have no matches on Left.
                while (lsmMappingCurrent != null)
                {
                    // Add the LSM only entry.
                    result.Add(
                        new MappingComparisonResult(
                            ssm,
                            lsmRangeCurrent,
                            MappingLocation.MappingInShardOnly,
                            null,
                            lsmMappingCurrent));

                    // LSM range exhausted for current iteration.
                    MoveToNextMapping(
                        lsmMappingIterator,
                        keyType,
                        out lsmMappingCurrent,
                        out lsmRangeCurrent,
                        out lsmMinKeyCurrent);
                }
            }

            return result;
        }

        /// <summary>
        /// Helper function that produces a list of MappingComparisonResults from union of points in the gsmMappings and lsmMappings.
        /// </summary>
        /// <param name="ssm">StoreShardmap to be referenced in produced MappingComparisonResults</param>
        /// <param name="gsmMappings">List of mappings from the GSM.</param>
        /// <param name="lsmMappings">List of mappings from the LSM.</param>
        /// <returns>List of mappingcomparisonresults: one for each range arising from the union of boundaries in gsmMappings and lsmMappings.</returns>
        internal static List<MappingComparisonResult> ComparePointMappings(
            IStoreShardMap ssm,
            IEnumerable<IStoreMapping> gsmMappings,
            IEnumerable<IStoreMapping> lsmMappings)
        {
            ShardKeyType keyType = ssm.KeyType;
            // Get a Linq-able set of points from the input mappings.
            //
            IDictionary<ShardKey, IStoreMapping> gsmPoints =
                gsmMappings.ToDictionary(gsmMapping => ShardKey.FromRawValue(keyType, gsmMapping.MinValue));
            IDictionary<ShardKey, IStoreMapping> lsmPoints =
                lsmMappings.ToDictionary(lsmMapping => ShardKey.FromRawValue(keyType, lsmMapping.MinValue));

            // Construct the output list. This is the concatenation of 3 mappings:
            //  1.) Intersection (the key exists in both the shardmap and the shard.)
            //  2.) Shard only (the key exists only in the shard.)
            //  3.) Shardmap only (the key exists only in the shardmap.)
            //
            List<MappingComparisonResult> results = (new List<MappingComparisonResult>()).Concat(
                // Intersection.
                lsmPoints.Keys.Intersect(gsmPoints.Keys).Select(
                    commonPoint =>
                        new MappingComparisonResult(
                            ssm,
                            new ShardRange(commonPoint, commonPoint.GetNextKey()),
                            MappingLocation.MappingInShardMapAndShard,
                            gsmPoints[commonPoint],
                            lsmPoints[commonPoint]))
                            ).Concat(
                // Lsm only.
                lsmPoints.Keys.Except(gsmPoints.Keys).Select(
                    lsmOnlyPoint =>
                        new MappingComparisonResult(
                            ssm,
                            new ShardRange(lsmOnlyPoint, lsmOnlyPoint.GetNextKey()),
                            MappingLocation.MappingInShardOnly,
                            null,
                            lsmPoints[lsmOnlyPoint]))
                            ).Concat(
               // Gsm only.
               gsmPoints.Keys.Except(lsmPoints.Keys).Select(
                    gsmOnlyPoint =>
                        new MappingComparisonResult(
                            ssm,
                            new ShardRange(gsmOnlyPoint, gsmOnlyPoint.GetNextKey()),
                            MappingLocation.MappingInShardMapOnly,
                            gsmPoints[gsmOnlyPoint],
                            null))).ToList();

            return results;
        }

        #endregion

        #region Private Helper Functions

        /// <summary>
        /// Helper function to advance mapping iterators.
        /// </summary>
        /// <param name="iterator">The iterator to advance.</param>
        /// <param name="keyType">The data type of the map key.</param>
        /// <param name="nextMapping">Output value that will contain next mapping.</param>
        /// <param name="nextRange">Output value that will contain next range.</param>
        /// <param name="nextMinKey">Output value that will contain next min key.</param>
        private static void MoveToNextMapping(
            IEnumerator<IStoreMapping> iterator,
            ShardKeyType keyType,
            out IStoreMapping nextMapping,
            out ShardRange nextRange,
            out ShardKey nextMinKey)
        {
            nextMapping = iterator.MoveNext() ? iterator.Current : null;
            nextRange = nextMapping != null ? new ShardRange(
                                ShardKey.FromRawValue(keyType, nextMapping.MinValue),
                                ShardKey.FromRawValue(keyType, nextMapping.MaxValue)) : null;
            nextMinKey = nextRange != null ? nextRange.Low : null;
        }

        #endregion
    }
}
