// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests
{
    internal abstract class TestShardMapVerifier
    {
        public abstract IMappingInfoProvider GetMappingForKey<TKey>(TestShardMap<TKey> testShardMap, TKey value);
        public abstract IMappingInfoProvider GetMappingForKey<TKey>(TestShardMap<TKey> testShardMap, TKey value, LookupOptions lookupOptions);
        public abstract void GetMappingForKey_NotExists<TKey>(TestShardMap<TKey> testShardMap, TKey value);
        public abstract void GetMappingForKey_NotExists<TKey>(TestShardMap<TKey> testShardMap, TKey value, LookupOptions lookupOptions);

        protected void VerifyMapping(IMappingInfoProvider mapping)
        {
            Assert.IsNotNull(mapping);
        }

        protected void AssertThrowsShardManagementException(Action a, ShardManagementErrorCode expectedCode)
        {
            ShardManagementException smme = Assert.ThrowsException<ShardManagementException>(a);
            Assert.AreEqual(ShardManagementErrorCode.MappingNotFoundForKey, smme.ErrorCode);
        }
    }

    /// <summary>
    /// Verifies methods on shard map using "regular" (exception-throwing) version of test method.
    /// </summary>
    internal class ExceptionBasedTestShardMapVerifier : TestShardMapVerifier
    {
        public override IMappingInfoProvider GetMappingForKey<TKey>(TestShardMap<TKey> testShardMap, TKey value)
        {
            var mapping = testShardMap.GetMappingForKey(value);
            VerifyMapping(mapping);
            return mapping;
        }

        public override IMappingInfoProvider GetMappingForKey<TKey>(TestShardMap<TKey> testShardMap, TKey value, LookupOptions lookupOptions)
        {
            var mapping = testShardMap.GetMappingForKey(value, lookupOptions);
            VerifyMapping(mapping);
            return mapping;
        }

        public override void GetMappingForKey_NotExists<TKey>(TestShardMap<TKey> testShardMap, TKey value)
        {
            AssertThrowsShardManagementException(
                () => testShardMap.GetMappingForKey(value),
                ShardManagementErrorCode.MappingNotFoundForKey);
        }

        public override void GetMappingForKey_NotExists<TKey>(TestShardMap<TKey> testShardMap, TKey value, LookupOptions lookupOptions)
        {
            AssertThrowsShardManagementException(
                () => testShardMap.GetMappingForKey(value, lookupOptions),
                ShardManagementErrorCode.MappingNotFoundForKey);
        }
    }

    /// <summary>
    /// Verifies methods on shard map using "try" version of test method.
    /// </summary>
    internal class TryBasedTestShardMapVerifier : TestShardMapVerifier
    {
        public override IMappingInfoProvider GetMappingForKey<TKey>(TestShardMap<TKey> testShardMap, TKey value)
        {
            bool ret = testShardMap.TryGetMappingForKey(value, out IMappingInfoProvider mapping);
            Assert.IsTrue(ret);
            VerifyMapping(mapping);
            return mapping;
        }

        public override IMappingInfoProvider GetMappingForKey<TKey>(TestShardMap<TKey> testShardMap, TKey value, LookupOptions lookupOptions)
        {
            bool ret = testShardMap.TryGetMappingForKey(value, lookupOptions, out IMappingInfoProvider mapping);
            Assert.IsTrue(ret);
            VerifyMapping(mapping);
            return mapping;
        }

        public override void GetMappingForKey_NotExists<TKey>(TestShardMap<TKey> testShardMap, TKey value)
        {
            bool ret = testShardMap.TryGetMappingForKey(value, out IMappingInfoProvider mapping);
            Assert.IsFalse(ret);
            Assert.IsNull(mapping);
        }

        public override void GetMappingForKey_NotExists<TKey>(TestShardMap<TKey> testShardMap, TKey value, LookupOptions lookupOptions)
        {
            bool ret = testShardMap.TryGetMappingForKey(value, lookupOptions, out IMappingInfoProvider mapping);
            Assert.IsFalse(ret);
            Assert.IsNull(mapping);
        }
    }
}
