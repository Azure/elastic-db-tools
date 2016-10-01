// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Diagnostics;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests.Stubs
{
    /// <summary>
    /// Stub type of Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.ICacheStoreMapping
    /// </summary>
    [DebuggerDisplay("Stub of ICacheStoreMapping")]
    [DebuggerNonUserCode]
    internal class StubICacheStoreMapping : ICacheStoreMapping
    {
        /// <summary>
        /// Sets the stub of ICacheStoreMapping.get_CreationTime()
        /// </summary>
        public Func<long> CreationTimeGet;
        /// <summary>
        /// Sets the stub of ICacheStoreMapping.HasTimeToLiveExpired()
        /// </summary>
        public Func<bool> HasTimeToLiveExpired;
        /// <summary>
        /// Sets the stub of ICacheStoreMapping.get_Mapping()
        /// </summary>
        internal Func<IStoreMapping> MappingGet;
        /// <summary>
        /// Sets the stub of ICacheStoreMapping.ResetTimeToLive()
        /// </summary>
        public Action ResetTimeToLive;
        /// <summary>
        /// Sets the stub of ICacheStoreMapping.get_TimeToLiveMilliseconds()
        /// </summary>
        public Func<long> TimeToLiveMillisecondsGet;

        private IStubBehavior ___instanceBehavior;

        /// <summary>
        /// Gets or sets the instance behavior.
        /// </summary>
        public IStubBehavior InstanceBehavior
        {
            get
            {
                return StubBehaviors.GetValueOrCurrent(this.___instanceBehavior);
            }
            set
            {
                this.___instanceBehavior = value;
            }
        }

        long ICacheStoreMapping.CreationTime
        {
            get
          {
                Func<long> func1 = this.CreationTimeGet;
                if (func1 != null)
                    return func1();
                return this.InstanceBehavior.Result<StubICacheStoreMapping, long>(this, "Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.ICacheStoreMapping.get_CreationTime");
            }
        }

        IStoreMapping ICacheStoreMapping.Mapping
        {
            get
          {
                Func<IStoreMapping> func1 = this.MappingGet;
                if (func1 != null)
                    return func1();
                return this.InstanceBehavior.Result<StubICacheStoreMapping, IStoreMapping>(this, "Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.ICacheStoreMapping.get_Mapping");
            }
        }

        long ICacheStoreMapping.TimeToLiveMilliseconds
        {
            get
          {
                Func<long> func1 = this.TimeToLiveMillisecondsGet;
                if (func1 != null)
                    return func1();
                return this.InstanceBehavior.Result<StubICacheStoreMapping, long>(this, "Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.ICacheStoreMapping.get_TimeToLiveMilliseconds");
            }
        }

        bool ICacheStoreMapping.HasTimeToLiveExpired()
        {
            Func<bool> func1 = this.HasTimeToLiveExpired;
            if (func1 != null)
                return func1();
            return this.InstanceBehavior.Result<StubICacheStoreMapping, bool>(this, "Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.ICacheStoreMapping.HasTimeToLiveExpired");
        }

        void ICacheStoreMapping.ResetTimeToLive()
        {
            Action action1 = this.ResetTimeToLive;
            if (action1 != null)
                action1();
            else
                this.InstanceBehavior.VoidResult<StubICacheStoreMapping>(this, "Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.ICacheStoreMapping.ResetTimeToLive");
        }
    }
}
