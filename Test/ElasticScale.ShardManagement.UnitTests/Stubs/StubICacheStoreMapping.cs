using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;
using Microsoft.QualityTools.Testing.Fakes;
using Microsoft.QualityTools.Testing.Fakes.Stubs;
using System;
using System.Diagnostics;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests.Stubs
{
    /// <summary>
    /// Stub type of Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.ICacheStoreMapping
    /// </summary>
    [StubClass(typeof(ICacheStoreMapping))]
    [DebuggerDisplay("Stub of ICacheStoreMapping")]
    [DebuggerNonUserCode]
    internal class StubICacheStoreMapping : StubBase<ICacheStoreMapping>, ICacheStoreMapping
    {
        /// <summary>
        /// Sets the stub of ICacheStoreMapping.get_CreationTime()
        /// </summary>
        public FakesDelegates.Func<long> CreationTimeGet;
        /// <summary>
        /// Sets the stub of ICacheStoreMapping.HasTimeToLiveExpired()
        /// </summary>
        public FakesDelegates.Func<bool> HasTimeToLiveExpired;
        /// <summary>
        /// Sets the stub of ICacheStoreMapping.get_Mapping()
        /// </summary>
        internal FakesDelegates.Func<IStoreMapping> MappingGet;
        /// <summary>
        /// Sets the stub of ICacheStoreMapping.ResetTimeToLive()
        /// </summary>
        public FakesDelegates.Action ResetTimeToLive;
        /// <summary>
        /// Sets the stub of ICacheStoreMapping.get_TimeToLiveMilliseconds()
        /// </summary>
        public FakesDelegates.Func<long> TimeToLiveMillisecondsGet;

        long ICacheStoreMapping.CreationTime
        {
            get
          {
                IStubObserver instanceObserver = this.InstanceObserver;
                if (instanceObserver != null)
                {
                    FakesDelegates.Func<long> func = (FakesDelegates.Func<long>)StubRuntime.BindProperty(typeof(FakesDelegates.Func<long>), (object)this, typeof(ICacheStoreMapping), "CreationTime", true, typeof(long));
                    instanceObserver.Enter(typeof(ICacheStoreMapping), (Delegate)func);
                }
                FakesDelegates.Func<long> func1 = this.CreationTimeGet;
                if (func1 != null)
                    return func1();
                return this.InstanceBehavior.Result<StubICacheStoreMapping, long>(this, "Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.ICacheStoreMapping.get_CreationTime");
            }
        }

        IStoreMapping ICacheStoreMapping.Mapping
        {
            get
          {
                IStubObserver instanceObserver = this.InstanceObserver;
                if (instanceObserver != null)
                {
                    FakesDelegates.Func<IStoreMapping> func = (FakesDelegates.Func<IStoreMapping>)StubRuntime.BindProperty(typeof(FakesDelegates.Func<IStoreMapping>), (object)this, typeof(ICacheStoreMapping), "Mapping", true, typeof(IStoreMapping));
                    instanceObserver.Enter(typeof(ICacheStoreMapping), (Delegate)func);
                }
                FakesDelegates.Func<IStoreMapping> func1 = this.MappingGet;
                if (func1 != null)
                    return func1();
                return this.InstanceBehavior.Result<StubICacheStoreMapping, IStoreMapping>(this, "Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.ICacheStoreMapping.get_Mapping");
            }
        }

        long ICacheStoreMapping.TimeToLiveMilliseconds
        {
            get
          {
                IStubObserver instanceObserver = this.InstanceObserver;
                if (instanceObserver != null)
                {
                    FakesDelegates.Func<long> func = (FakesDelegates.Func<long>)StubRuntime.BindProperty(typeof(FakesDelegates.Func<long>), (object)this, typeof(ICacheStoreMapping), "TimeToLiveMilliseconds", true, typeof(long));
                    instanceObserver.Enter(typeof(ICacheStoreMapping), (Delegate)func);
                }
                FakesDelegates.Func<long> func1 = this.TimeToLiveMillisecondsGet;
                if (func1 != null)
                    return func1();
                return this.InstanceBehavior.Result<StubICacheStoreMapping, long>(this, "Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.ICacheStoreMapping.get_TimeToLiveMilliseconds");
            }
        }

        bool ICacheStoreMapping.HasTimeToLiveExpired()
        {
            IStubObserver instanceObserver = this.InstanceObserver;
            if (instanceObserver != null)
            {
                FakesDelegates.Func<bool> func = new FakesDelegates.Func<bool>(((ICacheStoreMapping)this).HasTimeToLiveExpired);
                instanceObserver.Enter(typeof(ICacheStoreMapping), (Delegate)func);
            }
            FakesDelegates.Func<bool> func1 = this.HasTimeToLiveExpired;
            if (func1 != null)
                return func1();
            return this.InstanceBehavior.Result<StubICacheStoreMapping, bool>(this, "Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.ICacheStoreMapping.HasTimeToLiveExpired");
        }

        void ICacheStoreMapping.ResetTimeToLive()
        {
            IStubObserver instanceObserver = this.InstanceObserver;
            if (instanceObserver != null)
            {
                FakesDelegates.Action action = new FakesDelegates.Action(((ICacheStoreMapping)this).ResetTimeToLive);
                instanceObserver.Enter(typeof(ICacheStoreMapping), (Delegate)action);
            }
            FakesDelegates.Action action1 = this.ResetTimeToLive;
            if (action1 != null)
                action1();
            else
                this.InstanceBehavior.VoidResult<StubICacheStoreMapping>(this, "Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.ICacheStoreMapping.ResetTimeToLive");
        }
    }
}
