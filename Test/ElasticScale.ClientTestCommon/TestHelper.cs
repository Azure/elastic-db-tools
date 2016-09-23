using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.SqlDatabase.ElasticScale.Test.Common;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ClientTestCommon {
    public class TimedTest : IDisposable {

        private readonly Timer _timer;
        private bool isDisposed = false;

        public TimedTest(int timeout_in_ms = 10000) {
            _timer = new Timer(timer, null, timeout_in_ms, Timeout.Infinite);
        }

        private void timer(object state) {
            if(!this.isDisposed)
                AssertExtensions.Fail("Timeout Exceeded");
        }

        public void Dispose() {
            this.isDisposed = true;
            _timer.Dispose();
        }
    }


}
