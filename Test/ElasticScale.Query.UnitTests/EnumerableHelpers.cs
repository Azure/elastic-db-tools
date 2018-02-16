using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.Query.UnitTests
{
    public static class EnumerableHelpers
    {
        public static IEnumerable<T> ToConsumable<T>(this IEnumerable<T> source)
        {
            return new ConsumingEnumerable<T>(source);
        }

        /// <summary>
        /// IEnumerable wrapper that can only be enumerated once.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        public class ConsumingEnumerable<T> : IEnumerable<T>
        {
            private IEnumerable<T> _source;
            private int _consumed;

            public ConsumingEnumerable(IEnumerable<T> source)
            {
                _source = source;
                _consumed = 0;
            }

            public IEnumerator<T> GetEnumerator()
            {
                int wasConsumed = Interlocked.Exchange(ref _consumed, 1);
                if (wasConsumed == 0)
                {
                    return _source.GetEnumerator();
                }
                else
                {
                    throw new InvalidOperationException("GetEnumerator() has already been called. Cannot enumerate more than once");
                }
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }
        }
    }
}
