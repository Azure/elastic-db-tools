using System.Diagnostics;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Utility class for high precision time keeping.
    /// </summary>
    internal class TimerUtils
    {
        /// <summary>
        /// Ticks per millisecond = 10,000
        /// </summary>
        private const long TicksPerMilliSecond = 10000L;

        /// <summary>
        /// Ticks per second = 10,000,000
        /// </summary>
        private const double TicksPerSecond = 10000000.0;

        /// <summary>
        /// Frequency per tick of timer.
        /// </summary>
        private static double tickFrequency;

        /// <summary>
        /// Finds the appropriate tick frequency based on the timer we are using.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1810:InitializeReferenceTypeStaticFieldsInline")]
        static TimerUtils()
        {
            if (Stopwatch.IsHighResolution)
            {
                tickFrequency = TicksPerSecond / ((double)Stopwatch.Frequency);
            }
            else
            {
                tickFrequency = 1.0;
            }
        }

        /// <summary>
        /// Obtains the current timestamp.
        /// </summary>
        /// <returns>Current timestamp.</returns>
        internal static long GetTimestamp()
        {
            return Stopwatch.GetTimestamp();
        }

        /// <summary>
        /// Finds the time that has elapsed since the given <paramref name="startTimestamp"/>.
        /// </summary>
        /// <param name="startTimestamp">Original start timestamp.</param>
        /// <returns>Milliseconds interval b/w original and current time.</returns>
        internal static long ElapsedMillisecondsSince(long startTimestamp)
        {
            long elapsedTime = Stopwatch.GetTimestamp() - startTimestamp;

            if (Stopwatch.IsHighResolution)
            {
                return ((long)(((double)elapsedTime) * tickFrequency)) / TicksPerMilliSecond;
            }
            else
            {
                return elapsedTime / TicksPerMilliSecond;
            }
        }
    }
}
