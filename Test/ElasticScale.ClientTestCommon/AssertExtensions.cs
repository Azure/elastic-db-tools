using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.Test.Common
{
    public static class AssertExtensions
    {
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1026:DefaultParametersShouldNotBeUsed")]
        public static async Task AssertCanceledWithinTimeout(CancellationToken cancellationToken, TimeSpan timeout, string message = null)
        {
            try
            {
                await Task.Delay(timeout, cancellationToken);
                if (string.IsNullOrEmpty(message))
                {
                    Assert.Fail("The cancellation token was not cancelled within timeout of {0}", timeout);
                }
                else
                {
                    Assert.Fail(
                        "The cancellation token was not cancelled within timeout of {0}. Additional message: {1}",
                        timeout, message);
                }
            }
            catch (TaskCanceledException)
            {
                return;
            }
        }

        /// <summary>
        /// Asserts that executing the action throws exception of the specified type or one of its derived types. 
        /// If it is thrown, return the exception. If a different type of exception is thrown, it is not caught. 
        /// If nothing is thrown, the test fails.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public static TException AssertThrows<TException>(Action action) where TException : Exception
        {
            if (action == null)
            {
                throw new ArgumentNullException("action");
            }

            try
            {
                action();

                // Exception not thrown
                Assert.Fail("Exception of type {0} was expected, but no exception was thrown", typeof (TException));

                // Next line will never execute, it is required by the compiler
                return null;
            }
            catch (TException e)
            {
                // Success
                return e;
            }
            catch (Exception e)
            {
                // Wrong exception thrown
                Assert.Fail("Exception of type {0} was expected, exception of type {1} was thrown: {2}",
                    typeof (TException), e.GetType(), e.ToString());
                // Next line will never execute, it is required by the compiler
                return null;
            }
        }

        /// <summary>
        /// Asserts that executing the action throw an exception of type TExceptionOuter containing (at any .InnerException depth) an inner expcetion of type
        /// TExceptionInner. Both the instances of the outer and inner exceptions are returned to the caller through the out parameters.
        /// If a different type of exception is thrown, it is not caught. If nothing is thrown, the test fails.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1021:AvoidOutParameters", MessageId = "1#"), System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1021:AvoidOutParameters", MessageId = "2#")]
        public static void AssertThrows<TExceptionOuter, TExceptionInner>(Action action, out TExceptionOuter outer, out TExceptionInner inner) 
            where TExceptionOuter : Exception where TExceptionInner : Exception
        {
            outer = null;
            inner = null;

            if (action == null)
            {
                throw new ArgumentNullException("action");
            }

            try
            {
                action();

                // Exception not thrown
                Assert.Fail("Exception of type {0} containing {1} was expected, but was not thrown", 
                    typeof(TExceptionOuter), typeof(TExceptionInner));
            }
            catch (Exception ex)
            {
                TExceptionOuter exceptionOuter = ex as TExceptionOuter;

                if (exceptionOuter == null)
                {
                    throw;
                }

                outer = exceptionOuter;

                // Try to find an exception of that specified type
                do
                {
                    TExceptionInner exceptionInner = ex as TExceptionInner;
                    if (exceptionInner != null)
                    {
                        inner = exceptionInner;
                        return;
                    }
                }
                while ((ex = ex.InnerException) != null);

                // No exception was found, so rethrow
                throw;
            }
        }
        
        /// <summary>
        /// Waits for the task to complete and asserts that it throws an AggregateException with one InnerException, which is of the specified type.
        /// </summary>
        public static TException WaitAndAssertThrows<TException>(Task task) where TException : Exception
        {
            return WaitAndAssertThrows<TException>(task, TimeSpan.FromMilliseconds(-1));
        }

        /// <summary>
        /// Waits for the task to complete within the timeout and asserts that it throws one TException wrapped into AggregateException.
        /// </summary>
        public static TException WaitAndAssertThrows<TException>(Task task, TimeSpan timeout) where TException : Exception
        {
            if (task == null)
            {
                throw new ArgumentNullException("task");
            }

            try
            {
                if (task.Wait(timeout))
                {
                    // Exception not thrown, task completed
                Assert.Fail(
                    "The Task was expected to fail with an exception of type {0}, but it succeeded", 
                        typeof (TException));
                }
                else
                {
                    // Exception not thrown within timeout, task still running
                    Assert.Fail(
                        "The Task was expected to fail with an exception of type {0} within timeout {1}, but it is still running. Note: this leaves running task, which may have side effects.",
                        typeof(TException),
                        timeout);
                }
                // Next line will never execute, it is required by the compiler
                return null;
            }
            catch (AggregateException ex)
            {
                if (ex.InnerExceptions.Count == 1)
                {
                    TException innerException = ex.InnerException as TException;
                    if (innerException != null)
                    {
                        // There is exactly 1 inner exception, and it has the correct type
                        return innerException;
                    }
                }

                throw;
            }
        }


        public static string ToCommaSeparatedString<T>(this IEnumerable<T> collection)
        {
            return string.Join(", ", collection);
        }

        /// <summary>
        /// Asserts that the collection contains the expected item.
        /// </summary>
        public static void AssertContains<T>(T item, IEnumerable<T> collection)
        {
            AssertContains(item, collection, EqualityComparer<T>.Default);
        }

        /// <summary>
        /// Asserts that the specified outerString contains the specified substring.
        /// </summary>
        public static void AssertContains(string substring, string value)
        {
            if (value == null)
            {
                throw new ArgumentNullException("value");
            }

            int substringIndex = value.IndexOf(substring, StringComparison.Ordinal);
            if (substringIndex >= 0)
            {
                // Success
                return;
            }

            Assert.Fail("Expected string \'{0}\' to contain substring \'{1}\'", value, substring);
        }

        /// <summary>
        /// Asserts that the collection does not contain the expected item.
        /// </summary>
        public static void AssertDoesNotContain<T>(T item, IEnumerable<T> collection, IEqualityComparer<T> equalityComparer)
        {
            if (!collection.Contains(item, equalityComparer))
            {
                // Success
                return;
            }

            Assert.Fail("Item {0} found in sequence {1}", item, collection.ToCommaSeparatedString());
        }

        /// <summary>
        /// Asserts that the collection does not contain the expected item.
        /// </summary>
        public static void AssertDoesNotContain<T>(T item, IEnumerable<T> collection)
        {
            AssertDoesNotContain(item, collection, EqualityComparer<T>.Default);
        }

        /// <summary>
        /// Asserts that the collection contains the expected item.
        /// </summary>
        public static void AssertContains<T>(T item, IEnumerable<T> collection, IEqualityComparer<T> equalityComparer)
        {
            if (collection.Contains(item, equalityComparer))
            {
                // Success
                return;
            }

            Assert.Fail("Item {0} not found in sequence {1}", item, collection.ToCommaSeparatedString());
        }

        /// <summary>
        /// Asserts that two IEnumerables are equal, i.e. their elements are in the same order and are equal
        /// (using .Equals) to each other.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1026:DefaultParametersShouldNotBeUsed")]
        public static void AssertSequenceEqual<T>(IEnumerable<T> expected, IEnumerable<T> actual, string message = "")
        {
            AssertSequenceEqual(expected, actual, EqualityComparer<T>.Default, message);
        }

        /// <summary>
        /// Asserts that two IEnumerables are equal, i.e. their elements are in the same order and are equal
        /// (using .Equals) to each other.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1026:DefaultParametersShouldNotBeUsed")]
        public static void AssertSequenceEqual<T>(IEnumerable<T> expected, IEnumerable<T> actual, IEqualityComparer<T> equalityComparer, string message = "")
        {
            T[] expectedArray = expected.ToArray();
            T[] actualArray = actual.ToArray();
            if (expectedArray.SequenceEqual(actualArray, equalityComparer))
            {
                // Success
                return;
            }

            Assert.Fail(
                "Sequences were not equal. Message: {0}. Expected sequence had {1} elements, actual had {2}. Comma separated contents for expected: <{3}>, for actual: <{4}>",
                message,
                expectedArray.Length,
                actualArray.Length,
                expectedArray.ToCommaSeparatedString(),
                actualArray.ToCommaSeparatedString());
        }

        /// <summary>
        /// Asserts that two IEnumerables are equivalent, i.e. their elements might be in different order and but
        /// are the same.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1026:DefaultParametersShouldNotBeUsed")]
        public static void AssertSequenceEquivalent<T>(IEnumerable<T> expected, IEnumerable<T> actual, string message = "")
        {
            AssertSequenceEquivalent(expected, actual, EqualityComparer<T>.Default, message);
        }

        /// <summary>
        /// Asserts that two IEnumerables are equivalent, i.e. their elements might be in different order and but
        /// are the same.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1026:DefaultParametersShouldNotBeUsed")]
        public static void AssertSequenceEquivalent<T>(IEnumerable<T> expected, IEnumerable<T> actual, IEqualityComparer<T> equalityComparer, string message = "")
        {
            if (equalityComparer == null)
            {
                equalityComparer = EqualityComparer<T>.Default;
            }
            T[] expectedArray = expected.ToArray();
            T[] actualArray = actual.ToArray();

            if (expectedArray.Length == actualArray.Length)
            {
                HashSet<T> expectedSet = new HashSet<T>(expectedArray, equalityComparer);
                bool expectedSetContainsAllActual = actualArray.All(a => expectedSet.Contains(a, equalityComparer));
                if (expectedSetContainsAllActual)
                {
                    // Success
                    return;
                }
            }

            Assert.Fail(
                "Sequences were not equivalent. Message: {0}. Expected sequence had {1} elements, actual had {2}. Comma separated contents for expected: <{3}>, for actual: <{4}>",
                message,
                expectedArray.Length,
                actualArray.Length,
                expectedArray.ToCommaSeparatedString(),
                actualArray.ToCommaSeparatedString());
        }

        /// <summary>
        /// Asserts that the actual value is greater than the provided value.
        /// </summary>
        /// <param name="greaterThan">the value that <paramref name="actual"/> must be greater than</param>
        /// <param name="actual">the actual value</param>
        public static void AssertGreaterThan<T>(T greaterThan, T actual)
            where  T : IComparable<T>
        {
            if (actual.CompareTo(greaterThan) > 0)
            {
                // actual > greaterThan
                // Success
                return;
            }

            Assert.Fail("Actual value {0} was not greater than {1}.", actual, greaterThan);
        }

        /// <summary>
        /// Asserts that the actual value is greater than or equal to the provided value.
        /// </summary>
        /// <param name="greaterThanOrEqualTo">the value that <paramref name="actual"/> must be greater than</param>
        /// <param name="actual">the actual value</param>
        public static void AssertGreaterThanOrEqualTo<T>(T greaterThanOrEqualTo, T actual)
            where T : IComparable<T>
        {
            if (actual.CompareTo(greaterThanOrEqualTo) >= 0)
            {
                // actual >= greaterThanOrEqualTo
                // Success
                return;
            }

            Assert.Fail("Actual value {0} was not greater than or equal to {1}.", actual, greaterThanOrEqualTo);
        }

        /// <summary>
        /// Asserts that the actual value is less than the provided value.
        /// </summary>
        /// <param name="lessThan">the value that <paramref name="actual"/> must be greater than</param>
        /// <param name="actual">the actual value</param>
        public static void AssertLessThan<T>(T lessThan, T actual)
            where T : IComparable<T>
        {
            if (actual.CompareTo(lessThan) < 0)
            {
                // actual < lessThan
                // Success
                return;
            }

            Assert.Fail("Actual value {0} was not less than {1}.", actual, lessThan);
        }

        /// <summary>
        /// Asserts that the actual value is less than or equal to the provided value.
        /// </summary>
        /// <param name="lessThanOrEqualTo">the value that <paramref name="actual"/> must be greater than</param>
        /// <param name="actual">the actual value</param>
        public static void AssertLessThanOrEqualTo<T>(T lessThanOrEqualTo, T actual)
            where T : IComparable<T>
        {
            if (actual.CompareTo(lessThanOrEqualTo) <= 0)
            {
                // actual <= lessThanOrEqualTo
                // Success
                return;
            }

            Assert.Fail("Actual value {0} was not less than or equal to {1}.", actual, lessThanOrEqualTo);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1026:DefaultParametersShouldNotBeUsed")]
        public static void AssertEmpty<T>(this IEnumerable<T> enumerable, string message = "No elements were expected")
        {
            T[] array = enumerable.ToArray();
            if (array.Length == 0)
            {
                return;
            }
            Assert.Fail(
                "Message: {0}. Collection has {1} elements. string.Join representation: <{2}>",
                message,
                array.Length,
                string.Join<T>(",", array));
        }

        public static void AssertScalarOrSequenceEqual(object o1, object o2, string message = null)
        {
            if (o1 is IEnumerable)
            {
                AssertSequenceEqual(((IEnumerable)o1).Cast<object>(), ((IEnumerable)o2).Cast<object>(), message);
            }
            else
            {
                Assert.AreEqual(o1, o2, message);
            }
        }

        /// <summary>
        /// Asserts that the enumerable contains only one element, and returns it.
        /// </summary>
        public static T AssertSingle<T>(this IEnumerable<T> enumerable)
        {
            // Copy the enumerable to an array. Otherwise, when we get to the end of the enumerator
            // and realise that there was more than one element, we cannot print out the elements
            // without re-enumerating. If the caller don't want us to be helpful and print out the 
            // elements, they can just use Enumerable.Single() instead.
            T[] array = enumerable.ToArray();

            if (array.Length == 0)
            {
                Assert.Fail("The enumerable contains no elements.");
            }
            else if (array.Length == 1)
            {
                return array[0];
            }
            else
            {
                // Write out the items
                Console.Error.WriteLine("Items: [{0}]", array.ToCommaSeparatedString());

                // This assert will fail
                Assert.AreEqual(1, array.Length);
            }

            // The compiler doesn't know that the above Assert.Fail or Assert.AreEqual 
            // will throw, so it requires a throw or return.
            throw new InvalidOperationException();
        }

        /// <summary>
        /// Asserts that the given objects are equal, using the specified equalityComparer.
        /// </summary>
        public static void AreEqual<T>(T expected, T actual, IEqualityComparer<T> equalityComparer)
        {
            if (equalityComparer == null)
            {
                throw new ArgumentNullException("equalityComparer");
            }
            if (!equalityComparer.Equals(expected, actual))
            {
                Assert.Fail("Assert.AreEqual failed. expected: {0}, actual: {1}", expected, actual);
            }
        }
    }
}
