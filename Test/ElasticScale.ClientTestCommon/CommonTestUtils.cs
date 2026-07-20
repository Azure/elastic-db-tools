using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.Test.Common
{
    public static class CommonTestUtils
    {
        public static T SerializeDeserialize<T>(T originalException) where T : Exception
        {
            using (MemoryStream memStream = new MemoryStream())
            {
                BinaryFormatter formatter = new BinaryFormatter
                {
                    // Restrict deserialization to the exception graph these tests round-trip.
                    // This satisfies CA2301 and prevents BinaryFormatter from instantiating
                    // arbitrary (potentially dangerous) types.
                    Binder = new ExceptionRoundTripBinder(),
                };

                formatter.Serialize(memStream, originalException);
                memStream.Seek(0, SeekOrigin.Begin);

                return (T)formatter.Deserialize(memStream);
            }
        }

        /// <summary>
        /// A <see cref="SerializationBinder"/> that only permits the types that make up the
        /// serialized graph of the Elastic Scale exceptions exercised by the tests: the
        /// Elastic Scale types themselves (exceptions, <c>ShardLocation</c>, error-code enums),
        /// <see cref="Exception"/>, and lists/collections/arrays of those types. Any other type
        /// is rejected before it can be instantiated.
        /// </summary>
        private sealed class ExceptionRoundTripBinder : SerializationBinder
        {
            private const string ElasticScaleNamespacePrefix = "Microsoft.Azure.SqlDatabase.ElasticScale";

            public override Type BindToType(string assemblyName, string typeName)
            {
                // Resolving a Type does not instantiate anything; the object is only created
                // by the formatter after we return an allowed type. Reject disallowed types up front.
                Type type = Type.GetType(string.Format("{0}, {1}", typeName, assemblyName), throwOnError: false);

                if (type == null || !IsAllowed(type))
                {
                    throw new SerializationException(
                        string.Format("Type '{0}' from assembly '{1}' is not allowed to be deserialized.", typeName, assemblyName));
                }

                return type;
            }

            private static bool IsAllowed(Type type)
            {
                if (type.IsArray)
                {
                    return IsAllowed(type.GetElementType());
                }

                if (type.IsGenericType)
                {
                    Type definition = type.GetGenericTypeDefinition();
                    if (definition != typeof(List<>) && definition != typeof(ReadOnlyCollection<>))
                    {
                        return false;
                    }

                    return type.GetGenericArguments().All(IsAllowed);
                }

                return type.IsPrimitive
                    || type == typeof(string)
                    || type == typeof(Exception)
                    || IsElasticScaleType(type);
            }

            private static bool IsElasticScaleType(Type type) =>
                type.Namespace != null && type.Namespace.StartsWith(ElasticScaleNamespacePrefix, StringComparison.Ordinal);
        }
    }
}
