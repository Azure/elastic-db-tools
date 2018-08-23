using System;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.Test.Common
{
    public static class CommonTestUtils
    {
        public static T SerializeDeserialize<T>(T originalException) where T : Exception
        {
            using (MemoryStream memStream = new MemoryStream())
            {
                BinaryFormatter formatter = new BinaryFormatter();

                formatter.Serialize(memStream, originalException);
                memStream.Seek(0, SeekOrigin.Begin);

                return (T)formatter.Deserialize(memStream);
            }
        }
    }
}
