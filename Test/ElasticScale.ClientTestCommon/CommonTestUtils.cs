using System;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ClientTestCommon;

public static class CommonTestUtils
{
    public static T SerializeDeserialize<T>(T originalException) where T : Exception
    {
        using var memStream = new MemoryStream();
        var formatter = new BinaryFormatter();

        formatter.Serialize(memStream, originalException);
        _ = memStream.Seek(0, SeekOrigin.Begin);

        return (T)formatter.Deserialize(memStream);
    }
}
