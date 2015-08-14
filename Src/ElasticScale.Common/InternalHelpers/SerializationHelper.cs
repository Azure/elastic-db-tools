// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace Microsoft.Azure.SqlDatabase.ElasticScale
{
    /// <summary>
    /// Provide XML serialization\deserialization services.
    /// </summary>
    internal static class SerializationHelper
    {
        /// <summary>
        /// Serialize the members of an object of type T into SqlXml format.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static SqlXml SerializeXmlData<T>(T obj) where T : class
        {
            if (obj == null)
            {
                return SqlXml.Null;
            }

            using (MemoryStream memStream = new MemoryStream())
            {
                DataContractSerializer serializer = new DataContractSerializer(obj.GetType());
                serializer.WriteObject(memStream, obj);

                memStream.Position = 0;
                return new SqlXml(XmlReader.Create(memStream));
            }
        }

        /// <summary>
        /// Deserialize the contents of a string object into an object of type T.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="xml"></param>
        /// <returns></returns>
        public static T DeserializeXmlData<T>(string xml)
        {
            T objData = default(T);

            if (!string.IsNullOrEmpty(xml))
            {
                TextReader textReader = null;
                try
                {
                    textReader = new StringReader(xml);
                    using (XmlReader xmlReader = XmlReader.Create(textReader))
                    {
                        textReader = null;
                        DataContractSerializer serializer = new DataContractSerializer(typeof(T));
                        objData = (T)serializer.ReadObject(xmlReader);
                    }
                }
                finally
                {
                    if (textReader != null)
                    {
                        textReader.Dispose();
                    }
                }
            }

            return objData;
        }

        /// <summary>
        /// Deserialize the contents of a SqlXml object into an object of type T.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="xml"></param>
        /// <returns></returns>
        public static T DeserializeXmlData<T>(SqlXml xml)
        {
            T objData = default(T);

            if ((null != xml) && (!xml.IsNull))
            {
                using (XmlReader xmlReader = xml.CreateReader())
                {
                    DataContractSerializer serializer = new DataContractSerializer(typeof(T));
                    objData = (T)serializer.ReadObject(xmlReader);
                }
            }

            return objData;
        }

        /// <summary>
        /// Determines whether this obj can be XML serialized.
        /// (the reason object may fail serialization is if it contains characters considered invalid by XML standard)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="obj">The object.</param>
        /// <returns></returns>
        /// <remarks>
        /// This is fairly expensive check as it actually serializes and discards results, 
        /// but it's good enough for usage in tests
        /// </remarks>
        internal static bool CanBeXmlSerialized<T>(T obj) where T : class
        {
            try
            {
                SerializeXmlData(obj);
                return true;
            }
            catch (XmlException)
            {
                return false;
            }
        }
    }
}
