// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.Xml;
using System.Xml.Linq;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Serializes and deserializes the store objects to/from xml format.
    /// </summary>
    internal class StoreObjectFormatterXml
    {
        /// <summary>
        /// Serializes shard map to xml.
        /// </summary>
        /// <param name="name">Element name for shard map.</param>
        /// <param name="shardMap">Shard Map to serialize.</param>
        /// <returns>XElement representing Shard Map.</returns>
        internal static XElement WriteIStoreShardMap(string name, IStoreShardMap shardMap)
        {
            if (shardMap == null)
            {
                return new XElement(name, new XAttribute("Null", 1));
            }
            else
            {
                return new XElement(name,
                    new XAttribute("Null", 0),
                    new XElement("Id", shardMap.Id),
                    new XElement("Name", shardMap.Name),
                    new XElement("Kind", (int)shardMap.MapType),
                    new XElement("KeyKind", (int)shardMap.KeyType));
            }
        }

        /// <summary>
        /// Deserializes shard map from xml.
        /// </summary>
        /// <param name="xe">XElement representing shard map.</param>
        /// <returns>ShardMap read from <paramref name="xe"/>.</returns>
        internal static IStoreShardMap ReadIStoreShardMap(XElement xe)
        {
            if (Int32.Parse(xe.Attribute("Null").Value) == 0)
            {
                return new DefaultStoreShardMap(
                    Guid.Parse(xe.Element("Id").Value),
                    xe.Element("Name").Value,
                    (ShardMapType)Int32.Parse(xe.Element("Kind").Value),
                    (ShardKeyType)Int32.Parse(xe.Element("KeyKind").Value));
            }
            else
            {
                return null;
            }
        }

        /// <summary>
        /// Serializes shard to xml.
        /// </summary>
        /// <param name="name">Element name for shard.</param>
        /// <param name="shard">Shard to serialize.</param>
        /// <returns>XElement representing Shard.</returns>
        internal static XElement WriteIStoreShard(string name, IStoreShard shard)
        {
            if (shard == null)
            {
                return new XElement(name, new XAttribute("Null", 1));
            }
            else
            {
                return new XElement(name, new XAttribute("Null", 0),
                    new XElement("Id", shard.Id),
                    new XElement("Version", shard.Version),
                    new XElement("ShardMapId", shard.ShardMapId),
                    WriteShardLocation("Location", shard.Location),
                    new XElement("Status", (int)shard.Status));
            }
        }

        /// <summary>
        /// Deserializes shard from xml.
        /// </summary>
        /// <param name="xe">XElement representing shard.</param>
        /// <returns>Shard read from <paramref name="xe"/>.</returns>
        internal static IStoreShard ReadIStoreShard(XElement xe)
        {
            if (Int32.Parse(xe.Attribute("Null").Value) == 0)
            {
                return new DefaultStoreShard(
                    Guid.Parse(xe.Element("Id").Value),
                    Guid.Parse(xe.Element("Version").Value),
                    Guid.Parse(xe.Element("ShardMapId").Value),
                    ReadShardLocation(xe.Element("Location")),
                    Int32.Parse(xe.Element("Status").Value));
            }
            else
            {
                return null;
            }
        }

        /// <summary>
        /// Serializes shard mapping to xml.
        /// </summary>
        /// <param name="name">Element name for shard mapping.</param>
        /// <param name="mapping">Shard mapping to serialize.</param>
        /// <returns>XElement representing shard mapping.</returns>
        internal static XElement WriteIStoreMapping(string name, IStoreMapping mapping)
        {
            Debug.Assert(mapping != null);

            return new XElement(name,
                new XElement("Id", mapping.Id),
                new XElement("ShardMapId", mapping.ShardMapId),
                new XElement("MinValue", StringUtils.ByteArrayToString(mapping.MinValue)),
                new XElement("MaxValue",
                    new XAttribute("Null", mapping.MaxValue == null ? 1 : 0),
                    mapping.MaxValue == null ? null : StringUtils.ByteArrayToString(mapping.MaxValue)),
                new XElement("Status", mapping.Status),
                new XElement("LockOwnerId", mapping.LockOwnerId));
        }

        /// <summary>
        /// Deserializes shard mapping from xml.
        /// </summary>
        /// <param name="xe">XElement representing shard mapping.</param>
        /// <param name="xeShard">XElement representing shard corresponding to shard mapping.</param>
        /// <returns>Shard mapping read from <paramref name="xe"/> and <paramref name="xeShard"/>.</returns>
        internal static IStoreMapping ReadIStoreMapping(XElement xe, XElement xeShard)
        {
            return new DefaultStoreMapping(
                Guid.Parse(xe.Element("Id").Value),
                Guid.Parse(xe.Element("ShardMapId").Value),
                ReadIStoreShard(xeShard),
                StringUtils.StringToByteArray(xe.Element("MinValue").Value),
                Int32.Parse(xe.Element("MaxValue").Attribute("Null").Value) == 1 ?
                    null :
                    StringUtils.StringToByteArray(xe.Element("MaxValue").Value),
                Int32.Parse(xe.Element("Status").Value),
                Guid.Parse(xe.Element("LockOwnerId").Value));
        }

        /// <summary>
        /// Serializes shard location to xml.
        /// </summary>
        /// <param name="name">Element name for shard location.</param>
        /// <param name="location">Shard location to serialize.</param>
        /// <returns>XElement representing shard location.</returns>
        internal static XElement WriteShardLocation(string name, ShardLocation location)
        {
            Debug.Assert(location != null);

            return new XElement(name,
                new XElement("Protocol", (int)location.Protocol),
                new XElement("ServerName", location.Server),
                new XElement("Port", location.Port),
                new XElement("DatabaseName", location.Database));
        }

        /// <summary>
        /// Deserializes shard location from xml.
        /// </summary>
        /// <param name="xe">XElement representing shard location.</param>
        /// <returns>Shard location read from <paramref name="xe"/>.</returns>
        internal static ShardLocation ReadShardLocation(XElement xe)
        {
            return new ShardLocation(
                xe.Element("ServerName").Value,
                xe.Element("DatabaseName").Value,
                (SqlProtocol)Int32.Parse(xe.Element("Protocol").Value),
                Int32.Parse(xe.Element("Port").Value));
        }

        /// <summary>
        /// Serializes a lock object.
        /// </summary>
        /// <param name="lockOwner">Lock owner.</param>
        /// <returns>XElement representing serializes lock object.</returns>
        internal static XElement WriteLock(Guid lockOwner)
        {
            return new XElement("Lock",
                new XElement("Id", lockOwner));
        }

        /// <summary>
        /// Deserializes a lock object.
        /// </summary>
        /// <param name="xe">XElement representing lock.</param>
        /// <returns>Lock owner id for the lock object.</returns>
        internal static Guid ReadLock(XElement xe)
        {
            return Guid.Parse(xe.Element("Id").Value);
        }

        /// <summary>
        /// Converts a given shard range to xml.
        /// </summary>
        /// <param name="range">Input range.</param>
        /// <returns>XElement representing the given range.</returns>
        internal static XElement WriteShardRange(ShardRange range)
        {
            if (range == null)
            {
                return new XElement("Range", new XAttribute("Null", 1));
            }
            else
            {
                return new XElement("Range", new XAttribute("Null", 0),
                    new XElement("MinValue", StringUtils.ByteArrayToString(range.Low.RawValue)),
                    new XElement("MaxValue",
                        new XAttribute("Null", range.High.IsMax ? 1 : 0),
                        range.High.IsMax ? null : StringUtils.ByteArrayToString(range.High.RawValue)));
            }
        }

        /// <summary>
        /// Converts a given shard key to xml.
        /// </summary>
        /// <param name="key">Input key.</param>
        /// <returns>XElement representing the given key.</returns>
        internal static XElement WriteShardKey(ShardKey key)
        {
            Debug.Assert(key != null);
            return new XElement("Key",
                new XElement("Value", StringUtils.ByteArrayToString(key.RawValue)));
        }

        /// <summary>
        /// Serializes schema information to xml.
        /// </summary>
        /// <param name="name">Name of the schema information element.</param>
        /// <param name="schemaInfo">Actual schema info.</param>
        /// <returns>XElement representing the given schema info.</returns>
        internal static XElement WriteIStoreSchemaInfo(string name, IStoreSchemaInfo schemaInfo)
        {
            Debug.Assert(schemaInfo != null);

            using (XmlReader r = schemaInfo.ShardingSchemaInfo.CreateReader())
            {
                return new XElement(name,
                    new XElement("Name", schemaInfo.Name),
                    new XElement("Info", XElement.Load(r)));
            }
        }
    }
}
