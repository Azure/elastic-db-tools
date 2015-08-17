// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Runtime.Serialization;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Public type that represents the owner of a lock held on a mapping
    /// </summary>
    /// <remarks>This class is immutable</remarks>
    [Serializable()]
    [DataContract(Name = "MappingLockToken", Namespace = "")]
    public sealed class MappingLockToken : IEquatable<MappingLockToken>
    {
        /// <summary>
        /// Token representing the default state where the mapping isn't locked
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes",
            Justification = "MappingLockToken is an immutable type")]
        public static readonly MappingLockToken NoLock = new MappingLockToken(default(Guid));

        /// <summary>
        /// Token that can be used to force an unlock on any locked mapping
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes",
            Justification = "MappingLockToken is an immutable type")]
        public static readonly MappingLockToken ForceUnlock = new MappingLockToken(
            new Guid("FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF"));

        /// <summary>
        /// Instantiates an instance of <see cref="MappingLockToken"/>
        /// with the given lock owner id
        /// </summary>
        /// <param name="lockOwnerId">The lock owner id</param>
        internal MappingLockToken(Guid lockOwnerId)
        {
            this.LockOwnerId = lockOwnerId;
        }

        [DataMember()]
        internal Guid LockOwnerId
        {
            get;
            private set;
        }

        /// <summary>
        /// Creates an instance of <see cref="MappingLockToken"/>
        /// </summary>
        /// <returns>An instance of <see cref="MappingLockToken"/></returns>
        public static MappingLockToken Create()
        {
            return new MappingLockToken(Guid.NewGuid());
        }

        /// <summary>
        /// Determines whether the specified object is equal to the current object.
        /// </summary>
        /// <param name="obj">The object to compare with the current object.</param>
        /// <returns>True if the specified object is equal to the current object; otherwise, false.</returns>
        public override bool Equals(object obj)
        {
            return this.Equals(obj as MappingLockToken);
        }

        /// <summary>
        /// Compares two instances of <see cref="MappingLockToken"/>
        /// to see if they have the same owner
        /// </summary>
        /// <param name="other"></param>
        /// <returns>True if they both belong to the same lock owner</returns>
        public bool Equals(MappingLockToken other)
        {
            if (other != null)
            {
                return this.LockOwnerId == other.LockOwnerId;
            }

            return false;
        }

        /// <summary>
        /// Equality operator.
        /// </summary>
        /// <param name="leftMappingLockToken">An instance of <see cref="MappingLockToken"/></param>
        /// <param name="rightMappingLockToken">An instance of <see cref="MappingLockToken"/></param>
        /// <returns>True if both belong to the same lock owner</returns>
        public static bool operator ==(MappingLockToken leftMappingLockToken, MappingLockToken rightMappingLockToken)
        {
            return object.Equals(leftMappingLockToken, rightMappingLockToken);
        }

        /// <summary>
        /// Inequality operator.
        /// </summary>
        /// <param name="leftMappingLockToken">An instance of <see cref="MappingLockToken"/></param>
        /// <param name="rightMappingLockToken">An instance of <see cref="MappingLockToken"/></param>
        /// <returns>True if both belong to the same lock owner</returns>
        public static bool operator !=(MappingLockToken leftMappingLockToken, MappingLockToken rightMappingLockToken)
        {
            return !(leftMappingLockToken == rightMappingLockToken);
        }

        /// <summary>
        /// Calculates the hash code for this instance.
        /// </summary>
        /// <returns>Hash code for the object.</returns>
        public override int GetHashCode()
        {
            return this.LockOwnerId.GetHashCode();
        }
    }
}
