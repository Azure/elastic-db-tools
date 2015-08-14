// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.Recovery
{
    /// <summary>
    /// Recovery token generated and used by methods of the <see cref="RecoveryManager"/> 
    /// to perform conflict detection and resolution for shard maps.
    /// </summary>
    public sealed class RecoveryToken : IEquatable<RecoveryToken>
    {
        /// <summary>
        /// Parameterless constructor to generate a new unique token for shard map conflict detection and resolution.
        /// </summary>
        internal RecoveryToken()
        {
            this.Id = Guid.NewGuid();
        }

        /// <summary>
        /// Internal Guid for this token.
        /// </summary>
        private Guid Id
        {
            get;
            set;
        }

        /// <summary>
        /// Converts the object to its string representation.
        /// </summary>
        /// <returns>String representation of the object.</returns>
        public override string ToString()
        {
            return this.Id.ToString();
        }

        /// <summary>
        /// Calculates the hash code for this instance.
        /// </summary>
        /// <returns>Hash code for the object.</returns>
        public override int GetHashCode()
        {
            return this.Id.GetHashCode();
        }

        /// <summary>
        /// Determines whether the specified Object is equal to the current Object.
        /// </summary>
        /// <param name="obj">The object to compare with the current object.</param>
        /// <returns>True if the specified object is equal to the current object; otherwise, false.</returns>
        public override bool Equals(object obj)
        {
            RecoveryToken other = obj as RecoveryToken;

            if (other == null)
            {
                return false;
            }

            return this.Equals(other);
        }

        /// <summary>
        /// Performs equality comparison with another given RecoveryToken.
        /// </summary>
        /// <param name="other">RecoveryToken to compare with.</param>
        /// <returns>True if same locations, false otherwise.</returns>
        public bool Equals(RecoveryToken other)
        {
            if (other == null)
            {
                return false;
            }

            return this.Id.Equals(other.Id);
        }
    }
}
