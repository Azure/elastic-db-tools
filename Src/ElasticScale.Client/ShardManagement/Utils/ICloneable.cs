// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Represents objects that can clone themselves.
    /// </summary>
    /// <typeparam name="T">Type of object</typeparam>
    internal interface ICloneable<T> where T : ICloneable<T>
    {
        /// <summary>
        /// Clones the instance which implements the interface.
        /// </summary>
        /// <returns>Clone of the instance.</returns>
        T Clone();
    }
}
