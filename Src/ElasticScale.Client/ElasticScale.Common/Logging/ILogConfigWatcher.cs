﻿// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

#region "Usings"

using System;
#endregion

namespace Microsoft.Azure.SqlDatabase.ElasticScale
{
    /// <summary>
    /// Interface for exposing a logging configuration change
    /// </summary>
    internal interface ILogConfigWatcher
    {
        event EventHandler OnConfigChange;
    }
}
