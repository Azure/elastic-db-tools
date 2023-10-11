﻿// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ElasticScale.Common.Patterns;

/// <summary>
/// A disposable object which opts-out of disposing the inner disposable
/// only when instructed by the caller.
/// </summary>
/// <typeparam name="T"></typeparam>
internal sealed class ConditionalDisposable<T> : IDisposable where T : IDisposable
{

    /// <summary>
    /// Constructor which takes an inner disposable object.
    /// </summary>
    /// <param name="innerDisposable"></param>
    public ConditionalDisposable(T innerDisposable) => Value = innerDisposable;

    /// <summary>
    /// Used for notifying about disposable decision on inner object.
    /// </summary>
    internal bool DoNotDispose
    {
        get;
        set;
    }

    /// <summary>
    /// Disposes the inner object if DoNotDispose is set to false.
    /// </summary>
    public void Dispose()
    {
        if (!DoNotDispose)
            Value.Dispose();
    }

    /// <summary>
    /// Gets the inner disposable object.
    /// </summary>
    public T Value { get; }
}
