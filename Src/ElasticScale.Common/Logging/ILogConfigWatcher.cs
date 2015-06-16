﻿#region "Usings"
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
