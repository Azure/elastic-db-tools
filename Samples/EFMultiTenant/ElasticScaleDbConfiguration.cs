// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Configuration;
using System.Data.Entity;
using System.Data.Entity.Infrastructure;

namespace EFMultiTenantElasticScale
{
    public class ElasticScaleDbConfiguration : DbConfiguration
    {
        public ElasticScaleDbConfiguration()
        {
            // This sets the execution strategy so that transient faults are handled 
            // by the Transient Fault Handling Block. We are not using the default
            // execution strategy for SQL Server here as its exceptions encourage to use
            // the SqlAzureExecutionStrategy which would lead to wrong retry behavior
            // since it would not use the OpenConnectionForKey call. 
            // For more details, see http://msdn.microsoft.com/en-us/data/dn456835.aspx. 
            this.SetExecutionStrategy("System.Data.SqlClient", () => new DefaultExecutionStrategy());

            // There are legitimate cases, typically for migrations during development 
            // using Add-Migration and Update-Datase, where a connection to a 
            // development database is needed.
            // 
            // Usually, that would go through the DbContext default c'tor. However,
            // we limited calls to DbContext to just data dependent routing. This DbConfig
            // gives EF the opportunity to still bootstrap connections for those cases.
            // EF then finds and invokes the following c'tor when needed. 
            // For more information on DbConfiguration, see http://msdn.microsoft.com/en-us/data/jj680699.aspx.
            this.SetContextFactory<ElasticScaleContext<int>>(() =>
            {
                string connStr = ConfigurationManager.ConnectionStrings["DevelopmentDatabase"].ConnectionString;
                return new ElasticScaleContext<int>(connStr);
            });
        }
    }
}