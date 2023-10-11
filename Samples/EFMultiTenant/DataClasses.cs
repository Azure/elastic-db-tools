// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;

namespace EntityFrameworkMultiTenant;

// Let's use the standard blogging classes from the EF tutorial
public class Blog
{
    public int BlogId { get; set; }

    public string Name { get; set; }

    public string Url { get; set; }

    public virtual List<Post> Posts { get; set; }

    // All tables must have a TenantId to indicate which tenant owns each row.
    // Note: you should REMOVE TenantId from the EF data model (but not the underlying database tables) 
    // if you plan to use default constraints, otherwise EF will automatically supply a default value
    public int TenantId { get; set; }
}

public class Post
{
    public int PostId { get; set; }

    public string Title { get; set; }

    public string Content { get; set; }

    public int BlogId { get; set; }

    public virtual Blog Blog { get; set; }

    public int TenantId { get; set; } // same as TenantId in Blogs
}

public class User
{
    [Key]
    public string UserName { get; set; }

    public string DisplayName { get; set; }
}