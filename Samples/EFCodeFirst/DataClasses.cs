// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;

namespace EFCodeFirstElasticScale
{
    // Let's use the standard blogging classes from the EF tutorial
    public class Blog
    {
        public int BlogId { get; set; }

        public string Name { get; set; }

        public string Url { get; set; }

        public virtual List<Post> Posts { get; set; }
    }

    public class Post
    {
        public int PostId { get; set; }

        public string Title { get; set; }

        public string Content { get; set; }

        public int BlogId { get; set; }

        public virtual Blog Blog { get; set; }
    }

    public class User
    {
        [Key]
        public string UserName { get; set; }

        public string DisplayName { get; set; }
    }
}