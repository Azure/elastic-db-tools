// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace ElasticDapper
{
    // Let's use the standard blogging class
    public class Blog
    {
        public int BlogId { get; set; }

        public string Name { get; set; }

        public string Url { get; set; }

        public string LastReader { get; set; }
    }
}
