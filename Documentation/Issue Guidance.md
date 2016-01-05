Issue Guidance
=============

This page outlines how the elastic database tools (EDT) team thinks about and handles issues. For us, issues on GitHub represent actionable work that should be done at some future point. It may be as simple as a small product or test bug or as large as the work tracking the design of a new feature. However, it should be work that falls under the charter of elastic database tools client libraries. We will keep issues open even if the EDT team internally has no plans to address them in an upcoming release, as long as we consider the issue to fall under our purview.

###When we close issues
As noted above, we don't close issues just because we don't plan to address them in an upcoming release. So why do we close issues? There are few major reasons:

1. Issues unrelated to elastic database tools client library. When possible, we'll try to find a better home for the issue and open it there on your behalf.

2. Cross cutting work better suited for another team. Sometimes the line between the scenarios covered by elastic database tools client library and others blurs. For some issues, we may feel that the work is better suited for some other team than us. In these cases, we'll close the issue and open it with the partner team. If they end up not deciding to take on the issue, we can reconsider it here.

3. Nebulous and Large open issues. Large open issues are sometimes better suited for [User Voice](http://feedback.azure.com/forums/217321-sql-database), especially when the work will cross the boundaries of different areas and teams. 

4. Duplicate issues to the already logged ones. If the similar issue exist already in the issue log, the new issue will be closed with the information of the existing one.

Sometimes after debate, we'll decide an issue isn't a good fit for elastic database tools client library. In that case, we'll also close it. Because of this, we ask that you don't start working on an issue until it's tagged with "up for grabs" or "feature approved". Both you and the team will be unhappy if you spend time and effort working on a change we'll ultimately be unable to take. We try to avoid that.

###Labels
We use GitHub labels on our issues in order to classify them. We have the following categories per issue:

* **Area**: These labels call out the feature areas the issue applies to. In addition to tags per feature, we have a few other tags: **Infrastructure**, for issues that relate to our build or test infrastructure, and **Meta** for issues that deal with the repository itself, the direction of the elastic database tools, our processes, etc.
	* [EDCL](https://github.com/Microsoft/Elastic-database-client-library-for-Azure-SQL-Database/labels/Area%3A%20EDCL) - Elastic database tools client library code.
	 * 	[Shard Map Management](https://github.com/Microsoft/Elastic-database-client-library-for-Azure-SQL-Database/labels/Area%3A%20Shard%20Map%20Management)
	 * 	[Multi-Shard Query](https://github.com/Microsoft/Elastic-database-client-library-for-Azure-SQL-Database/labels/Area%3A%20Multi-Shard%20Query)
	 * 	[Data Dependent Routing](https://github.com/Azure/elastic-db-tools/labels/Area%3A%20Data%20Dependent%20Routing)

	* [Split-Merge](https://github.com/Microsoft/Elastic-database-client-library-for-Azure-SQL-Database/labels/Area%3A%20Split-Merge)
	


* **Type**: These labels classify the type of issue. We use the following types: 

  * [api addition](https://github.com/Microsoft/Elastic-database-client-library-for-Azure-SQL-Database/labels/Api%20Addition): Issues which would add APIs to the assembly.
	
  * [bug](https://github.com/Microsoft/Elastic-database-client-library-for-Azure-SQL-Database/labels/Bug): Issues for bugs in the assembly.
	
  * [documentation](https://github.com/Microsoft/Elastic-database-client-library-for-Azure-SQL-Database/labels/Documentation): Issues relating to documentation (e.g. incorrect documentation, enhancement requests)
	
  * [enhancement](https://github.com/Microsoft/Elastic-database-client-library-for-Azure-SQL-Database/labels/Enhancement): Issues related to an assembly that improve it, but do not add new APIs (e.g performance improvements, code cleanup)
	
  * [test bug](https://github.com/Microsoft/Elastic-database-client-library-for-Azure-SQL-Database/labels/Test%20Bug): Issues for bugs in the tests for a specific assembly.

* **Ownership**: These labels are used to specify who owns specific issue. Issues without an ownership tag are still considered "up for discussion" and haven't been approved yet. We have the following different types of ownership: 
	
	* [up for grabs](https://github.com/Microsoft/Elastic-database-client-library-for-Azure-SQL-Database/labels/Up%20For%20Grabs): Small sections of work which we believe are well scoped. These sorts of issues are a good place to start if you are new. Anyone is free to work on these issues.
	
	* [feature approved](https://github.com/Microsoft/Elastic-database-client-library-for-Azure-SQL-Database/labels/Feature%20Approved): Larger scale issues. Like up for grabs, anyone is free to work on these issues, but they may be trickier or require more work.
	
	* [grabbed by community](https://github.com/Microsoft/Elastic-database-client-library-for-Azure-SQL-Database/labels/Grabbed%20by%20Community): Someone outside the EDT team has assumed responsibility for addressing this issue and is working on a fix. The comments for the issue will call out who is working on it. You shouldn't try to address the issue without coordinating with the owner.
	
	* [grabbed by assignee](https://github.com/Microsoft/Elastic-database-client-library-for-Azure-SQL-Database/labels/Grabbed%20by%20Assignee): Like grabbed by community, except the person the issue is assigned to is making a fix. This will be someone on the EDT team.

* **Project Management**: These labels are used to facilitate the team's Kanban Board. Labels indicate the current status and swim lane.
	
	* [Community](https://github.com/Microsoft/Elastic-database-client-library-for-Azure-SQL-Database/labels/Community): Community Engagement & Open Development Swim Lane. :swimmer: 
		
	* [Port to GitHub](https://github.com/Microsoft/Elastic-database-client-library-for-Azure-SQL-Database/labels/Port%20to%20GitHub): Swim lane :swimmer: tracking the work remaining to open source elastic database tools client library
	
	* [Infrastructure](https://github.com/Microsoft/Elastic-database-client-library-for-Azure-SQL-Database/labels/Infrastructure): Swim lane :swimmer: tracking OSS Engineering and Infrastructure
	
	* [X-Plat](https://github.com/Microsoft/Elastic-database-client-library-for-Azure-SQL-Database/labels/X-Plat): Swim lane :swimmer: for Cross Platform Support

* **Priority**: Priority for the completion of work based on the business goals.
	* [Pri 0](https://github.com/Azure/elastic-db-tools/labels/Pri%200): Needs Immediate Attention 
	* [Pri 1](https://github.com/Azure/elastic-db-tools/labels/Pri%201): Cannot release without fixing the issue
	* [Pri 2](https://github.com/Azure/elastic-db-tools/labels/Pri%202): Should be fixed in this release
	* [Pri 3](https://github.com/Azure/elastic-db-tools/labels/Pri%203): Nice to Have 
	* [Pri 4](https://github.com/Azure/elastic-db-tools/labels/Pri%204): Next Release

In addition to the above, we have a handful of other labels we use to help classify our issues. Some of these tag cross cutting concerns (e.g. cross platform, performance, serialization impact) where as others are used to help us track additional work needed before closing an issue (e.g. needs API review). Finally, we have the "needs more info" label. We use this label to mark issues where we need more information in order to proceed. Usually this will be because we can't reproduce a reported bug. We'll close these issues after a little bit if we haven't gotten actionable information, but we welcome folks who have acquired more information to reopen the issue.

###Assignee
We assign each issue to a EDT team member. In most cases, the assignee will not be the one who ultimately fixes the issue (that only happens in the case where the issue is tagged "grabbed by assignee"). The purpose of the assignee is to act as a point of contact between the EDT team and the community for the issue and make sure it's driven to resolution. If you're working on an issue and get stuck, please reach out to the assignee (just at mention them) and they will work to help you out.

###Kanban Board
[https://waffle.io/Azure/elastic-db-tools](https://waffle.io/Azure/elastic-db-tools)
