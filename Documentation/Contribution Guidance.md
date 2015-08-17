Contributing to EDT 
====================

The EDT team maintains several guidelines for contributing to the EDT repos, which are provided below. Many of these are straightforward, while others may seem subjective. An EDT team member will be happy to explain why a guideline is defined as it is.

Contribution Guidelines
=======================
* [Contribution Bar](#contribution-bar) describes the bar that the team uses to accept changes.
* [Contributing Ports](#contributing-ports) describes the slightly different bar for contributing changes that enable EDT to work on other OSes and chips.
* [General Contribution Guidance](#general-contribution-guidance) describes general contribution guidance, including more subjective stylistic guidelines.
* [DOs and DON'Ts](#dos-and-donts) provides a partial checklist summary of contributing guidelines, in classic framework guidelines do/don't style.
* [Contributor License Agreement (CLA)](#contributor-license-agreement) describes the requirement and process of signing a Contributor License Agreement (CLA).
* [Contribution Workflow](Contribution Workflow.md) describes the workflow that the team uses for considering and accepting changes.
	
Contribution "Bar"
==================
Project maintainers will merge changes that align with project priorities and/or improve the product significantly for a broad set of apps. 
Maintainers will not merge changes that have narrowly-defined benefits, due to compatibility risk. The EDT codebase is used by several Microsoft products (e.g. SQL Server). Changes to the open source codebase can become part of these products, but are first reviewed and tested to ensure they are correct for those products and will not inadvertently break applications. We may revert changes if they are found to be breaking.

Contributing Ports
==================
We encourage ports of EDT and Split-Merge to other platforms. Ports have a weaker contribution bar, since they do not contribute to compatibility risk with existing Microsoft products on Windows. For ports, we are primarily looking for functionally correct implementations.

General Contribution Guidance
=============================
There are several issues to keep in mind when making a change.
Managed Code Compatibility
--------------------------
Please review Breaking Changes before making changes to managed code. Please pay the most attention to changes that affect the Public Contract.
Typos
-----
Typos are embarrassing! We will accept most PRs that fix typos. In order to make it easier to review your PR, please focus on a given component with your fixes or on one type of typo across the entire repository. If it's going to take >30 mins to review your PR, then we will probably ask you to chunk it up.
Commit Messages
---------------
Please format commit messages as follows (based on this [excellent post](http://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html)):
```
Summarize change in 50 characters or less.

Provide more detail after the first line. Leave one blank line below the
summary and wrap all lines at 72 characters or less.

If the change fixes an issue, leave another blank line after the final
paragraph and indicate which issue is fixed in the specific format
below.

Fix #42
```

Also do your best to factor commits appropriately, i.e. not too large with unrelated things in the same commit, and not too small with the same small change applied N times in N different commits. If there was some accidental reformatting or whitespace changes during the course of your commits, please rebase them away before submitting the PR.

DOs and DON'Ts
==============
* **DO** follow our coding style (C# code-specific)
* **DO** give priority to the current style of the project or file you're changing even if it diverges from the general guidelines.
* **DO** include tests when adding new features. When fixing bugs, start with adding a test that highlights how the current behavior is broken.
* **DO** keep the discussions focused. When a new or related topic comes up it's often better to create new issue than to side track the discussion.
* **DO** blog and tweet (or whatever) about your contributions, frequently!
* **DON'T** send PRs for style changes. 
* **DON'T** surprise us with big pull requests. Instead, file an issue and start a discussion so we can agree on a direction before you invest a large amount of time.
* **DON'T** commit code that you didn't write. If you find code that you think is a good fit to add, file an issue and start a discussion before proceeding.
* **DON'T** submit PRs that alter licensing related files or headers. If you believe there's a problem with them, file an issue and we'll be happy to discuss it.
* **DON'T** add API additions without filing an issue and discussing with us first. See API Review Process.

Contributor License Agreement
=============================
You must sign a .NET Foundation Contribution License Agreement (CLA) before your PR will be merged. This a one-time requirement for projects in EDT. You can read more about [Contribution License Agreements (CLA)](https://en.wikipedia.org/wiki/Contributor_License_Agreement) on Wikipedia.
However, you don't have to do this up-front. You can simply clone, fork, and submit your pull-request as usual.
When your pull-request is created, it is classified by a CLA bot. If the change is trivial, i.e. you just fixed a typo, then the PR is labelled with cla-not-required. Otherwise it's classified as cla-required. In that case, the system will also tell you how you can sign the CLA. Once you signed a CLA, the current and all future pull-requests will be labelled as cla-signed.
Signing the CLA might sound scary but it's actually super simple and can be done in less than a minute.
