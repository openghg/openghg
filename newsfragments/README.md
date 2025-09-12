# News Fragments

This directory contains "news fragments" which are short files that contain a small **feature** or **change** announcement that will be included in the next `CHANGELOG.md` when a release is made.

The files are organized by type (the file extension) and reference an issue or PR number:

## Fragment Types

- `feature` - for new features (maps to "Added" in CHANGELOG)
- `update` - for updates/changes (maps to "Updated" in CHANGELOG)  
- `bugfix` - for bug fixes (maps to "Fixed" in CHANGELOG)
- `doc` - for documentation changes
- `removal` - for removed features
- `misc` - for miscellaneous changes

## Naming Convention

Files should be named `{issue_or_pr_number}.{type}`, for example:

- `1234.feature` - for a new feature related to issue #1234
- `5678.bugfix` - for a bug fix related to PR #5678
- `9012.update` - for an update related to issue #9012

## Example Usage

When you contribute a bug fix on issue 1234, create a file:

```bash
echo "Fixed that bug that was causing crashes!" > newsfragments/1234.bugfix
```

When you add a new feature from PR 5678:

```bash  
echo "Added support for new data format" > newsfragments/5678.feature
```

## Building the Changelog

To generate the changelog (usually done at release time):

```bash
towncrier build --version=X.Y.Z
```

This will collect all the fragments and add them to the CHANGELOG.md file.