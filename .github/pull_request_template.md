## Description

<!-- What does this PR change and why? Link the related issue if there is one. -->

Fixes #

## Checklist

- [ ] Tests added or updated to cover the change
- [ ] Documentation updated where relevant
- [ ] PR title follows the conventional commit format (e.g. `fix(sqlserver): ...`)

## Review checklist

<!-- Keep the sections that apply to this change and delete the rest.
     The full version, with the reasoning behind each line, is the `de-review`
     skill: .claude/skills/de-review/SKILL.md -->

**Dialect or SQL rendering**
- [ ] Every dialect that needs this overrides it; the ones inheriting the base are still correct
- [ ] Quoting and escaping go through the dialect seam, not the call site
- [ ] Warehouse types outside the mapping are handled explicitly, not defaulted to `TABLE`
- [ ] Rendered SQL changed → snapshots re-recorded, not the job retried
- [ ] A CI lane actually exercises this warehouse

**Core seam or base class**
- [ ] The change sits at the right altitude — base or dialect, not copied into a leaf
- [ ] Every caller is identified
- [ ] Downstream consumers cannot misread the result
- [ ] Nothing here duplicates a sibling connector or a base implementation

**Check semantics and evaluation**
- [ ] Every field that makes two instances different is in `_get_id_properties()`
- [ ] The exit code is right: `1` checks failed, `3` engine error, `4` results not sent
- [ ] Nothing re-implements what `check_collections` already provides
- [ ] The check cannot pass while reporting the wrong number

**CI, workflow, packaging**
- [ ] The test is collected by a lane, and fails when the code under test is deleted
- [ ] The dependency floor is real, not just what the lockfile resolved
- [ ] The change makes a gap red, rather than only documenting it
