# One Button Release - Implementation Plan

## Goal

Replace the current local `./scripts/release.sh` workflow with a fully automated, GitHub-native "one button" release that anyone with appropriate permissions can trigger from the GitHub UI — no local checkout, tooling, or git credentials required.

---

## Current State

### How releases work today

1. A developer runs `./scripts/release.sh <version>` from their local machine
2. `tbump` updates version strings across 16 files, commits, creates a `v<version>` tag, and pushes
3. `tbump` post-push hook runs `gh release create` to create a GitHub Release with auto-generated notes
4. The script then bumps to the next prerelease (e.g. `4.0.8rc0`), commits with `[skip ci]`, and pushes
5. GitHub Actions detects the tag and runs: checks → tests → build → publish to Dev PyPI + Public PyPI + Soda PyPI

### What works well

- `tbump.toml` config reliably updates all 16 version locations
- Staggered PyPI uploads avoid 503 errors
- Dev releases on every main push provide continuous pre-release packages
- `[skip ci]` on prerelease bump prevents infinite loops

### Pain points / risks

| Problem | Impact |
|---------|--------|
| Requires local checkout + tooling (tbump, gh, git) | Friction, bus factor |
| Tag is pushed **before** CI tests run on that tag | Broken releases can reach PyPI |
| No pre-flight checks (is main green? does version exist?) | Human error |
| No approval gate | Accidental releases |
| No structured rollback if PyPI upload partially fails | Inconsistent package state |
| No audit trail (no PR/issue associated with release) | Compliance gap |
| Script assumes specific local environment (bash, git config) | Portability issues |

---

## Proposed Solution

A single `workflow_dispatch`-triggered GitHub Actions workflow that handles the entire release lifecycle.

### User experience

1. Go to GitHub → Actions → "Release" workflow
2. Click "Run workflow"
3. Enter version (e.g. `4.0.8`) — or leave blank to auto-detect from current RC
4. Optionally select: dry run, skip approval
5. Click the green button
6. Workflow runs: validate → test → tag → build → publish → bump next version
7. Get a summary with links to the GitHub Release and published packages

---

## Implementation Steps

### Phase 1: Core workflow — `workflow_dispatch` release

**File:** `.github/workflows/release.yaml` (new)

#### Step 1.1: Workflow trigger and inputs

```yaml
on:
  workflow_dispatch:
    inputs:
      version:
        description: 'Release version (e.g. 4.0.8). Leave blank to auto-detect from current RC.'
        required: false
        type: string
      dry_run:
        description: 'Dry run — validate and test but do not publish'
        required: false
        type: boolean
        default: false
```

#### Step 1.2: Version validation job

- Parse version from input or auto-detect from `soda-core/src/soda_core/__version__.py` (strip `rcN` suffix)
- Validate format: must match `MAJOR.MINOR.PATCH`
- Check version doesn't already exist as a git tag
- Check version doesn't already exist on public PyPI (`pip index versions soda-core`)
- Fail fast with clear error messages

#### Step 1.3: Pre-flight checks job

- Verify the latest commit on `main` has passing CI (use `gh run list --branch main --status success`)
- Verify the working tree is clean (no unexpected state)
- Output a summary of what will be released: version, package count, diff since last tag

#### Step 1.4: Run full test suite

- Reuse the existing test matrix logic from `main.workflow.yaml`
- Run the same `check`, `define-test-matrix`, and `test` jobs
- This ensures we test **before** tagging, not after

#### Step 1.5: Version bump + tag + GitHub Release

- Checkout `main` at the validated commit
- Use `tbump --only-patch --non-interactive <version>` to update all version files
- Commit: `Bump to <version>`
- Create and push tag: `v<version>`
- Create GitHub Release: `gh release create v<version> --generate-notes --verify-tag`

#### Step 1.6: Build all packages

- Reuse existing matrix from `scripts/release_matrix.sh`
- For each package: `python3 -m build`
- Upload artifacts for the publish step

#### Step 1.7: Publish to registries

- Upload to Soda private PyPI (credentials from AWS Secrets Manager)
- Upload to public PyPI (using `PYPI_API_TOKEN`)
- Keep the existing staggered upload logic to avoid 503s
- Report per-package success/failure in job summary

#### Step 1.8: Bump to next prerelease

- Calculate next version (e.g. `4.0.8` → `4.0.9rc0`)
- Use `tbump --only-patch --non-interactive <next_version>`
- Commit: `[skip ci] Bump to next prerelease version <next_version>`
- Push to `main`

#### Step 1.9: Summary and notifications

- Write a GitHub Actions job summary with:
  - Released version and link to GitHub Release
  - List of published packages with PyPI links
  - Any warnings or partial failures
  - Link to the prerelease bump commit

---

### Phase 2: Safety and approval gates

#### Step 2.1: GitHub Environment with required reviewers

- Create a `production-release` GitHub Environment
- Add required reviewers (e.g. team leads, release managers)
- The publish step (1.7) uses this environment, so it pauses for approval after tests pass but before anything is published
- Reviewers see: version, test results, package list — then approve or reject

#### Step 2.2: Dry run mode

- When `dry_run: true`, run everything up to and including the build step
- Skip: tagging, publishing, version bump
- Output what **would** have happened
- Useful for validating the process before a real release

#### Step 2.3: Concurrency control

```yaml
concurrency:
  group: release
  cancel-in-progress: false
```

- Prevent concurrent releases
- Don't cancel in-progress releases (that would be dangerous)

---

### Phase 3: Robustness

#### Step 3.1: PyPI upload retry and failure handling

- Wrap twine upload in retry logic (3 attempts with exponential backoff)
- If a package fails after retries, continue with remaining packages but mark the release as partial failure
- Output a clear matrix of package → registry → status

#### Step 3.2: Rollback guidance

- If publish partially fails, output instructions for manual recovery:
  - Which packages need to be re-uploaded
  - Commands to re-run just the failed uploads
- Consider a separate `workflow_dispatch` for re-publishing a specific package

#### Step 3.3: Tag protection

- If tests fail, ensure no tag is created and no GitHub Release exists
- If publish fails, the tag and release already exist (this is intentional — the version is "claimed" even if PyPI upload needs retry)

---

### Phase 4: Polish and migration

#### Step 4.1: Auto-detect version from RC

- If no version input is provided, read current version from `__version__.py`
- If it's `X.Y.ZrcN`, propose `X.Y.Z` as the release version
- If it's already a stable version, fail with a message asking for explicit version input

#### Step 4.2: Release notes enhancement

- Auto-generate a changelog section grouping commits by type (feat/fix/chore) since last tag
- Include contributor list
- Still use `--generate-notes` as a base, but prepend structured sections

#### Step 4.3: Slack / Teams notification (optional)

- Post to a release channel on success/failure
- Include: version, link to release, link to workflow run, package count

#### Step 4.4: Deprecate `scripts/release.sh`

- Add a deprecation notice to the script that points to the GitHub Actions workflow
- Keep it functional for emergency use but log a warning
- Update any documentation referencing the script

#### Step 4.5: Update `main.workflow.yaml`

- Remove or gate the tag-triggered `release-to-pypi` job since the new workflow handles it
- Keep the `release-to-dev-pypi` job for main branch pushes (unchanged)
- Ensure no duplicate publishing if someone manually pushes a tag

---

## Dependency Graph

```
Phase 1 (Core)
  1.1 Trigger + inputs
  1.2 Version validation ──┐
  1.3 Pre-flight checks  ──┤
                            ├──→ 1.4 Test suite
                            │         │
                            │         ▼
                            ├──→ 1.5 Tag + GitHub Release
                            │         │
                            │         ▼
                            ├──→ 1.6 Build packages
                            │         │
                            │         ▼
                            ├──→ 1.7 Publish to registries
                            │         │
                            │         ▼
                            ├──→ 1.8 Bump next prerelease
                            │         │
                            │         ▼
                            └──→ 1.9 Summary

Phase 2 (Safety)                Phase 3 (Robustness)
  2.1 Approval gate               3.1 Retry logic
  2.2 Dry run mode                3.2 Rollback guidance
  2.3 Concurrency control         3.3 Tag protection

Phase 4 (Polish)
  4.1 Auto-detect version
  4.2 Release notes
  4.3 Notifications
  4.4 Deprecate old script
  4.5 Update main workflow
```

---

## Files to Create / Modify

| Action | File | Description |
|--------|------|-------------|
| **Create** | `.github/workflows/release.yaml` | New one-button release workflow |
| **Modify** | `.github/workflows/main.workflow.yaml` | Remove/gate tag-triggered release-to-pypi job |
| **Modify** | `scripts/release.sh` | Add deprecation warning, keep as fallback |
| **Modify** | `tbump.toml` | Remove `[[after_push]]` hook (GitHub Release creation moves to workflow) |
| **Create** | `scripts/validate_version.py` | Version validation + auto-detection helper |

---

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| New workflow has a bug on first real release | Test with `dry_run: true` first. Keep `release.sh` as emergency fallback. |
| GitHub Actions runner doesn't have required secrets | Validate secrets exist in the pre-flight step before running tests |
| Concurrent release attempts cause conflicts | Concurrency group prevents parallel runs |
| Tag created but publish fails | Tag is intentional (claims version). Provide retry mechanism for publish. |
| Auto-detected version is wrong | Always show the resolved version in the summary before proceeding. Approval gate catches mistakes. |

---

## Success Criteria

- [ ] A release can be performed entirely from the GitHub UI with zero local setup
- [ ] Tests run and pass **before** any tag or publish happens
- [ ] Failed tests block the release completely
- [ ] Approval gate prevents accidental releases
- [ ] Dry run mode works end-to-end without side effects
- [ ] All 13 packages publish to both PyPI registries
- [ ] Next prerelease version is bumped automatically
- [ ] GitHub Release is created with meaningful notes
- [ ] The old `release.sh` script still works as a fallback
- [ ] No duplicate publishes if both old and new paths are triggered
