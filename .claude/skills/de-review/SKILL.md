---
name: de-review
description: Review a PR against this repository's engine review conventions — the invariants that must hold, the checks that matter per kind of change, and the failure shapes agent-written PRs produce here. Use when asked to review a PR or branch, or to self-check before requesting human review.
---

# DE review

The engine review for the v4 Python stack. It has a shape: a small set of things worth
blocking on, a larger set of notes that ship with the approval, and findings that name a
mechanism rather than a preference. Reproduce the shape, not just the checklist.

Do not summarise the code. Prove things about it.

## Public repositories

`soda-core` is public and a review posted there is world-visible. When reviewing it:

- Never name private repositories, internal packages, customers, or internal ticket IDs.
- Say "downstream consumers" rather than enumerating them.
- Every finding must be provable from that repository alone. If a concern depends on code
  you cannot see, say so and stop there.
- Do not introduce a reference to anything not already visible in the file under review.

## How to run

1. Get the diff and the PR description (`gh pr diff`, `gh pr view`). Review the claim, not
   only the code — an unexplained hunk is itself a finding, and a PR body that promises
   something the diff does not do is a finding too.
2. Read the existing review threads before forming findings. A finding that already has a
   thread, resolved or not, is not raised again; report which earlier findings now look
   resolved instead.
3. Classify twice: by **nature** of the change and by **area** it touches. Run only the
   lenses and passes that apply — a typo fix does not get an architectural review.
4. Open changed files at the changed line. **The diff cannot tell you which class a hunk
   lands in** — `*_data_source.py` holds both the `DataSourceImpl` and the `SqlDialect`,
   and the hunk header shows the enclosing function, never the class.
5. Deliver in the output format below: suggested verdict, findings, and an explicit list
   of what you did not check.

## Lens by nature

| Nature | What to prove |
|---|---|
| **Refactor** | Behavioural equivalence. Show the before/after of the critical path. Surface any behaviour change hiding inside the refactor. |
| **Bug fix** | The fix addresses the root cause, not the symptom; a test exists that would have caught the original; the scope is tight. |
| **New subsystem** | It sets a precedent worth copying. Judge it as a template, not only as functionality. |
| **Prototype** | It demonstrates what it claims. Flag shortcuts that mislead — hardcoded values standing in for real complexity — and anything that would be dangerous if it shipped. |
| **Dependency change** | The floor is real, not just what the lockfile resolved. CI installs the lock, so a wrong floor never fails here. |
| **Mixed** | Split into groups and say which lens applies to which files. |

## Passes by area

### Dialect / SQL
*`sql_dialect.py`, a `*_data_source.py`, anything building SQL.*

- Every dialect that needs this overrides it, and the ones inheriting the base are still
  correct. The base is the default for every warehouse nobody thought about.
- Quoting and escaping go through the dialect seam, not the call site. A hard-coded `"` is
  wrong for MySQL and SQL Server.
- Warehouse types outside the mapping are handled explicitly. `convert_table_type_to_enum`
  fails open — anything unrecognised silently becomes `TABLE`.
- If rendered SQL changed, snapshots were re-recorded rather than the job retried.
  Snapshot replay is keyed on the SQL text.
- A CI lane actually exercises this warehouse.

### Core seam
*A base class or method that subclasses override.*

- The change sits at the right altitude — base or dialect rather than copied into a leaf.
  This is the most common substantive finding in these repos.
- Every caller is identified. An override on a method with no caller changes nothing.
- Downstream consumers cannot misread the result. Older readers ignore fields they do not
  know, which can turn a stricter check into a silently weaker one.
- Nothing here duplicates a sibling connector or a base implementation.

### Check semantics
*Check types, thresholds, outcomes, contract verification.*

- Every field that makes two instances different is in `_get_id_properties()`. Identity
  keys history: re-key a check and its results are orphaned.
- The exit code is right — `1` checks failed (a success: the engine did its job), `3`
  engine or parse error, `4` results not sent.
- Nothing re-implements what `check_collections` already provides — the framework already
  gates unmeasured metrics to NOT_EVALUATED and supplies diagnostic defaults.
- The check cannot pass while reporting the wrong number.

### CI / packaging
*`.github/`, `pyproject.toml`, `uv.lock`, tbump.*

- The test is collected by a lane, and it fails when the code under test is deleted.
- The change makes a gap red, rather than only documenting it.

### Security surface
*New inputs, credentials, anything that reaches a warehouse or a log.*

- Values from a contract, a data source's own metadata, or user configuration reach SQL
  as dialect-rendered literals and quoted identifiers, never through string formatting.
- Credentials stay out of logs, error messages, and diagnostics. A connection failure
  names the host and the user, never the secret.
- Data leaving the engine (failed rows, samples, profiled values) is bounded and opt-in.
  A new path that ships user data outward is a configuration surface that reaches users.

### Outside contribution
*Author is not a member.*

- The base branch is one that can reach current users.
- The claim holds for the warehouses named, and no wider.
- No other open PR fixes the same bug at a different altitude.

## Invariants

Assert these hold; each has been broken before.

- One metric, one Measurement.
- A metric's identity contains every field that makes two metrics different.
- A check type registers both halves — YAML parser and impl — through the same registry
  the built-in check types use.
- SQL is rendered by the dialect; quoting and escaping go through its seam.
- Exit `1` is a success. The run fails when the engine could not do its job.
- Unmeasured metrics give NOT_EVALUATED, never a computed default.
- Installed means active: any package exposing a `soda.plugins.*` entry point is loaded,
  and a plugin that fails to load is only a warning.
- Warn once per cause, not once per row.
- Snapshots are keyed on SQL text.

## Failure shapes in agent-written PRs

Weight these higher when the PR looks agent-authored.

- Invents warehouse syntax that does not exist — check the function against vendor docs.
- Copies the neighbouring connector instead of using the shared abstraction.
- Overrides a plausible but wrong class; the diff cannot show you which one.
- Patches one check type where the shared derivation was the right place.
- Flips a capability flag wider than its justification, silently moving which tests run.
- Matches the strings it expected; unlisted values fail open.
- Writes tests that survive deleting the code under test, or mock away the thing under test.
- Bloats READMEs and docstrings.
- Leaves comments describing what the code used to do.

## Verdict

**Block only on what cannot be fixed after merge:** a warehouse nobody tested, a
correctness defect, a configuration surface that reaches users, a change that breaks
downstream consumers. Everything else — altitude, naming, structure, test shape — is a
note that ships with an approval.

Ask it as a rollback question. Once merged and released, can the change be reverted
without leaving something behind: re-keyed check identities with orphaned history, a
payload shape a downstream consumer already stored, snapshots re-recorded against it?
What a revert cannot undo is what blocks.

Approve-with-notes is the norm and it is the right default.

## Finding format

For each finding:

- **Claim** — what the code does.
- **Evidence** — `path:line` plus the mechanism: the input that breaks it, the dialect it
  is wrong for, the caller that would see the old shape. A finding that names a mechanism
  gets fixed; a stated preference usually does not.
- **Assumptions** — what must be true for your claim to hold.

Mark each **blocking** or **note**, and say which are objective (it is wrong) versus
judgment (I would do it differently).

End with **what I did not check**: tests you could not run, warehouses you have no access
to, behaviour you cannot determine statically. Never omit uncertainty to appear thorough —
an honest gap is worth more than a confident guess. Treat an untested changed path as a
risk, not an aside.

Do not relay a tool's finding without triaging it yourself.

## Output format

One review per run, findings only. A finding that anchors to a changed line goes inline on
that line; the rest go in the body. The body:

```
## DE review
Suggested verdict: <approve with notes | block>, <one line naming what decides it>
Earlier findings now resolved: <list, or none>

### Blocking, not anchorable
- <claim; evidence path:line and mechanism; assumptions>

### What I did not check
- <tests not run, warehouses without access, changed paths not exercised>
```

In CI the review is submitted with event COMMENT and a human sets the verdict. Locally the
same body is the report.
