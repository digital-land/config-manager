# Design: stale add-data confirmation causing duplicate entity numbers

Related: [digital-land/config#2751](https://github.com/digital-land/config/issues/2751)

Status: **Option B implemented** (config-manager) — see [Implementation](#implementation-option-b-shipped)

## Problem statement

The add-data flow makes a point-in-time assessment of the shared `config-manager-update` branch,
but nothing invalidates or warns about that assessment if the branch advances before the user
confirms. Confirmation blindly replays the frozen result onto whatever the branch looks like now.

(See the linked issue for the incident that surfaced this — a transform page left open ~24 hours
while another submission advanced the branch, resulting in entity `5110315` assigned twice.)

Entity numbers are the primary keys of the platform. A duplicate assignment means two real-world
things share one identity. Detection today happens late: the PR tests flag the duplicate only
after it has already been committed to the shared branch, and a dev then has to manually unpick
the PR.

## How the flow works today

The race spans three repos. There are three phases, and the entity numbers assigned in phase 1
are trusted verbatim in phase 3.

### Phase 1 — assessment (async-request-backend)

When the user submits the add-data form, config-manager creates an async request
(`_submit_add_data_preview`, [controllers/form.py](../../application/blueprints/datamanager/controllers/form.py))
whose params include `github_branch` (a branch *name*, default `config-manager-update`) — never a
commit SHA.

The request processor (`add_data_workflow`, `request-processor/src/application/core/workflow.py`)
downloads `lookup.csv`, `entity-organisation.csv` etc. over HTTP from the **mutable ref**
`raw.githubusercontent.com/digital-land/config/refs/heads/{github_branch}/pipeline/{collection}/…`
(`workflow.py:728`). New entity numbers are assigned in `_assign_entries`
(`request-processor/src/application/core/pipeline.py:188`) as `max_entity + 1` over that snapshot,
and frozen into the stored response as `pipeline-summary.new-entities`.

**No commit SHA is recorded anywhere** — not in the request params, not in the response.

### Phase 2 — review and confirm (config-manager)

The transform/preview pages re-fetch the stored result on each load
([services/async_api.py](../../application/blueprints/datamanager/services/async_api.py)) and
display the assigned entity numbers. The page can sit open indefinitely — `RequestMeta`
([db/models.py](../../application/db/models.py)) stores only `request_id` and
`endpoints_to_retire`; there is no timestamp, no expiry, no branch-state check. The only lock in
the system is the manual, whole-service `ServiceLock`, which does not prevent this race.

On confirm, `handle_add_data_confirm`
([controllers/preview.py](../../application/blueprints/datamanager/controllers/preview.py)) fires
a GitHub `repository_dispatch` (`trigger_add_data_async_workflow`,
[services/github.py](../../application/blueprints/datamanager/services/github.py)) carrying only
`request_id`, `branch`, `triggered_by` and `retire_endpoints`. Fire-and-forget.

### Phase 3 — commit (config repo)

`.github/workflows/add-data-async-script.yml` receives the dispatch and runs `bin/add_data.py`,
which re-fetches the stored async response by `request_id`, checks out the **current** HEAD of
`config-manager-update`, and `append_lookup` (`bin/add_data.py:303`) appends the frozen entity
numbers verbatim to `pipeline/{collection}/lookup.csv`. There is no max-entity re-check, no
duplicate detection, and the workflow has **no `concurrency:` group**, so two confirmations can
also push concurrently.

The `config-manager-update → main` PR is auto-merged (`auto-merge-config-manager.yml`). The PR
tests do flag duplicate entity numbers — but only once the bad commit is already on the shared
branch, at which point a dev has to step in and manually unpick the PR (untangling the offending
rows from any other submissions that have landed since).

## Options

### Option A — stale timeout

Expire the transform result after ~1 hour (timestamp on `RequestMeta`; confirm route rejects and
prompts a re-run once expired).

**Pros**

- Trivial to implement; config-manager only.
- Reduces the *probability* of the race by shrinking the window.

**Cons**

- Blunt instrument — it doesn't test the actual failure condition:
  - **False positives:** the branch may not have changed in 24 hours; the user is forced to
    re-run for nothing.
  - **False negatives:** two submissions minutes apart still collide. The race exists at *any*
    interval; an hour just makes it rarer.
- Introduces an arbitrary constant the team will argue about and eventually tune reactively.

**User message:** "This result has expired. Re-run the transform to continue." — easy to write,
but doesn't tell the user anything true about *why*.

### Option B — confirm-time branch check in config-manager

Capture the `config-manager-update` HEAD SHA when the assessment is submitted
(GitHub API `GET /repos/digital-land/config/branches/config-manager-update`), store it on
`RequestMeta`. When the user confirms, fetch the current HEAD and compare. If the branch has
advanced — ideally scoped to commits touching `pipeline/{collection}/`, so commits to *other*
collections don't block — refuse the confirm and prompt a re-run.

How the collection scoping works: one call to the compare API,
`GET /repos/digital-land/config/compare/{assessed_sha}...config-manager-update`, whose `files[]`
array lists every file changed between the two points. Block the confirm only if a changed
filename starts with `pipeline/{collection}/`. Fail closed (treat as changed) if the diff is
truncated (`files` is capped at 300 entries) or the compare status is `diverged` (force push).
If in practice this still blocks people too often, the escalation is a blob-SHA check: fetch the
git blob `sha` of `lookup.csv` (and the other CSVs the assessment read) at both refs via the
contents API and compare — provably safe to confirm if the files are byte-identical, at the cost
of a couple of calls per file.

Reducing baseline skew: the baseline SHA is read by config-manager at submission time, slightly
before the async worker actually fetches the CSVs. If an add-data workflow is mid-push to the
branch at that moment, the baseline would capture a commit the worker hasn't seen yet, producing
a spurious re-run prompt at confirm. To avoid this, at submission we first check the GitHub
Actions API for an active `add-data-async-script` run and, if one is in flight, wait (bounded) for
it to finish before reading HEAD. This lands the baseline on the settled post-workflow state that
the worker will read. The wait is a no-op in the common case (no workflow running) and capped so
it never hangs the submit request; on timeout we proceed and rely on the confirm-time check.

**Pros**

- Targets the actual failure condition: only blocks when the branch really changed.
- Best UX of all the options — the user is told *before* anything is committed, on the page they
  are already on, with an accurate reason and a clear action (re-run).
- Single-repo change (config-manager + one migration).
- Collection scoping means unrelated submissions don't interfere.

**Cons**

- **TOCTOU window remains.** The check happens when the user clicks confirm, but the commit
  happens later in a GitHub Action. Two users confirming near-simultaneously both pass the check
  and both commit. The window shrinks from hours to seconds, but it is not closed.
- The SHA is captured by config-manager at *submission* time, not by the worker at *fetch* time —
  a small skew window (seconds) between the two. Largely mitigated by the workflow-wait above,
  but not eliminated (a workflow could still start just after we read HEAD).
- Adds GitHub API calls to the submit and confirm paths (rate limits, auth failures to handle),
  and the submission-time wait briefly occupies a web worker when a workflow is in flight.

**User message:** "The configuration branch has changed since this transform was run
(new data was added for this collection). Re-run the transform to get up-to-date entity numbers."

### Option C — SHA in the dispatch payload; GitHub Action enforces

This is "Tech Approach 1" from the ticket. Pass the assessed-against SHA through the
`repository_dispatch` `client_payload`; `bin/add_data.py` compares it against the HEAD it checks
out and **fails cleanly** (no commit, no push) if the branch has moved.

The strongest variant: async-request-backend resolves the branch HEAD *first* and fetches the
CSVs **pinned at that SHA** (`refs/heads/{branch}` → the resolved commit URL), recording the SHA
in the stored response. This puts the SHA where the truth is (the process that actually read the
files), and as a side benefit fixes a latent torn-read bug — today the several CSVs are fetched
in separate HTTP calls against a moving ref and could straddle a commit.

**Pros**

- Server-side enforcement at the last possible moment — closes Option B's TOCTOU window. Even
  two simultaneous confirmations can't both land (the second finds HEAD moved).
- Enforced regardless of which client triggers the dispatch (protects future callers, manual
  `workflow_dispatch` runs, etc.).
- SHA-pinned fetching makes the assessment internally consistent.

**Cons**

- Three repos touched (async-request-backend, config-manager pass-through, config script).
- **The rejection happens *after* the user confirmed.** Getting that feedback back to the UI is
  awkward: config-manager would have to poll the workflow run, or the user sees a success page
  followed by a silently-failed commit. On its own, the UX is worse than B.
- Strict SHA equality is coarse: *any* commit to the branch (even another collection) rejects the
  submission. Scoping to collection-touching commits is possible in the script (`git diff
  --name-only <sha>..HEAD`) but adds logic.

#### Option C2 — auto re-run on rejection

Extension of C: when the Action rejects, config-manager (or the Action itself) re-submits the
add_data job against the new HEAD and retries the commit automatically.

**Cons (why this is flagged as risky)**

- **Thundering herd.** If N users add data to the same collection concurrently, each hits the
  barrier in turn: N submissions serialize into N−1 automatic re-runs of the full transform. With
  4 people adding brownfield-land at once, one "wins" and 3 jobs re-run — possibly repeatedly.
- **It silently changes what the user reviewed.** The re-run re-transforms and assigns *different*
  entity numbers than the ones shown on the preview page the user confirmed. If the preview step
  exists so a human vouches for the result, committing something else defeats it. (If the team
  decides the specific numbers are *not* part of what users review, Option D below is the more
  honest version of that decision.)
- Re-run loops need retry limits, failure surfacing, and cost real pipeline time.

### Option D — re-derive entity numbers at commit time

Make the commit step correct by construction: `bin/add_data.py` re-computes `max_entity` from the
freshly checked-out `lookup.csv` and renumbers the `new-entities` rows before appending. Pair it
with a dedupe on `prefix + reference + organisation` so the same record submitted twice (the
other face of this race) is dropped rather than double-added.

**Pros**

- Eliminates the number-collision class entirely, with no user interaction, no timeout, no retry
  loop. The commit is idempotent with respect to the branch advancing.
- Concurrent submissions to the same collection all succeed (given a `concurrency:` group so the
  re-derivations themselves serialize — see defence-in-depth below).

**Cons**

- The entity numbers shown on the preview page are no longer necessarily what gets committed.
  This is a product question the team must answer: **is the number itself part of what the user
  reviews, or only the mapping/facts?** If numbers must be stable from preview to commit, D is
  ruled out as the sole fix.
- Duplicates the assignment logic outside `digital_land.pipeline.Lookups` unless the Action
  installs digital-land-python — divergence risk between the two implementations.
- Doesn't catch *semantic* conflicts (the same real-world entity submitted twice under different
  references) — though nothing else on this list does either.

### Option E — per-collection lock

Extend the existing `ServiceLock` model to per-collection locks, auto-acquired when an
assessment starts and released on confirm or expiry. Only one in-flight add-data per collection.

**Pros**

- Prevents the race by construction for same-collection concurrency within config-manager.
- Conceptually simple to explain: "someone else is adding data to this collection right now."

**Cons**

- A lock held by an idle browser tab blocks every other user of that collection — this incident's
  tab sat open for 24 hours. So the lock needs an expiry, which reintroduces Option A's arbitrary
  timeout, now with the added failure mode of a lock expiring *under* a user who then confirms
  anyway.
- Doesn't protect against commits to `config-manager-update` from outside config-manager
  (batch-assign, manual pushes).
- Lock lifecycle edge cases (crashed jobs, abandoned sessions, admin overrides) are a permanent
  maintenance tax.

## Comparison

| | A: timeout | B: confirm check | C: Action enforces | C2: auto re-run | D: re-derive | E: lock |
|---|---|---|---|---|---|---|
| Targets actual failure condition | ✗ | ✓ | ✓ | ✓ | ✓ | partly |
| Closes the TOCTOU window fully | ✗ | ✗ | ✓ | ✓ | ✓ | ✗ |
| UX when it triggers | forced re-run, no reason | clear, pre-commit, actionable | post-confirm failure, hard to surface | silent substitution | invisible (just works) | blocked entry |
| Commits only what the user reviewed | ✓ | ✓ | ✓ | ✗ | ✗ (numbers may shift) | ✓ |
| Concurrent users, same collection | still collide | last to confirm re-runs | last to confirm fails, re-runs | N−1 auto re-runs | all succeed | serialized at entry |
| Repos touched | 1 | 1 | 3 | 3 | 1–2 | 1 |
| Effort | XS | S | M | L | M | M |

## Recommendation

**Do Option B now, with the compare-API collection scoping. Option C is a worthwhile follow-up,
not part of the initial fix.**

- **B is the fix to ship.** It is a single-repo change, catches the overwhelmingly common case
  (the stale tab left open for hours), and gives the user an accurate, actionable message
  *before* anything is committed. The residual gap — the seconds-wide window between the
  confirm-click check and the Action committing — is a different order of magnitude from the
  hours-wide window that caused the incident, and we don't need millisecond-level guarantees to
  solve the actual problem.
- **C later, as hardening.** Recording the SHA in async-request-backend (and pinning the CSV
  fetches to it) and having `bin/add_data.py` refuse to push onto a moved HEAD would close the
  residual window and protect non-config-manager dispatchers. It touches three repos and its
  post-confirm failure UX only becomes acceptable once B has made it a rare last line of
  defence — so it earns its place as a follow-up ticket, not a blocker on B.

A is rejected: it doesn't test the failure condition and both false-positives and false-negatives.
E is rejected: idle-tab locks punish other users and still need the timeout it was meant to avoid.
C2 is rejected as specified: it commits something other than what the user reviewed and herds
under concurrency.

**D is worth keeping on the table as the long-term direction** — it is the only option where
concurrent submissions all just work. Adopting it is a product decision: it requires agreeing
that the specific entity numbers on the preview page are informational, not part of what the user
signs off. If the team reaches that agreement, D can replace C's "reject" with "re-number", and B
becomes optional polish.

## Implementation (Option B, shipped)

Option B is implemented in config-manager. The pieces:

- **Baseline storage.** New nullable `branch_sha` column on `RequestMeta`
  (`application/db/models.py`), migration `e5f6a7b8c9d0_add_request_meta_branch_sha.py`.
- **Capture at submission.** `record_branch_baseline(request_id, github_branch)` in
  `controllers/preview.py` reads the branch HEAD and stores it, called from both submission
  sites: `_submit_add_data_preview` (`controllers/form.py`) and `_submit_assign_entities_request`
  (`controllers/flagged_resources.py`). No-op for brand-new-branch submissions; fails **open**
  (logs and skips) so it can never block a submission.
- **Workflow-wait before capture.** `wait_for_add_data_workflow_idle()` +
  `add_data_workflow_running()` (`services/github.py`) poll the GitHub Actions API for an active
  `add-data-async-script` run and wait (bounded) before reading HEAD, reusing one installation
  token across polls. Bounds: `ADD_DATA_WORKFLOW_WAIT_TIMEOUT` (default 60s) and
  `ADD_DATA_WORKFLOW_POLL_INTERVAL` (default 5s) in `config/config.py`; gunicorn `--timeout`
  raised to 120s in the `Procfile` so the worker survives the wait.
- **Confirm-time check.** `get_branch_head_sha` + `config_branch_changed_for_collection`
  (`services/github.py`) run in `handle_add_data_confirm` (`controllers/preview.py`) before the
  workflow is triggered. The compare call is scoped to `pipeline/{collection}/` and **fails
  closed** on a diverged history, a truncated (≥300-file) diff, or any API error. When it blocks,
  the user gets `templates/datamanager/add-data-stale.html` with a "Re-run transform" action.
- **Tests.** `tests/unit/blueprints/datamanager/services/test_github.py` covers the HEAD read,
  the collection-scoped compare (changed / other-collection / diverged / truncated / API-error),
  the workflow-running detection, and the bounded wait loop.

Both fail-safe directions are deliberate and opposite: baseline capture **fails open** (never
block a submission over an infrastructure hiccup), while the confirm-time check **fails closed**
(never let a possibly-stale result through). Correctness lives in the confirm check; the
submission-side pieces exist only to keep the baseline accurate and cut false-positive re-runs.

## Defence-in-depth (do these regardless of the option chosen)

Neither of these fixes the race; both would have caught or contained this incident, and both are
cheap:

1. **Fail before the push, not on the PR.** The PR tests already flag duplicate entity numbers,
   but only after the bad commit is on the shared branch — leaving a dev to unpick the PR by
   hand. Running the same duplicate check in `bin/add_data.py` *before* it commits and pushes
   would turn "corrupted shared branch + manual clean-up" into "one failed workflow run, branch
   untouched". Same detection, moved earlier, no unpicking.
2. **`concurrency:` group on `add-data-async-script.yml`** (keyed per branch, or per
   branch+collection). Today two confirmation workflows can check out, append, and push
   concurrently — a second, independent way to corrupt the branch that none of options A/B/E
   address.

## Open questions for the discussion

1. **Message wording.** When a confirm is blocked (Option B), what exactly do we tell the user?
   Proposed: name the collection, say the configuration changed since their transform ran, and
   present a single "Re-run transform" action. Do we show *what* changed (the other PR/commit)?
2. **Check scope.** Any commit to the branch, or only commits touching the same collection's
   pipeline files? (Recommendation assumes collection-scoped; strict is simpler but blocks more.)
3. **Is this partly a usage problem?** A transform left open for 24 hours suggests the flow
   invites drive-by abandonment. Should the UI discourage it (banner after N minutes, auto-refresh
   of the preview) independent of the correctness fix?
4. **Are entity numbers part of the review?** This decides whether Option D is available as the
   end-state. If numbers are informational, D is the cleanest destination.
5. **Follow-up scheduling.** B ships on its own. Do we raise the Option C hardening (and/or the
   defence-in-depth items) as tickets now, and who owns the async-request-backend and config-repo
   pieces when we do?
