<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

Apache Hadoop JDK 25 Migration
==============================

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

Purpose
-------

This document is the migration record for making the `hadoop-common-project`
module tree build and run correctly on JDK 25 (LTS) while remaining fully
supported on JDK 17. It exists because the migration required a small number of
judgement calls where the correct behaviour was ambiguous, and every such
resolution has to be written down rather than left implicit in a diff.

The page carries three deliverables that are part of the change itself, not
commentary on it:

* the **static audit** of SecurityManager-era API usage, in both its before and after states
* the **flaky-test exclusion justification list**, which records that no test was excluded and proves why none could be
* the **behavioural resolution register**, which records each ambiguous decision and the reasoning that settled it

It also root-causes the environmental test failures observed during
verification, so that a reader who reproduces them can tell immediately that
they are not migration regressions.

### What this change is, and what it is not

**The module tree already compiled and largely passed its tests on JDK 25
before this change was made.** Measured on the base commit with the corrected
thirteen-module reactor invocation given under
[Build and Verification Environment](#Build_and_Verification_Environment):
`BUILD SUCCESS`, exit code 0, **zero `[ERROR]` lines**, 02:24 min against a cold
local repository — a warm one completes the same thirteen modules in well under
a minute.

This change is therefore **not** a compilation fix, and it should not be read
as one. It does two things:

* **(a) It purges SecurityManager-era API usage.** JEP 486 permanently disabled the Security Manager in JDK 24, and JDK 25 inherits that state. The API that remains is deprecated for removal and already semantically inert, so the usage is removed by inlining rather than by reimplementation.
* **(b) It closes a silent `javax.security.auth.Subject` propagation hole.** `Subject.callAs`, the replacement for the terminally deprecated `Subject.doAs`, establishes the current subject in a way a pool worker thread does not see. On the runtime this change is verified against, Temurin **25.0.4+7**, it does so with a `ScopedValue` binding — read from that release's own `java.base` sources, not inferred — and such a binding is confined to the dynamic extent of the call on the binding thread. Whatever mechanism any other release uses, the measured JDK 25 outcome is what matters here: code that previously ran as an authenticated user now runs with no subject at all — and it compiles cleanly, throws nothing, and logs nothing.

Defect (b) is the reason this work matters. No compiler diagnoses it and no
pre-existing test detects it. It surfaces later as an authorization denial, an
anonymous-principal audit record, or a `NullPointerException` raised far from
its cause.

### The shape of the fix

A single utility, `org.apache.hadoop.util.concurrent.SubjectPreservingTasks`,
captures `SubjectUtil.current()` **at task submission** — in the decorator's
constructor, which runs on the submitting thread — and re-establishes it inside
the worker. Its surface is exactly `wrap(Runnable)`, `wrap(Callable<T>)` and
`unwrap(Runnable)`, backed by two private static final nested decorators and
nothing besides.

Three properties of that utility are load-bearing:

* **Re-establishment uses `SubjectUtil.doAs`, never `SubjectUtil.callAs`.** `Subject.callAs` is specified to wrap an escaping exception in a `CompletionException`. `SubjectUtil.doAs` unwraps that and rethrows the original cause, which preserves exception identity for `FutureTask`, for every `Future.get()` caller, and for the JDK-8071638 diagnostic in `ExecutorHelper`. Using `callAs` would change the observed type of every propagated exception.
* **Two fast paths hand the task back unchanged.** The first is `SubjectUtil.THREAD_INHERITS_SUBJECT`, which is `true` on every runtime up to and including JDK 21. Such a runtime carries a creator's subject across a thread boundary itself, so the utility stays out of its way entirely: `wrap` returns its argument, and this change costs that runtime nothing whatsoever — no allocation, no extra frame, and behaviour bit-identical to the base commit. JDK 17 support is therefore preserved *by construction* rather than by testing and hoping. The second is a subject read as `null` at submission. Establishing an absent identity is not the same as leaving one out: `Subject.callAs` null-checks only its action, so a `null` subject is bound, and a binding of nothing would mask an identity in force where the task runs. What each fast path means for what a task actually observes is measured and recorded under [Behavioural Resolution Register](#Behavioural_Resolution_Register).
* **A `null` task, and a task this utility already prepared, are returned unchanged too.** The first keeps a `null` rejected by its destination executor exactly as it always was; the second keeps a task an executor is offered a second time — as `ThreadPoolExecutor.DiscardOldestPolicy` offers it — at one layer rather than letting it gain a second, so that a single `unwrap(Runnable)` always reaches the task as submitted.

The capture point is the substance of the fix. The idiom this change replaces
captured the subject inside `ThreadFactory.newThread`, which runs **once per
worker thread**. In any pool that reuses threads, every task after the first
therefore executed under the *first* submitter's identity. That is a security
defect, not merely a correctness one — privilege confusion with real
authorization and audit consequences — and it is invisible to the obvious test,
because a test that submits from a single identity and asserts the subject is
visible **passes** against the broken implementation. Exposing it requires two
submitters on a thread-reusing pool, which is why the new
`TestExecutorSubjectPropagation` includes an explicit thread-reuse case, and
why that case states its expectation in terms of
`SubjectUtil.THREAD_INHERITS_SUBJECT` rather than asserting one outcome for
both runtimes: on JDK 25 the second submitter's task is required to observe the
second submitter and demonstrably not the first, while on a runtime that hands a
new worker its creator's identity the same task keeps observing that creator,
which is exactly what it observed before this change.

A `ThreadFactory`-based fix was impossible rather than merely inferior. All
**9** `new ThreadFactoryBuilder()` sites in this module tree use the shaded
Guava builder
(`org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder`,
supplied by `hadoop-shaded-guava` — `hadoop-thirdparty-guava.version` at
`hadoop-project/pom.xml:L108-110`, artifact at `:L275-277`), whose `build()`
emits a plain `java.lang.Thread` that Hadoop can neither subclass nor
intercept.

Scope and Non-Goals
-------------------

### In scope

The thirteen modules reachable from `hadoop-common-project`, principally
`hadoop-auth`, `hadoop-common`, `hadoop-nfs`, `hadoop-kms` and
`hadoop-registry`. Within them: removal of SecurityManager-era API usage,
introduction of the centralized submission-time propagation seam, redirection
of executor construction through Hadoop's own instrumented factories,
replacement of the deprecated-for-removal `Subject.doAs` call sites in
`hadoop-auth` with the existing `SubjectUtil` bridge, and the **three** mandated
regression tests for the propagation defect.

Those three are the whole of the test change: no pre-existing test file is
touched, no existing assertion is altered, and no test is added anywhere else.
The change set is exactly **39 paths** — 7 added and 32 updated — of which 34
are main source, 3 are the new tests, 1 is this page and 1 is `BUILDING.txt`.
Coverage that would have required a fourteenth test file, or a fifteenth, is
recorded instead as a deliberate deferral under
[Other deliberate deferrals under R1](#Other_deliberate_deferrals_under_R1),
because adding it would have taken the change past the 39 paths above.

### Explicit non-goals

* **No behaviour change outside the identity a pooled task runs under.** No RPC wire format, token serialization layout, delegation-token semantic, Kerberos or JAAS authentication outcome, log output format, configuration schema, or public signature in `org.apache.hadoop.security.*` changes. What does change is the one thing this work exists to change, and only on a runtime that no longer carries a subject across a thread boundary: on JDK 25, a task submitted into a Hadoop-owned pool now runs under its own submitter's identity instead of under none, so work that would have been authorized and audited with no subject at all is authorized and audited as the user that asked for it. That correction is the purpose of the change, and entry (v) of the [Behavioural Resolution Register](#Behavioural_Resolution_Register) states what each runtime does.
* **No dependency or build-plugin version change.** See [Governing Constraints](#Governing_Constraints) and the version table under [Build and Verification Environment](#Build_and_Verification_Environment).
* **No JVM launch-argument change.** No `--add-opens` and no `--add-exports` is added.
* **No test is disabled, excluded, weakened or tagged flaky.** See the [Flaky-Test Exclusion Justification List](#Flaky-Test_Exclusion_Justification_List).
* **No new configuration key.** The seam reads the submitting thread's own subject, so there is no switch to configure and nothing to add to `core-default.xml`.
* **No package move, class split, or API reshaping.** Three classes are added to an existing package; nothing is relocated.
* **No native code change.** All C, C++, JNI and CMake sources are out of scope, which is the direct cause of one documented test failure.
* **No cross-module edit.** `hadoop-hdfs-project`, `hadoop-yarn-project`, `hadoop-mapreduce-project` and `hadoop-tools` are untouched. A cross-module edit would have been permissible only if compilation strictly broke, and it does not.
* **No opportunistic cleanup.** Incidental warnings and pre-existing defects are catalogued below and deliberately left alone.

### Supported runtimes

This change is verified on **JDK 17 and JDK 25**, and on those two runtimes
only.

On JDK 25 it supplies an identity the runtime no longer carries across a thread
boundary. On JDK 17 it supplies nothing, because the runtime still carries it:
`SubjectUtil.THREAD_INHERITS_SUBJECT` is `true` there, so `wrap` returns its
argument and the compiled behaviour of every pool is what it was at the base
commit — no allocation, no extra frame, nothing to regress. That is why JDK 17
compatibility is a property of the design rather than a hope resting on a test
run.

The consequence for the tests is that they assert an **outcome** rather than a
mechanism, so that one assertion holds on both runtimes wherever the outcome is
genuinely the same: a task submitted into a pool whose worker did not exist
before the submission observes its submitter on JDK 17 because the runtime gave
the new worker that identity, and on JDK 25 because the utility established it.
Two cases are the exception, because their observable outcome genuinely differs
between the two runtimes — a worker that has already served one submitter and
then serves another, and a submission carrying no identity at all. Each of those
states its expectation per runtime, and each is measured and explained under
[Behavioural Resolution Register](#Behavioural_Resolution_Register).

Governing Constraints
---------------------

### Provenance of these constraints

**There is no separate rules document in this repository.** The seven binding
constraints recorded below were set out with the requirements for this
migration; they are summarized here, rather than transcribed, so that a reader
of the change can see what governed it without leaving the tree. Their not being
under version control is no licence to lower the bar: standard Apache Hadoop
documentation and engineering practice applies on top of these seven.

### What each constraint requires

* **R1 — Traceability.** Every hunk must trace to a concrete JDK 25 incompatibility or to the Subject-propagation defect; no opportunistic cleanup, refactoring or modernization may ride along. R1 is largely *subtractive*: it is the reason a long list of defensible improvements was deliberately **not** made, and each of those is catalogued below as a deferral rather than left to look like an oversight.
* **R2 — No blanket `--add-opens` / `--add-exports`** introduced to suppress a problem rather than fix it. Honoured by verification rather than by editing: zero JVM flags were added, and none is needed.
* **R3 — No reflection shim that keeps SecurityManager-era code paths alive.** No such shim exists: every SecurityManager-era site is removed outright rather than guarded, and **no reflective construct is introduced anywhere** — not in production code and not in test code — nor is any pre-existing one altered, including in the several touched files that are reflection-based by nature. The one reflective construct in the design reflects over the *replacement* API, which is a materially different thing — see Conflict 1 under [Documented Conflict Resolutions](#Documented_Conflict_Resolutions). The whole of what the diff contains on this point is registered under [R3 in practice - no new reflection](#R3_in_practice_-_no_new_reflection).
* **R4 — No new dependency for security-context handling**; only the JDK and existing Hadoop utilities. Zero dependencies were added, removed or re-versioned.
* **R5 — No disabling or excluding failing tests**, except tests documented as flaky at baseline, each with a written justification. The baseline flaky set is provably empty, so no exclusion was permissible and none was made.
* **R6 — Logging output formats and configuration file schemas must not change.** This is the constraint with the largest design consequence: it is what turned a plausible implementation into a correct one, by forcing the unwrap obligations described below.
* **R7 — Where correct behaviour is ambiguous, pre-JDK-18 behaviour at the base commit is the tie-breaker, and each resolution must be documented.** Its documentation clause is why this page exists at all, and its register is the [Behavioural Resolution Register](#Behavioural_Resolution_Register) — though the page is not that register alone: it also carries the [Static Audit of SecurityManager-Era API](#Static_Audit_of_SecurityManager-Era_API) required of this change, the [Flaky-Test Exclusion Justification List](#Flaky-Test_Exclusion_Justification_List) required by R5, and the [Documented Conflict Resolutions](#Documented_Conflict_Resolutions).

### R3 in practice - no new reflection

R3 forbids a reflection-based shim that keeps a SecurityManager-era code path
alive. This change introduces **no reflective construct at all** — not in
production code and not in test code — so the constraint is met with room to
spare. Because that is an easy claim to make and a tedious one to check, the
whole of what the diff contains on the point is registered here.

**What production contains.** Of the 34 main-source files this change touches,
**not one gains, loses or alters a reflective construct**. Several of them are
reflection-based by nature and were so before this change — `util/CleanerUtil.java`
and `hadoop-auth`'s `SubjectUtil` (whose change here is javadoc only) build
`MethodHandle`s, while `util/dynamic/DynMethods.java`,
`util/dynamic/DynConstructors.java` and `io/FastByteComparisons.java` reach for
declared members — which is no coincidence, since reflection is exactly what the
removed privileged blocks were guarding. The diff can mislead on this point if
read alone: two lines in `io/FastByteComparisons.java`
(`Unsafe.class.getDeclaredField("theUnsafe")` and the `setAccessible` beside it)
and one line in `hadoop-auth`'s `util/PlatformName.java` appear on its added
side, yet the identical text appears on the removed side at greater indentation,
because deleting the block that wrapped the body dedented it — and the
`PlatformName` line is a comment mentioning `Class.forName`, not a call at all.
The genuinely reflective construct in the design — `SubjectUtil`'s
`MethodHandle` bridge — predates this change, and is discussed as the single
audit exemption under
[Static Audit of SecurityManager-Era API](#Static_Audit_of_SecurityManager-Era_API).

**What the tests contain.** The change adds three test files and modifies none.
Searched for `setAccessible`, `getDeclaredField`, `getDeclaredMethod`,
`getMethod`, `getDeclaredConstructor`, `Class.forName`, `java.lang.reflect` and
`MethodHandle`, all three return **zero** matches. Every fact they assert is
read through published API: `SubjectUtil.current()` for the identity in force
inside a task, `UserGroupInformation.getCurrentUser()` for the user that work
would be authorized and audited as, `Thread.currentThread()` for the worker that
ran it, `Future` results and exceptions for what a caller is handed, and the
pools' own `org.slf4j` output — captured with the pre-existing
`GenericTestUtils.LogCapturer` — for what an operator would read.

**Why production visibility was not widened instead.** Adding an accessor, or
relaxing a modifier, purely so that a test could observe something would be a
production hunk tracing to neither a JDK 25 incompatibility nor the
Subject-propagation defect — precisely what R1 forbids — and it would enlarge
the published surface of production classes for the benefit of test code. No
such hunk exists: the published surface of every touched class is exactly what
it was at the base commit.

**What none of it does.** No construct in any of these files names
`SecurityManager`, `AccessControlContext`, `AccessController` or
`java.security.Policy`, and none re-enters a removed path or keeps one
reachable. The static audit therefore stands exactly as recorded under
[Static Audit of SecurityManager-Era API](#Static_Audit_of_SecurityManager-Era_API).

**What this costs, stated plainly.** Several of the inlined bodies are private
and are not reached through published state on the runtimes this project builds
with, so removing the privileged block around them leaves them without direct
coverage. Reaching them would take either reflection, which R3's spirit and this
section's own claim both argue against, or a widened production surface, which
R1 forbids, or test files beyond the 39 this change touches. That is
recorded as a deliberate deferral rather than dressed up as coverage — see
**Coverage of inlined private bodies** under
[Other deliberate deferrals under R1](#Other_deliberate_deferrals_under_R1).


### R6 in practice: the unwrap obligations

A task decorator changes the runtime type of the object the pool sees. Left
unaddressed, that silently changes operator-visible output — which R6 forbids
outright. Three obligations follow, and all three were found by reading the
code rather than by reasoning from the API:

* **`ExecutorHelper` fidelity site 1** — the `LOG.debug("afterExecute in thread: " … ", runnable type: " + r.getClass().getName())` statement at `ExecutorHelper.java:L37-38` as it stood at the base commit. Without unwrapping, every DEBUG line would have named the decorator instead of the real task type.
* **`ExecutorHelper` fidelity site 2** — the JDK-8071638 guard `if (t == null && r instanceof Future<?> && ((Future<?>) r).isDone())` at `ExecutorHelper.java:L46` at the base commit, preceded by its `// Handle JDK-8071638` comment. A decorator is a `Runnable`, not a `Future`, so this condition would have evaluated `false` for every pooled task, the `((Future<?>) r).get()` that follows would never have run, and the `LOG.warn("Caught exception in thread {}  + : ", …)` further down — whose doubled space is preserved verbatim — would have **silently stopped reporting task exceptions**. That is a regression with no compiler signal and no failing test.
* **Both `beforeExecute` overrides** — `HadoopThreadPoolExecutor.java:L82-83` and `HadoopScheduledThreadPoolExecutor.java:L61-62` at the base commit, byte-identical text logging `r.getClass().getName()`.

Two points are worth recording precisely, because both are easy to overlook and
expensive to rediscover:

* **`ExecutorHelper` has two fidelity sites, not one.** The `instanceof Future<?>` guard is the conspicuous one; the debug line that names the task's type is the other, and a fix that addressed only the guard would still have altered operator-visible output. A single reassignment at the top of `logThrowableFromAfterExecute` covers both at once, which is how it is implemented: `r = SubjectPreservingTasks.unwrap(r);` at `ExecutorHelper.java:L36`, with the two sites at `:L38` and `:L47`.
* **R6 is the *only* reason `ExecutorHelper.java` is in scope at all.** Nothing in the functional requirements would have identified it as needing a change.

One further R6 consequence: `HadoopExecutors.shutdown(...)` logs the executor
*object* itself (`"Gracefully shutting down executor service {}. Waiting max
{} {}"`, at `HadoopExecutors.java:L118-119` at the base commit). The two new
forwarding executor-service types satisfy that by construction, because they
extend the shaded Guava `ForwardingExecutorService`, whose `ForwardingObject`
superclass renders `delegate().toString()` and which neither new type overrides.
This line therefore continues to render the delegate rather than the wrapper.
Operator-visible thread names are likewise frozen — for example
`UserGroupInformation.java:L936` at the base commit sets
`"TGT Renewer for " + userName`, and that string is untouched.

Nothing further was needed, and nothing further was added. In particular the two
task decorators do **not** override `toString()`: no site in this module tree's
main source interpolates a task's own string form. The one rejection handler in
main source, at `util/BlockingThreadPoolExecutorService.java:L140-141`
(base-commit `:L142-143`), logs `executor.toString()` — the pool, not the task —
and every site that does name a task calls `unwrap(Runnable)` first, which is the
whole of the obligation above.
Adding a delegating `toString()` for a caller that does not exist would have
been surface with nothing behind it.

There is no third obligation, because nothing in the seam interposes on the
future a caller is handed. `newTaskFor` is deliberately not overridden, so a
submission for a result still yields `java.util.concurrent.FutureTask` and the
DEBUG line still names exactly what it named at the base commit. The
consequences of *not* interposing there are not silently dropped: they are
recorded as a residual risk under
[Behavioural Resolution Register](#Behavioural_Resolution_Register).

### R6 in practice: the frozen configuration artifacts

No configuration key was introduced, and no configuration schema or log format
changed. The seam reads the submitting thread's own subject rather than a
property, so there is nothing to add to `core-default.xml`. The following
artifacts were verified byte-for-byte unchanged against the base commit; their
exact sizes are recorded so the check is repeatable:

| Artifact (under `hadoop-common-project/hadoop-common/`) | Bytes |
|---|---|
| `src/main/resources/core-default.xml` | 160654 |
| `src/main/conf/log4j.properties` | 14451 |
| `src/main/conf/hadoop-policy.xml` | 14007 |
| `src/main/conf/hadoop-metrics2.properties` | 3321 |
| `src/test/resources/log4j.properties` | 940 |
| `src/main/conf/core-site.xml` | 774 |
| `src/main/conf/ssl-client.xml.example` | 2316 |
| `src/main/conf/ssl-server.xml.example` | 3766 |
| `src/main/conf/workers` | 10 |

`src/main/conf/hadoop-env.sh` (16741 bytes) is frozen and unchanged on the same
basis.

### R2 in practice: zero JVM-flag changes

`hadoop_finalize_jpms_opts` in
`hadoop-common-project/hadoop-common/src/main/bin/hadoop-functions.sh` is
byte-identical to the base commit: the function opens at `:L1578`,
`-XX:+IgnoreUnrecognizedVMOptions` is at `:L1580`, **eleven pre-existing
`--add-opens` entries occupy `:L1581-L1591`** (`java.base/` to `java.io`,
`java.lang`, `java.lang.reflect`, `java.math`, `java.net`, `java.text`,
`java.util`, `java.util.concurrent`, `java.util.zip`, `sun.security.util` and
`sun.security.x509`, each `=ALL-UNNAMED`), `--enable-native-access=ALL-UNNAMED`
is at `:L1592`, and the function closes at `:L1593`. Those eleven entries are
pre-existing and untouched; none was added by this change.

That file honours the directive at `hadoop-project/pom.xml:L168-170`, which
states that its flag list must be kept in sync with the POM's test arguments.
The two flag-bearing files therefore change together or not at all, and this
change chooses **not at all**. The measured symmetry holds: **11**
`--add-opens` occurrences in `hadoop-functions.sh` and **11** in
`hadoop-project/pom.xml`, whose `<extraJavaTestArgs>` block begins at `:L171`
and is consumed by the `maven-surefire-plugin.argLine` property at `:L187`.

Across the source, script and POM files of `hadoop-common-project/`, the root
`pom.xml` and `hadoop-project/pom.xml`, the following counts were measured, all
**zero**: `--add-exports`, `-Djava.security.manager`, `-Djava.security.policy`
and `--enable-preview`. The implicit requirement to remove SecurityManager
launcher arguments was therefore **already satisfied at the base commit**, and
R2 is honoured by verifying that rather than by editing anything. No
`--add-opens` is needed by the design in any case, because `SubjectUtil`'s
`MethodHandle` lookup targets `javax.security.auth.Subject` — an exported
public API in `java.base`.

One caveat for anyone re-running those counts: **this page is itself the only
file under `hadoop-common-project/` in which those four flag names appear**, as
prose naming what is absent rather than as configuration. Exclude
`src/site/markdown/JDK25Migration.md` from the search, or restrict it to
`*.java`, `*.sh` and `pom.xml`, and every count is zero. The same caution
applies to the SecurityManager-era audit below: this document discusses the API
it removed, so it must never be included in the audit surface, which is
`hadoop-common-project/*/src/main/java` only.

Static Audit of SecurityManager-Era API
---------------------------------------

This section is the mandated static-audit deliverable. The acceptance criterion
is that the audit yields **zero hits in main source outside documented shims**.

The audit command is:

```
grep -rE "SecurityManager|AccessControlContext|AccessController\.|java\.security\.Policy" \
     hadoop-common-project/*/src/main/java
```

### Baseline state at the base commit

**28 matching lines across 10 files** — 2 files in `hadoop-auth` and 8 in
`hadoop-common`. Line numbers in this table are those of the base commit, since
most of the lines they identify no longer exist.

One file is the documented compatibility shim and is exempt; it is listed first
and is discussed in full below.

| File | Hits | Line(s) at the base commit |
|---|---|---|
| `hadoop-auth/.../security/authentication/util/SubjectUtil.java` *(exempt shim)* | 9 | L100, L105, L154, L162, L179, L191, L200, L211, L215 |

The remaining nine files were driven to zero by this change:

| File | Hits | Line(s) at the base commit |
|---|---|---|
| `hadoop-common/.../util/concurrent/SubjectInheritingThread.java` | 10 | L31, L80, L81, L83, L130, L131, L133, L148, L149, L151 — all javadoc, e.g. `{@linkplain SecurityManager#getThreadGroup …}` |
| `hadoop-common/.../util/CleanerUtil.java` | 2 | L76 `final Object hack = AccessController.doPrivileged(`, L175 `final Throwable error = AccessController.doPrivileged(` |
| `hadoop-common/.../io/FastByteComparisons.java` | 1 | L140 `theUnsafe = (Unsafe) AccessController.doPrivileged(` |
| `hadoop-common/.../metrics2/impl/MetricsConfig.java` | 1 | L25 `import static java.security.AccessController.*;` |
| `hadoop-common/.../security/authorize/PolicyProvider.java` | 1 | L20 `import java.security.Policy;` |
| `hadoop-common/.../util/BlockingThreadPoolExecutorService.java` | 1 | L62 `SecurityManager s = System.getSecurityManager();` |
| `hadoop-common/.../util/dynamic/DynConstructors.java` | 1 | L196 `AccessController.doPrivileged(new MakeAccessible(hidden));` |
| `hadoop-common/.../util/dynamic/DynMethods.java` | 1 | L431 `AccessController.doPrivileged(new MakeAccessible(hidden));` |
| `hadoop-auth/.../org/apache/hadoop/util/PlatformName.java` | 1 | L92 `return AccessController.doPrivileged((PrivilegedAction<Boolean>) () -> {`, in `isSystemClassAvailable` |

Note that `PlatformName` sits in package `org.apache.hadoop.util`, not in
`org.apache.hadoop.security.authentication.util` where the rest of `hadoop-auth`
keeps its utilities. A search that assumes the latter finds nine of these ten
files; the count above is ten.

### Post-change state, and the single exemption

The audit now returns **9 matching lines in exactly 1 file**:
`hadoop-auth/src/main/java/org/apache/hadoop/security/authentication/util/SubjectUtil.java`.
**That file is the sole documented-shim exemption from the zero-hit
criterion**, and the exemption is recorded in its own javadoc so that anyone
running the audit finds the explanation next to the code.

Because `SubjectUtil` received a javadoc-only update in this change, it grew
from 410 to 518 lines and its residual hits moved. Both sets of line numbers
are given, so the audit is reproducible against either state:

| Nature | Post-change line | Base-commit line | Content |
|---|---|---|---|
| comment | L178 | L100 | `// For 22 and 23 the behavior actually depends on whether the SecurityManager` |
| comment | L183 | L105 | `// SecurityManager warnings to the console.` |
| javadoc | L232 | L154 | `* a method handle for Subject.getSubject(AccessController.getContext())` |
| comment | L240 | L162 | `// Subject.getSubject(AccessControlContext) is deprecated for removal and` |
| javadoc | L257 | L179 | `* Look up the method handle for Subject#getSubject(AccessControlContext)` |
| string literal | L269 | L191 | `.loadClass("java.security.AccessControlContext");` |
| javadoc | L278 | L200 | `* Look up the method handle for AccessController.getAccessControlContext()` |
| comment | L289 | L211 | `// AccessController.` |
| string literal | L293 | L215 | `.loadClass("java.security.AccessControlContext");` |

That is **7 comment or javadoc lines and 2 string-literal lines**. A **third**
`loadClass` literal — `.loadClass("java.security.AccessController");`, at
post-change `:L291` and base-commit `:L213` — names an SecurityManager-era
class as a string but **does not match the audit regex**, because no `.`
follows `AccessController` inside the quotes. It is called out explicitly here
so that nobody reconciling the count concludes a line was overlooked.

**There is no compile-time reference to any SecurityManager-era type anywhere
in `SubjectUtil`.** Both class names are resolved purely by name, inside the
pre-JDK-18 fallback helpers `lookupGetSubject()` and `lookupGetContext()`
(base-commit `:L187-197` and `:L208-223`), and that fallback is reached only
when `Subject.current()` is absent, as determined by `lookupCurrent()`
(base-commit `:L159-176`).

The reason it qualifies for exemption is developed as Conflict 1 under
[Documented Conflict Resolutions](#Documented_Conflict_Resolutions), and
reduces to this: the shim reflects over the **replacement** API, not the
removed one. It resolves `Subject.callAs` at base-commit `:L74-87` via
`lookup.findStatic(Subject.class, "callAs", MethodType.methodType(Object.class,
Subject.class, Callable.class))`, it preserves no SecurityManager-era code
path, and it exists solely because `maven.compiler.release` is 17.

### Companion check: the plain import form

`import java.security.AccessController;` has no `.` after `AccessController`,
so it is **invisible** to the audit regex above. A companion check is therefore
required for the audit to mean what it appears to mean:

```
grep -rn "java.security.AccessController\|java.security.Policy" \
     hadoop-common-project/hadoop-common/src/main/java/
```

**Fifteen import lines were deleted from `hadoop-common`: 12 primary deletions
plus 3 consequential ones.** Both figures are given because a bare count of 12
does not reconcile against the enumeration. Line numbers are those of the base
commit, as the lines no longer exist.

**12 primary deletions** — the imported type is removed or redirected:

* `java.security.AccessController` — 4 sites: `io/FastByteComparisons.java:22`, `util/dynamic/DynMethods.java:25`, `util/dynamic/DynConstructors.java:24`, `util/CleanerUtil.java:26`
* `import static java.security.AccessController.*;` — 1 site: `metrics2/impl/MetricsConfig.java:25`
* `java.security.Policy` — 1 site: `security/authorize/PolicyProvider.java:20`
* `java.util.concurrent.Executors` — 5 sites: `security/UserGroupInformation.java:52`, `ha/ZKFailoverController.java:26`, `fs/FileUtil.java:53`, `metrics2/lib/MutableQuantiles.java:25`, `metrics2/lib/MutableRollingAverages.java:27`
* `java.util.concurrent.ScheduledThreadPoolExecutor` — 1 site: `ipc/Server.java:69`

**3 consequential deletions** — `java.security.PrivilegedAction` became unused
once the action body was inlined: `io/FastByteComparisons.java:23`,
`metrics2/impl/MetricsConfig.java:28`, `util/CleanerUtil.java:27`.

**`hadoop-auth` accounts separately for 2 deletions and 1 addition.** Both
deletions are in `org/apache/hadoop/util/PlatformName.java`, which carried
`import java.security.AccessController;` at base-commit `:L21` and `import
java.security.PrivilegedAction;` at `:L22`, immediately above `import
java.util.Arrays;` at `:L23`. Inlining the `(PrivilegedAction<Boolean>) () ->
{…}` body in `isSystemClassAvailable` left **both** unreferenced, so both went;
`java.util.Arrays` stays, since `hasIbmTechnologyEditionModules()` still uses it.
The single addition is `import
org.apache.hadoop.security.authentication.util.SubjectUtil;` at
`server/KerberosAuthenticationHandler.java:22`. Its counterpart
`client/KerberosAuthenticator.java` needed none: it already imported
`SubjectUtil` at base-commit `:L22`, so redirecting its `Subject.doAs` call left
its import block untouched.

### Retention rules

A naive import sweep would break compilation. These retentions are deliberate,
and the line numbers here are **post-change**, because the imports still exist:

* `java.util.concurrent.Executors` is **retained** in `util/concurrent/HadoopExecutors.java:24` and in the three Netty-hosting files `oncrpc/SimpleTcpServer.java:21`, `oncrpc/SimpleUdpServer.java:21` and `portmap/Portmap.java:22`.
* `java.util.concurrent.ScheduledThreadPoolExecutor` is **retained** in `util/concurrent/HadoopScheduledThreadPoolExecutor.java:29`, which extends it.
* `java.util.concurrent.ThreadPoolExecutor` is **retained** in `io/ReadaheadPool.java` and `fs/shell/CopyCommandWithMultiThread.java`, whose nested policy classes `ThreadPoolExecutor.DiscardOldestPolicy` and `CallerRunsPolicy` still resolve through it, and in `util/BlockingThreadPoolExecutorService.java`, where it is a field type.
* `javax.security.auth.Subject` is **retained** in both `hadoop-auth` Kerberos files. Only the call target moved to `SubjectUtil`; the type is still named.
* `java.security.PrivilegedAction` is **retained** in `util/dynamic/DynMethods.java:25` and `util/dynamic/DynConstructors.java:24` — the `MakeAccessible implements PrivilegedAction<Void>` class at `DynMethods.java:529` is deliberately kept and simply invoked directly — and also in `ha/ZKFailoverController.java:22`, `security/SecurityUtil.java:28`, `security/UserGroupInformation.java:37`, `util/Daemon.java:21` and `util/concurrent/SubjectInheritingThread.java:21`.

### Why the removals are provable no-ops

JEP 486 specifies four behaviours that make the removals safe by definition
rather than by inspection. On JDK 24 and later:

* `System.getSecurityManager()` returns `null` unconditionally.
* `AccessController.doPrivileged` **"executes the given action immediately"**.
* `Subject.getSubject(AccessControlContext)` throws `UnsupportedOperationException` unconditionally.
* Attempting to enable a Security Manager at startup produces an unsuppressible `java.lang.Error` during `System.initPhase3`.

Each was confirmed empirically on Eclipse Temurin 25.0.4+7:
`System.getSecurityManager()` returned `null`;
`AccessController.doPrivileged` executed normally; `Subject.doAs` succeeded;
and `Subject.getSubject(...)` threw
`java.lang.UnsupportedOperationException: getSubject is not supported`.

Inlining a privileged action therefore reproduces exactly what the JVM already
does, and collapsing the
`System.getSecurityManager() != null ? s.getThreadGroup() :
Thread.currentThread().getThreadGroup()` idiom to its second branch is exact,
because the condition is now permanently false.

**All seven `doPrivileged` sites used the unchecked `PrivilegedAction` form.
Zero used `PrivilegedExceptionAction` and zero involved
`PrivilegedActionException`.** Consequently **no `throws` clause changed and no
`catch` block became unreachable** anywhere in this change. In particular:

* `catch (SecurityException | NoSuchMethodException e)` in `DynMethods` and `DynConstructors` remains valid, because `Class.getDeclaredMethod` and `getDeclaredConstructor` still declare `SecurityException`.
* `SecurityException` does **not** match the audit regex, which is also why the three `@throws SecurityException` javadoc entries in `SubjectInheritingThread` legitimately remain.
* `UserGroupInformation`'s `PrivilegedActionException` catch chain is unaffected, because it handles exceptions from `UGI.doAs` — a path this change does not touch.

Finally, `java.security.PrivilegedAction` and `PrivilegedExceptionAction` are
**not** removed by JEP 486 and remain in wide legitimate use — **52**
occurrences of the two types in `hadoop-common` main source alone, **17** of
`PrivilegedAction` across 8 files and **35** of `PrivilegedExceptionAction`
across 13, including the `@InterfaceAudience.Public` signatures
`UserGroupInformation.doAs(PrivilegedAction<T>)` at `:L1936` and
`doAs(PrivilegedExceptionAction<T>)` at `:L1954`, both preserved unchanged.
Their continued presence is correct and expected, and is **not** an audit
failure.

Count these two type names on a word boundary. A plain substring search for
`PrivilegedAction` returns 26 in the same tree, because it also matches the 6
occurrences of `PrivilegedActionException` and the 3 of the private helper
`tracePrivilegedAction`; neither is one of the two action types, so that 26
overstates `PrivilegedAction` by nine.

Flaky-Test Exclusion Justification List
---------------------------------------

This section is the mandated exclusion-justification deliverable. Its content
is short, because the honest answer is short.

**Zero tests were excluded, disabled, or tagged flaky by this change.** No
justification list of excluded tests can be produced, because there are no
exclusions to justify — and the reason none is permissible is itself worth
recording.

### The baseline flaky set is provably empty

R5 permits an exclusion only for a test documented as flaky *at baseline*. In
`hadoop-project/pom.xml` the exclusion machinery is inert:

* `:L40` — `<test.exclude>_</test.exclude>`, a literal placeholder underscore
* `:L41` — `<test.exclude.pattern>_</test.exclude.pattern>`, likewise
* `:L2592-2594` — the surefire `<includes>` block, containing only `**/Test*.java`
* `:L2595-2599` — the surefire `<excludes>` block, containing only `**/${test.exclude}.java` (`:L2596`), `${test.exclude.pattern}` (`:L2597`) and `**/Test*$*.java` (`:L2598`)

The last of those is an **inner-class filter, not a test suppression**. Since
the two placeholder properties expand to a literal underscore, the baseline
excludes nothing. The flaky baseline is therefore empty, no exclusion is
permissible under R5, and none was made: **zero new `@Disabled`, zero new
`<exclude>`, zero new `@Tag("flaky")`**.

### Zero new `@FlakyTest` applications

The `@FlakyTest` mechanism exists and must not be newly applied. Precisely:

* it is **defined** at `src/test/java/org/apache/hadoop/test/tags/FlakyTest.java`, where the annotation is declared at `:L43` and carries `@Tag("flaky")` at `:L41`
* there is **one pre-existing application**, at `src/test/java/org/apache/hadoop/fs/contract/AbstractContractUnbufferTest.java:30` (the import) and `:43` (`@FlakyTest("buffer underflow")`)

That single pre-existing application is left exactly as found. The accurate
statement is **zero *new* `@FlakyTest` applications**, not "zero usages".

### Pre-existing `@Disabled` annotations, untouched

**28** `@Disabled` annotations exist across exactly 6 files. All are left
exactly as found; the census is recorded so that any future change to the
count is visible:

| File (under `hadoop-common/src/test/java/org/apache/hadoop/`) | `@Disabled` |
|---|---|
| `crypto/TestCryptoStreamsNormal.java` | 12 |
| `crypto/TestCryptoStreamsForLocalFS.java` | 9 |
| `fs/TestSymlinkLocalFSFileSystem.java` | 4 |
| `conf/TestConfigurationFieldsBase.java` | 1 |
| `fs/TestFileUtil.java` | 1 |
| `metrics2/lib/TestMetricsRegistry.java` | 1 |

### Tag definitions carry no applications

All **5** `@Tag(` occurrences in `hadoop-common` test source are tag
*definitions* inside `src/test/java/org/apache/hadoop/test/tags/`:
`FlakyTest.java:41` (`@Tag("flaky")`), `RootFilesystemTest.java:38`
(`@Tag("rootfilesystem")`), `ScaleTest.java:37` (`@Tag("scale")`),
`LoadTest.java:42` (`@Tag("load")`) and `IntegrationTest.java:49`
(`@Tag("integration")`). None is applied to a test.

### No conditional-skip mechanism either

R5's intent extends past the exclusion configuration. A runtime guard is a
*de facto* exclusion, so none was added: the three new suites contain no
`@EnabledOnJre`, `@DisabledOnJre`, `@EnabledForJreRange`, `@EnabledIf`,
`@DisabledIf`, `assumeTrue`, `assumingThat` or `@Disabled`. **Every one of their
71 tests runs and asserts on both JDK 17 and JDK 25**; not one is skipped,
short-circuited or reduced to a no-op on either runtime.

That is a stronger statement than "the same assertions everywhere", and the
difference matters. Wherever the observable outcome is genuinely the same on
both runtimes, a single assertion covers both, which is most of the suite and is
what makes it meaningful: it asserts propagation *outcomes*, not the mechanism
that produces them. In the two cases where the outcome genuinely differs — a
worker that has already served one submitter and then serves another, and a
submission carrying no identity at all — the test asserts the outcome *for the
runtime it is running on*, branching on
`SubjectUtil.THREAD_INHERITS_SUBJECT` and asserting fully in both branches. That
is the opposite of a skip: it is what stops the divergence being hidden, and both
branches are exercised, since verification runs the whole suite under both
runtimes. Each of those two cases is measured and explained under
[Behavioural Resolution Register](#Behavioural_Resolution_Register).

The observed test failures are handled the only way R5 allows — by root-cause
analysis and documentation. See
[Environmental Test Failures](#Environmental_Test_Failures).

Behavioural Resolution Register
-------------------------------

This register exists because R7 requires every ambiguous behavioural decision
to be documented. **Ten** judgement calls arose, and each was resolved
**conservatively toward pre-JDK-18 semantics**, which is the tie-breaker R7
prescribes.

The first four are sites deliberately left alone. The last six are different
in kind: they are outcomes of the design as specified — what a pooled task
observes on each runtime, and five consequences of the design's deliberate
narrowness. They are recorded here because they were **measured** rather
than assumed, and because in every case the measurement shows the outcome is no
worse than the base commit on either runtime. Where a resolution turns on the
identity fast paths, the reasoning behind those fast paths is developed under
[Conflict 5](#Conflict_5_-_The_identity_fast_paths_versus_the_per-submitter_guarantee).

### (i) Netty accept and I/O event-loop groups left non-propagating

Four event-loop groups are constructed with an explicit cached thread pool and
were deliberately **not** routed through the propagation seam. All four are in
`hadoop-common` itself:

* `hadoop-common/src/main/java/org/apache/hadoop/oncrpc/SimpleTcpServer.java:68` — `workerGroup = new NioEventLoopGroup(workerCount, Executors.newCachedThreadPool());`
* `hadoop-common/src/main/java/org/apache/hadoop/oncrpc/SimpleUdpServer.java:61` — `workerGroup = new NioEventLoopGroup(workerCount, Executors.newCachedThreadPool());`
* `hadoop-common/src/main/java/org/apache/hadoop/portmap/Portmap.java:112` — `workerGroup = new NioEventLoopGroup(0, Executors.newCachedThreadPool());`
* `hadoop-common/src/main/java/org/apache/hadoop/portmap/Portmap.java:130` — `udpGroup = new NioEventLoopGroup(0, Executors.newCachedThreadPool());`

Three further no-argument groups have the same disposition and are likewise
untouched: `oncrpc/SimpleTcpClient.java:70`, `oncrpc/SimpleTcpServer.java:67`
and `portmap/Portmap.java:111`, each `new NioEventLoopGroup();`.

**Resolution and justification.** Accept and I/O event loops read bytes off the
wire; they carry no caller identity to propagate. On pre-JDK-18 runtimes they
carried none either, so leaving them non-propagating preserves the behaviour
the tie-breaker protects, while wrapping them would add per-task cost for no
semantic gain. Recorded as an accepted residual risk.

For the avoidance of doubt about where these sites live: they are in
`hadoop-common`, at the line numbers above, and not in the NFS module their
subject matter suggests. The `hadoop-common-project/hadoop-nfs` tree contains no
`Executors.new*` and no `EventLoopGroup` construction at all — it holds only
`mount/` and `nfs/` packages.

### (ii) `RolloverSignerSecretProvider` cannot reach the centralized utility

`hadoop-auth/src/main/java/org/apache/hadoop/security/authentication/util/RolloverSignerSecretProvider.java:95`
holds `scheduler = Executors.newSingleThreadScheduledExecutor();`, inside
`protected synchronized void startScheduler(long initialDelay, long period)`
declared at `:L92`, with `scheduler.scheduleAtFixedRate(new Runnable() {`
following at `:L96`. It **cannot** be redirected, because the module dependency
direction is one-way:

* `hadoop-common-project/hadoop-common/pom.xml` declares `hadoop-auth` at **compile scope** (block `:L236-240`, with the `<artifactId>` at `:L238` and `<scope>compile</scope>` at `:L239`) and additionally consumes its **test-jar** (block `:L241-246`, `<artifactId>` at `:L243`, `<type>test-jar</type>` at `:L244`, `<scope>test</scope>` at `:L245`)
* `hadoop-common-project/hadoop-auth/pom.xml` contains **zero** references to `hadoop-common`
* the reactor order confirms it: MiniKDC [7], Auth [8], Auth Examples [9], Common [10]

**Resolution and justification.** `org.apache.hadoop.util.concurrent.*` lives
in `hadoop-common` and is therefore unreachable from `hadoop-auth`. Closing
this gap would require relocating the utility into `hadoop-auth` — a
restructuring R1 forbids, and one that would expand the change far beyond the
defect. The site is left as-is and recorded as a known structural residual
risk. Anyone tempted to simply add the import will hit a reactor-ordering
failure.

### (iii) `FindClass` protection-domain lookup left in place

`hadoop-common/src/main/java/org/apache/hadoop/util/FindClass.java:268` holds
`CodeSource source = clazz.getProtectionDomain().getCodeSource();`, inside
`private void loadedClass(String name, Class clazz)` spanning `:L266-271`
(`out("Loaded %s as %s", name, clazz);` at `:L267`, `URL url =
source.getLocation();` at `:L269`, `out("%s: %s", name, url);` at `:L270`).

**Resolution and justification.** This is a **class-location diagnostic, not a
permission check**, and JEP 486 does not degrade it: `getProtectionDomain()`
and `getCodeSource()` behave identically on pre-JDK-18 runtimes, on JDK 17 and
on JDK 25. It is a justified non-change. It also does not match the audit
regex, so it has no bearing on the zero-hit criterion.

### (iv) Retained `sun.misc.Unsafe` usages left untouched

`hadoop-common/src/main/java/org/apache/hadoop/io/FastByteComparisons.java` and
`.../io/nativeio/NativeIO.java` both emit terminal-deprecation warnings for
`sun.misc.Unsafe` on JDK 25.

**Resolution and justification.** Remediating them is modernization that traces
to neither a JDK 25 incompatibility nor the Subject-propagation defect, so R1
forbids it here; native code is out of scope besides. These are **catalogued
and deliberately deferred**, not overlooked. `FastByteComparisons` is edited by
this change only to inline its privileged action, and its `Unsafe` usage is
left exactly as found.

### (v) What a pooled task observes, per runtime

The two identity fast paths are implemented exactly as the design specifies:
`wrap` hands its argument straight back when `SubjectUtil.THREAD_INHERITS_SUBJECT`
is `true`, and again when the subject read at submission is `null`. Each leaves an
outcome that differs between the two supported runtimes, so each is recorded here
with the measurement that established it. Temurin 17.0.20+8 and Temurin 25.0.4+7,
one-worker `HadoopThreadPoolExecutor`, worker created by the first submission:

| What is submitted, and by whom | JDK 17 (`THREAD_INHERITS_SUBJECT` `true`) | JDK 25 (`false`) | JDK 25 at the base commit |
|:---|:---|:---|:---|
| First submission, by `alice` | `alice` | `alice` | **nothing at all** |
| Later submission by `bob`, worker reused | **`alice`** | **`bob`** | **nothing at all** |
| Later submission carrying no identity | **`alice`** | **nothing at all** | nothing at all |

Every cell of the JDK 17 column is also the JDK 17 behaviour of the base commit,
measured there and unchanged by this work. The middle column is what this change
delivers; the right-hand column is the defect it closes.

**Resolution and justification, first fast path.** On JDK 17 a pooled task
observes the identity in force when its *worker* was created rather than that of
its own submitter, because `wrap` returns the task untouched and the runtime does
the propagating itself. R7 designates pre-JDK-18 behaviour at the base commit as
the tie-breaker, so here the conservative resolution and the specified one
coincide: the fast path is what keeps JDK 17 bit-identical, and bit-identical is
what R7 asks for. On JDK 25 the flag is `false`, every submission is prepared,
and the per-submitter guarantee holds strictly — `bob`'s task observes `bob` on
the worker `alice` created.

The practical reading for an operator is that JDK 25 is the runtime on which a
Hadoop-owned pool may be shared across identities with the guarantee in force,
and that JDK 17 behaves exactly as it always has.
`TestExecutorSubjectPropagation` states its reuse expectation in terms of the
flag for this reason, and both of its branches run and assert on both runtimes.

**Resolution and justification, second fast path.** A submission carrying no
identity is handed over unchanged, so it observes whatever its worker holds: the
worker's creation-time identity on JDK 17, and **nothing** on JDK 25. Both are
the base commit's behaviour, unchanged.

That second measurement is worth stating carefully, because a plausible reading
of the code predicts otherwise. `BlockingThreadPoolExecutorService.getNamedThreadFactory()`
builds its workers as `SubjectInheritingThread`s, and so does the
`UserGroupInformation` TGT-renewer pool at `security/UserGroupInformation.java:L934`;
that class captures the creator's identity in its `start()` override at
`util/concurrent/SubjectInheritingThread.java:L179-184` and re-establishes it
around the whole of `work()` at `:L201-215`, which would give a subjectless task
the worker's creator. **It does not, because that override never runs for a pool
worker.** From JDK 21 a `ThreadPoolExecutor` starts its workers through the
thread container that owns them — `container.start(t)` at
`java.base/java/util/concurrent/ThreadPoolExecutor.java:899` in JDK 25's
`src.zip`, against `t.start()` at `:945` in JDK 17's — and that path bypasses the
public `Thread.start()` entirely, so `startSubject` is never captured and `run()`
re-establishes nothing. Measured directly with a real `SubjectInheritingThread`
worker in a real one-worker pool on Temurin 25.0.4+7: the first submission, made
under `alice`, observed **nothing**, and so did the subjectless submission after
it — identical to a plain-`Thread` worker, and identical to the base commit.

The alternative to the fast path — establishing the absence — is not available:
`Subject.callAs` applies `Objects.requireNonNull` to its action only, so a `null`
subject is *bound*, and a binding of nothing masks any identity in force where
the task runs. The fast path is therefore a correctness requirement rather than
an optimization, and it leaves no identity anywhere that the base commit did not
leave in the same place. Recorded as a documented outcome rather than a risk.

### (vi) The KMS eager key refill is not prepared at all

`crypto/key/kms/ValueQueue.java` receives three hunks in this migration, and only
one of them is the redirection: `new ThreadPoolExecutor(...)` at base-commit
`:L256` becomes `new HadoopThreadPoolExecutor(...)` at `:L257`, the import that
name needs is added, and six comment lines at `:L430-435` record in the code
itself what this entry records here. The identifier change is the whole of the
redirection, per the design's instruction that no call site gain propagation
logic. That redirection adds no propagation to the refill path, because the
refill never passes through the pool's submission methods.
`ValueQueue.submitRefillTask(String, Queue<E>)` at `:L414` says so in its own
pre-existing comment at `:L427-429` — "The submit/execute method of the
ThreadPoolExecutor is bypassed and the Runnable is directly put in the backing
BlockingQueue so that we can control exactly how the runnable is inserted into
the queue" — and then does exactly that at `:L436-437`, `queue.put(new
NamedRunnable(keyName) { ... })`. Nothing prepares that task, so an eager refill
runs under whatever identity its filler thread holds rather than under that of
the request whose draw emptied the queue.

**Resolution and justification.** This is left as it is, and it is **not a
regression against the base commit on either runtime.** The filler pool's workers
come from the shaded Guava `ThreadFactoryBuilder`, which emits plain
`java.lang.Thread`; measured on Temurin 25.0.4+7, such a worker observes
`Subject.current() == null`, so the base commit gave a refill no identity there
either, and on JDK 17 it gave it the filler thread's creation-time identity, which
it still does. Closing the gap would mean either having the refill read the
identity in its own constructor — propagation logic at a call site, which the
design forbids in as many words, requiring that no call site gain any — or
routing the refill through
`execute()`, which would surrender the control over queue insertion that the
comment above exists to protect, and with it the key de-duplication that
`UniqueKeyBlockingQueue` provides and the removal by object identity that
`ValueQueue.drain(String)` performs at `:L318`. Both are changes of behaviour to
the KMS refill path in service of a gap that is not a regression, so both are out
of bounds under R1. Recorded as a known residual risk; the synchronous draw path,
which is the one that returns keys to a caller, is unaffected because it runs on
the caller's own thread.

### (vii) A cancelled-but-queued prepared task keeps its captured identity reachable

Under the design as specified, what a direct Hadoop pool puts on its queue for a
prepared submission is the decorator, holding both the `FutureTask` the pool made
and the `Subject` captured at submission. Two consequences follow on JDK 25,
where preparation is active. A caller that cancels its work and walks away leaves
that decorator on the queue, and the captured `Subject` — with the credentials in
it — stays reachable for as long as the entry does, which on a pool whose workers
are all occupied or whose queue has stopped draining is until something else
displaces it. And the inherited `ThreadPoolExecutor.purge()` no longer recognises
such an entry as a cancelled `Future`, because the queued object is a `Runnable`
decorator rather than the `Future` itself, so a sweep does not reclaim it.

**Resolution and justification.** No fix is available inside the surface the
design freezes. Reclaiming those entries would require `HadoopThreadPoolExecutor`
to override `newTaskFor`, `remove` and `purge`, and to add a task type of its
own — surface the design excludes explicitly: it prescribes overriding
`execute(Runnable)` **only**, and warns against `newTaskFor` precisely because a
second wrapping layer would defeat the single-level `unwrap` that the R6 logging
obligations depend on. Overriding the bulk submission methods to reclaim what an
`invokeAny` abandons is excluded on the same grounds. The specified design takes
precedence over a remediation that contradicts it, so the property is documented
rather than engineered away.

Three things bound it. It arises only on JDK 25, since on JDK 17 nothing is
prepared and the queued object is the pool's own `FutureTask`, exactly as at the
base commit. It affects only the two direct pools: the forwarding services
prepare the task a future was *given* to run, and `FutureTask.finishCompletion`
clears its `callable` once complete — cancellation included — identically on JDK
17 and JDK 25, so a completed or cancelled future there lets go of the prepared
task and of the identity in it. And an entry is reclaimed as soon as the queue
drains past it, which is the normal case for a pool that is making progress.
Recorded as an accepted residual risk. A caller holding credentials it needs
released promptly should cancel *and* let the pool drain, or use one of the
forwarding services.

### (viii) A task can be prepared twice where one executor forwards into another

Preparation is applied once per executor, and an executor that has already
prepared a task is recognised and left alone: `SubjectPreservingTasks.wrap` returns
its argument unchanged when handed something it produced earlier, which is what
keeps a single reading enough to recover the submitted task. That recognition works
on the object it is handed, so it is defeated whenever an intervening layer hides
the earlier preparation, and two compositions in this tree do exactly that.
`util/SemaphoredDelegatingExecutor.java` prepares a task and then wraps the result
in its own permit-releasing decorator before forwarding, so a chain of one such
executor over `util/BlockingThreadPoolExecutorService.java` — the pairing the
object-store connectors build — hands the inner pool a permit wrapper it does not
recognise, and the inner pool prepares it again. Submitting rather than scheduling
through one of the forwarding services onto a pool Hadoop owns does the same by a
different route: the pool builds a `FutureTask` around the already-prepared task,
and prepares that future in turn. In both cases a subject-bearing task on JDK 25
ends up inside two decorators and two `SubjectUtil.doAs` calls.

**Resolution and justification.** Left as it is, because **the outcome is
identical and only the cost differs.** Both layers capture on the same submitting
thread and therefore capture the same subject, so the task runs under exactly one
identity — its submitter's — whether it is established once or twice; what the
second layer costs is one object and one nested scoped-value binding per
submission. Removing it would mean giving the executors a way to classify their
delegates and vary their preparation accordingly, which is surface beyond the four
composition sites the design fixes for
`util/SemaphoredDelegatingExecutor.java` and beyond the single funnel it fixes for
each pool; the design's own note on
`SubjectPreservingScheduledExecutorService` states the submit-rather-than-schedule
case plainly rather than engineering around it. What must be protected is the
recovery of the submitted task, and it is: recovery removes exactly one layer, so
a second one stays visible instead of being concealed, and it is asserted directly
— `testATaskAlreadyCarryingAnIdentityIsNotGivenASecond` submits through a
forwarding service over a Hadoop pool and requires the pool's own debug line to
still name the class that was submitted. Removing either guard from `wrap` makes
that test fail, with the doubled class name printed in the failure. Recorded as an
accepted cost.

### (ix) Bulk submission through a forwarding service prepares before it delegates

The four bulk submission methods of `SubjectPreservingExecutorService` prepare
every task in the collection they are given and then hand the prepared collection
on, which is what makes each task read the identity of the thread that called
`invokeAll` or `invokeAny` rather than that of a worker. Two consequences follow
for a service obtained from one of the four single-thread factories in
`HadoopExecutors`, which are the only places such a service is built. A timed
`invokeAll` or `invokeAny` starts its wait when the delegate is called with the
prepared tasks, so the time preparation took is not counted against it; and a
collection whose iterator produces its elements slowly is drained before the first
task is submitted, where the delegate on its own would have begun submitting
sooner.

**Resolution and justification.** Left as it is. Preparing the tasks before
delegating is what the design prescribes for all four of these methods, and there
is no way to read the submitting thread's identity for a task without touching the
task before the delegate runs it. The two methods say so where a caller reads
them: "the wait is the wrapped service's own, measured from the moment it is
called with the prepared tasks". The effect is bounded by what preparation
actually does — one object allocated per task, on an in-memory collection — so for
the materialized lists these methods are called with in this tree it is not
measurable; it is observable only with an iterator built to be slow, which is not
a shape any caller here uses. Recorded so that a caller who does pass such an
iterator, or who needs a deadline measured from its own call, knows to establish
the deadline itself rather than assume this class is transparent to it.

### (x) A shut-down list hands back what was queued, not what was submitted

Entry (vii) records that what a direct Hadoop pool puts on its queue for a
prepared submission is the decorator. `ThreadPoolExecutor.shutdownNow()` returns
the tasks still on that queue, so on a runtime that prepares them the list it
returns holds decorators rather than the objects their submitters passed. Within
this change's reach the place a reader is most likely to meet that is
`util/AsyncDiskService.java`, whose `shutdownNow()` at `:L156` aggregates the
per-volume pools' own lists at `:L163` and returns them under a name of Hadoop's
own; the executor implementations reached here return theirs through the
`ExecutorService` method of the same name. A caller that only drains or counts
that list sees no difference; one that casts an element back to the type it
submitted, or reads a field off it, would not find what it expects.

**Resolution and justification.** Left as it is, and the risk is bounded by
measurement rather than by assumption. `AsyncDiskService` is
`@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})` at `:L44` and
`@InterfaceStability.Unstable` at `:L45`, so its contract is not a public one.
The only caller of that list anywhere in this repository is
`hadoop-mapreduce-project/.../mapreduce/util/MRAsyncDiskService.java:148-149`,
which forwards it on without inspecting an element. Of the 132 `shutdownNow()`
lines in the tree, that one and the aggregation at `:L163` above are the only two
that use the returned value at all — every other call discards it — and **neither
reads an element of it**. The related
hazard of identifying a task by object identity is likewise absent: `getQueue()`
appears nowhere in `hadoop-common-project`, and the single pool `remove` —
`executor.remove(e)` at `crypto/key/kms/ValueQueue.java:L318` — matches tasks that
never went through `execute()` and were therefore never prepared, which is entry
(vi) above. Handing back the submitted object instead would mean tracking every
prepared task outside the queue the JDK owns, which is state and behaviour neither
the design nor R1 admits. A caller that does need the object it submitted has the
public means already: `SubjectPreservingTasks.unwrap(Runnable)` at `:L160` returns
it, and returns anything else unchanged.

### Other deliberate deferrals under R1

None of the following was changed, and each is a decision with a reason rather
than an oversight.

* **Per-call-site coverage of the fourteen redirections.** What each redirection changes is which class constructs a pool, and every one of those classes is covered directly: the nine factories of `HadoopExecutors`, both pool classes, both forwarding services and both decorating executors in `org.apache.hadoop.util` are each submitted to and asserted on by `TestExecutorSubjectPropagation`, and removing the fix from `SubjectPreservingTasks` fails 45 of its tests. What is not covered is a per-site assertion that a given call site's own pool propagates — that a task submitted to, say, the delay executor inside `ha/ZKFailoverController.java` observes its submitter. Every one of those pools is a private field of a class whose test file lies outside the 39 paths this change touches, and reaching one from outside would take reflection, which [R3 in practice - no new reflection](#R3_in_practice_-_no_new_reflection) rules out. Each redirection is therefore established statically instead: what it consists of is a single identifier, so it is settled by reading the diff for that one line, and six of the fourteen are pinned down further by the deleted import each one causes, enumerated under [Companion check: the plain import form](#Companion_check:_the_plain_import_form). That is a property of the change rather than of a run, and it is the kind of property a reader checks rather than executes.
* **Coverage of inlined private bodies.** Of the seven inlined privileged actions, five end up exercised through published state and two do not. `CleanerUtil`'s two bodies decide the public `UNMAP_SUPPORTED` and `UNMAP_NOT_SUPPORTED_REASON` and are reached again whenever `getCleaner().freeBuffer(...)` runs; `FastByteComparisons`' body decides which comparer `compareTo` uses; and both `DynMethods.Builder.hiddenImpl` and the matching `DynConstructors` builder are public. The two without coverage are `PlatformName.isSystemClassAvailable` at `:L89`, which is private and reached only from `hasIbmTechnologyEditionModules()` at `:L68` — itself short-circuited by `JAVA_VENDOR_NAME.contains("IBM")` at `:L65`, so on the Temurin runtimes this project builds with it is never called at all — and the `pluginLoader = new URLClassLoader(urls, defaultLoader);` branch of the package-private `MetricsConfig.getPluginLoader()` at `:L247`, which needs a `plugin.urls` entry that the existing suite supplies nowhere: the only two in-tree references are **commented out**, at `TestMetricsSystemImpl.java:112` and `:154`. Closing either gap would take reflection, which [R3 in practice - no new reflection](#R3_in_practice_-_no_new_reflection) rules out; or a widened production surface, which R1 forbids; or test files beyond the 39 this change touches. Both are therefore deferred rather than dressed up as coverage. What makes the deferral safe is that neither body's *semantics* changed: per JEP 486 a JDK 24+ runtime already executed these actions immediately, so inlining them reproduces exactly what the JVM was doing — see [Why the removals are provable no-ops](#Why_the_removals_are_provable_no-ops).
* **A navigation entry for this page.** `hadoop-project/src/site/site.xml` carries the site's menu — `Compatibility.md` is reached from it through `<item name="Compatibility Specification" href="hadoop-project-dist/hadoop-common/Compatibility.html"/>` at `:L50` — and this page is deliberately **not** added to it. Doxia renders every `src/site/markdown/*.md` into the corresponding `.html` whether or not it is listed, so the page is published either way; `site.xml` governs only the menu. This change is limited to two documentation paths, this page and `BUILDING.txt`, so the menu entry is left to whoever next edits `site.xml`.
* **The `require.test.libhadoop` build-configuration defect** — fully root-caused under [Environmental Test Failures](#Environmental_Test_Failures) and deliberately not fixed.
* **Three call sites that already route correctly** — `util/ShutdownHookManager.java:L80`, `hadoop-registry/.../RegistryAdminService.java:L113` and the cached-pool site in `hadoop-registry/.../RegistryDNS.java` (base-commit `:L172`, now `:L171`) already call `HadoopExecutors`, so the centralized fix reaches them automatically and none received a hunk. Editing them would be a change with no defect behind it. The first two files are untouched entirely; `RegistryDNS` was edited only at its *other* pool site, a raw `Executors.newSingleThreadExecutor()` at base-commit `:L1177`, which is why the cached-pool line above shifted by one.
* **The superseded capture idiom in `UserGroupInformation`** (base-commit `:L934-936`, where the TGT-renewer pool's `ThreadFactory` returns a `SubjectInheritingThread`) is left in place; removing it is cleanup, not migration. It is genuinely superseded rather than merely redundant: on JDK 25 that thread's `start()` override never runs, because a pool starts its workers through their thread container instead, so the idiom propagates nothing there and the submission-time capture is what makes the renewer's task run as the right user; on JDK 17 the runtime propagates the identity itself and both are no-ops. The measurement is entry (v) of the [Behavioural Resolution Register](#Behavioural_Resolution_Register).
* **The `MakeAccessible` class** at `util/dynamic/DynMethods.java:529` is retained and simply invoked directly.
* **Cosmetic and typographic issues**, deliberately untouched: the "Kerbeors" spelling in a `UserGroupInformation` javadoc comment (base-commit `:L946`); the unused `import org.apache.hadoop.util.Shell;` at `TestSubjectPropagation.java:30`; the 114-character line at `util/Daemon.java:36`; and the three coexisting license-header styles inside `util/concurrent` — a clean form in `package-info.java`, a form carrying a stray ` * *` line in the four pre-existing seam files (`ExecutorHelper.java`, `HadoopExecutors.java`, `HadoopThreadPoolExecutor.java` and `HadoopScheduledThreadPoolExecutor.java`, and nowhere else in the package — the three new `SubjectPreserving*` files do not reproduce it), and a one-space `/**` form in `SubjectInheritingThread.java` — which are **not** normalized.
* **Two out-of-module POM warnings** surfaced by the reactor and left to their owners: a duplicate `org.mockito:mockito-junit-jupiter` declaration in `hadoop-yarn-server-nodemanager`, and a missing `protobuf-maven-plugin` version in `hadoop-yarn-csi`.

Some context makes these deferrals honest rather than negligent: **checkstyle
does not gate this build.** The root `pom.xml` sets
`<failOnViolation>false</failOnViolation>` at `:L521` and declares
`maven-checkstyle-plugin` at `:L642-643` with **no `<executions>` binding**.
The 114-character line in unchanged main source is itself the proof, since
`checkstyle.xml` sets `LineLength` `max` to 100. `NewlineAtEndOfFile` is
commented out in the same configuration, which is why the pre-existing absence
of a trailing newline in `HadoopExecutors.java` is legitimate and was not
"corrected".

Documented Conflict Resolutions
-------------------------------

**Five** conflicts are resolved here, each with its justification. Four are
conflicts between the technical specifications, the seven constraints, and the
codebase as it actually is. The fifth is between two things the design itself
specifies, and it is settled by recording what each runtime measurably does
rather than asserting a single outcome of both.

### Conflict 1 - Reflection-guarded shims versus R3

The technical specifications permit SecurityManager-era API usage "inside
documented, reflection-guarded compatibility shims". **R3 forbids reflection
shims that keep SecurityManager-era code paths alive.**

**Resolution: R3 controls.** No SecurityManager-era code path is preserved
anywhere; every site is removed by inlining. The only reflection this change
puts to work is `SubjectUtil`'s cross-version dispatch of the **replacement**
API, which already existed; the pre-existing reflective code in the other
touched files is left exactly as it was, and nothing reflective is added to
production or to test code. The whole of what the diff contains on this point
is registered under
[R3 in practice - no new reflection](#R3_in_practice_-_no_new_reflection).

**Justification.** The two statements reconcile once "shim" is read precisely.
The specifications' permission contemplates shims that keep **old** behaviour
reachable, which is exactly what R3 prohibits. `SubjectUtil` does the opposite:
it makes **new** behaviour reachable from old bytecode. Reading the permission
narrowly costs nothing here, because no site actually needed it, and it
satisfies the stricter of the two constraints — the correct posture whenever a
permission and a prohibition overlap.

Concretely, **this change introduces no reflective construct anywhere, and
alters none that was already there**. The new `SubjectPreservingTasks` utility
contains no `MethodHandle`, no `java.lang.reflect` usage and no
`java.lang.reflect.Proxy`; a design based on `Proxy` over the executor
interfaces was evaluated and rejected, since dynamic dispatch would add
per-call overhead where a compile-time forwarding class costs nothing. The three
new test suites are equally free of it, and what they observe instead — together
with what that costs in coverage — is set out under
[R3 in practice - no new reflection](#R3_in_practice_-_no_new_reflection).

### Conflict 2 - Compiler release 17 versus a JDK 18 API

The requirements permit raising `maven.compiler.release` beyond 17 only if a
JDK-25-only API is *unavoidable*. The replacement API this migration depends on
does not exist at release 17.

Verified evidence: the root `pom.xml` sets `<javac.version>17</javac.version>`
at `:L142`, and the `jdk17+` profile in `hadoop-project/pom.xml` at `:L2828-2837`
activates on `<jdk>[17,)</jdk>` and sets `maven.compiler.release` from that
property — and **that range includes JDK 25**, so building on JDK 25 still
targets release 17. Two measurements close the question:

* `javac --release 17` **fails** on a direct `Subject.callAs` / `Subject.current` reference (`cannot find symbol: method callAs(Subject,Callable<Void>)`; `cannot find symbol: method current()`), while `--release 18` and `--release 25` both succeed.
* `javap -v` on the JDK-25-compiled `HadoopExecutors.class` reports **`major version: 61`** — Java 17 bytecode — proving the reactor genuinely compiles to release 17 while running on JDK 25.

**Resolution: the release stays at 17.** All `Subject` API access flows through
`SubjectUtil`'s `MethodHandle` bridge.

**Justification.** Because the bridge exists and already works in exactly this
configuration, the API is **not unavoidable**, so the requirements' own escape
clause never opens. Raising the release would additionally emit bytecode that
JDK 17 cannot load, breaking the mandated JDK 17 compatibility check. The
result is **one source set, one artifact, one bytecode level** — no
Multi-Release JAR, no `--enable-preview`, no per-JDK source directories and no
build-file surgery. As a direct consequence, no code added by this change
references `Subject.callAs`, `Subject.current` or `ScopedValue` directly; such
a reference would not compile.

### Conflict 3 - Preserved assertions versus SecurityManager assertions

The requirements freeze existing test assertions while permitting
infrastructure adaptation only where the JDK removed an API a test depended on.
Had any test *asserted* SecurityManager behaviour, those directives would have
collided.

**Resolution: the conflict is moot — no collision exists.** Running the audit
over `hadoop-common-project/*/src/test/java` returns **exactly 5 hits, all
inside comments**:

* `hadoop-common/src/test/java/org/apache/hadoop/security/TestUserGroupInformation.java:880`
* `hadoop-common/src/test/java/org/apache/hadoop/util/concurrent/TestSubjectPropagation.java:154`, `:155`, `:185`, `:186`

**No test assertion, setup step or teardown step references `SecurityManager`,
`AccessControlContext`, `AccessController` or `java.security.Policy`.**

**Justification.** The audit is exhaustive over the test tree, and the
empirical result corroborates it: `TestUserGroupInformation` passes **41/41 on
both JDK 17 and JDK 25**, unmodified. Rewording those five comments was the only
touch the directives permitted, and it was declined; they are itemized under
[Test-Source Comment Corrections](#Test-Source_Comment_Corrections), which
records the two files as left byte-for-byte unchanged.

### Conflict 4 - A tie-breaker runtime the base commit cannot build on

R7 designates pre-JDK-18 behaviour at the base commit as the tie-breaker for
ambiguous decisions, naming a runtime older than JDK 17. **The base commit
cannot be built on that runtime at all.** Two independent barriers make it
impossible, and both are structural rather than incidental:

1. The enforcer binds `requireJavaVersion` to `${enforced.java.version}`, which resolves to **`[17,)`** — the property is defined at root `pom.xml:L149` and the rule is bound at `:L192-194` — and `maven-enforcer-plugin` **3.5.0** (declared at root `pom.xml:L108`) runs on the default lifecycle. An older build is rejected **before compilation begins**. For completeness, `<requireMavenVersion>` sits at `:L189-191` against `<enforced.maven.version>[3.3.0,)</enforced.maven.version>` at `:L150`.
2. `javac.version` is **17** at root `pom.xml:L142`, and an older `javac` cannot emit `--release 17`.

**Resolution: JDK 17 at the base commit is used as the faithful proxy for
pre-JDK-18 behaviour**, and every tie-breaking observation in this migration
was taken there.

**Justification.** The proxy is *faithful*, not merely convenient.
`SubjectUtil.checkThreadInheritsSubject()` returns `true` for any
`JAVA_SPEC_VER <= 21` — at base-commit `:L94-108`, with the early return at
`:L96-97`, the comment `// 24+ never inherits the Subject.` at `:L99`, a
deliberate-conservatism block at `:L100-105` covering the non-LTS, end-of-life
JDK 22 and 23 case, and `return false;` at `:L106`. On JDK 17 the subject
therefore **is** inherited by newly created threads, which is precisely the
pre-JDK-18 semantics the tie-breaker exists to protect. The JDK 17 differential
test run is the artifact that records those observations. Declaring R7
unsatisfiable would have discarded a constraint the requirements treat as
important; the proxy preserves its intent exactly.

Note that `THREAD_INHERITS_SUBJECT` returns `false` for JDK 22 and 23, where
inheritance was conditional. Both are non-LTS and end-of-life, and the
conservative `false` is safe **in both directions in which the flag is now
read**. At the thread-*creation* boundary, where it was already consulted —
`util/concurrent/SubjectInheritingThread.java:180` and `:202`, and
`util/Daemon.java:48` and `:65` — a `false` means each of those establishes the
subject explicitly instead of relying on the runtime to have copied it, which is
correct whether or not the runtime would have. At the task-*submission*
boundary, which this change adds, a `false` means the identity is captured and
re-established rather than left to the runtime — again correct whether or not
the runtime would have carried it. The flag's own declaring comment says as much
at base-commit `:L100-105`: it exists to decide whether a `doAs` can be
optimized out. Where the flag is `true` the runtime carries the identity itself
and the optimization is exact at the creation boundary; what it means at the
submission boundary is the subject of
[Conflict 5](#Conflict_5_-_The_identity_fast_paths_versus_the_per-submitter_guarantee)
and of entry (v) of the
[Behavioural Resolution Register](#Behavioural_Resolution_Register).

### Conflict 5 - The identity fast paths versus the per-submitter guarantee

The design specifies two conditions under which the wrapping helper hands its
argument straight back, and it also specifies the objective the whole change
exists to serve. On one runtime those two things do not fully coincide, and this
is where that is reconciled and its consequence recorded rather than glossed.

The two conditions are `SubjectUtil.THREAD_INHERITS_SUBJECT` being `true`, which
is what makes the change a provable no-op on JDK 17, and a captured subject of
`null`, which is what keeps a binding of nothing from masking an identity in
force where the task runs. The objective is that a worker reused across
submitters run each task under the identity of that task's own submitter, proved
by a same-worker A-then-B regression test that must pass on **both** JDK 17 and
JDK 25.

**Resolution: both fast paths are implemented exactly as specified, and the
per-submitter guarantee is stated per runtime rather than as a single outcome
asserted of both.** The specification of the fast paths is frozen and takes
precedence; what a reader is owed in exchange is a precise account of what each
runtime therefore does, which is entry (v) of the
[Behavioural Resolution Register](#Behavioural_Resolution_Register).

**Justification.**

* **The fast paths are the mechanism by which JDK 17 support is preserved.** The requirement is not that JDK 17 be *tested* and found working, but that this change cost it nothing: no allocation, no extra frame, behaviour bit-identical to the base commit. Only returning the task unchanged delivers that, and it delivers it by construction rather than by measurement. Removing the first fast path in order to make one assertion read the same on both runtimes would trade a guarantee for a test's convenience.
* **On a runtime where the flag is `true`, the runtime is already doing the propagating.** `THREAD_INHERITS_SUBJECT` reports that a newly created thread is given its creator's identity. A pool worker is created once and then reused, so what such a runtime propagates to a pooled task is the identity in force when that *worker* was created. Measured on JDK 17 (Temurin 17.0.20+8) on a one-worker pool: a task submitted by `bob` after a task submitted by `alice` observes **`alice`**, and a task submitted with no identity at all also observes **`alice`**. That is exactly the behaviour of the base commit, and exactly the pre-JDK-18 semantics R7 designates as the tie-breaker — see [Conflict 4](#Conflict_4_-_A_tie-breaker_runtime_the_base_commit_cannot_build_on). It is not, however, the same situation as JDK 25's, and it is worth being exact about the difference rather than describing both as one defect. What JDK 25 does is lose the identity altogether, so pooled work runs with no subject; what such a runtime does is hand a reused worker the identity it was created with, so a task submitted by `bob` can be authorized and audited as `alice` — an identity confusion of its own, measured above and recorded here rather than denied. It is left as it stands because R7 designates the base commit's pre-JDK-18 behaviour as the tie-breaker and because altering it would change what a supported runtime does; a caller there that needs a strictly per-submitter identity has the remedy it always had, which is to establish the identity inside the task, exactly as the nested cases in entry (v) of the [Behavioural Resolution Register](#Behavioural_Resolution_Register) do.
* **On JDK 25 the guarantee holds strictly, which is where the defect is.** Measured on Temurin 25.0.4+7, the same pool with no fix at all gives a task **no** identity — `Subject.current()` returns `null`, the defect this change exists to close. With the change in place `THREAD_INHERITS_SUBJECT` is `false`, so every submission is prepared and `bob`'s task observes `bob` on the worker `alice` created. The A-then-B test therefore proves the guarantee on the runtime that needs proving.
* **This is why the tests assert an outcome per runtime.** `TestExecutorSubjectPropagation` states the reuse expectation in terms of `SubjectUtil.THREAD_INHERITS_SUBJECT` rather than asserting one outcome of both runtimes: where the flag is `false` it requires the second submitter's own identity, and where it is `true` it requires the identity the runtime itself carried. Both branches run and assert on both runtimes — nothing is skipped, as recorded under [No conditional-skip mechanism either](#No_conditional-skip_mechanism_either). A test written to demand `bob` on both would not be a stronger test; it would be a test of a mechanism the design deliberately does not use on JDK 17.
* **The captured-`null` fast path is a correctness requirement, not an optimization.** `Subject.callAs` applies `Objects.requireNonNull` to its action only, so a `null` subject is *bound*, and a binding of nothing masks whatever identity is in force where the task runs. Handing the task back unchanged is the only behaviour that leaves an enclosing identity alone. Its consequence — that a subjectless submission observes whatever its worker happens to hold — is recorded as entry (v) of the register, along with the measurement showing it is unchanged from the base commit on both runtimes.

**What this means for the seam's contents.** Because both fast paths stand, the
seam needs nothing beyond what the design prescribes, and it contains nothing
beyond it. `SubjectPreservingTasks` publishes exactly `wrap(Runnable)`,
`wrap(Callable<T>)` and `unwrap(Runnable)`, backed by two private static final
nested decorators. `HadoopThreadPoolExecutor` overrides `execute(Runnable)` and
nothing else; `HadoopScheduledThreadPoolExecutor` overrides its four `schedule*`
methods and nothing else; and neither overrides `newTaskFor`, `submit`,
`invokeAll`, `invokeAny`, `remove`, `purge` or `shutdownNow`. `HadoopExecutors`
wraps its four JDK-delegating returns in the two forwarding services, which
intercept only submission methods and forward everything else. Two consequences
of that restraint are real and are recorded rather than left implicit: a
cancelled-but-queued prepared task keeps its captured identity reachable until
the executor drops the queue entry, which is entry (vii) of the register; and
`ValueQueue`'s refill, which is put straight into its pool's backing queue
instead of being submitted through it, is not prepared at all, which is entry
(vi). Both are properties of the frozen design, both were measured to be no
worse than the base commit, and neither is addressable without adding surface
the design excludes.

Regression Tests and Verification Evidence
------------------------------------------

This section is the durable record of what was verified, on which runtimes, and
by exactly which commands. It exists so that the claims made elsewhere on this
page can be re-derived rather than taken on trust: surefire's report trees live
under `target/`, which is build output and is not retained in the repository, so
what is published here is the reproducing invocation together with the counts it
produced.

### The three mandated regression tests

All three live in
`hadoop-common/src/test/java/org/apache/hadoop/util/concurrent/`, are JUnit 5
Jupiter throughout, and are the whole of the test change.

| Suite | Tests | Lines | Requirement it proves |
|---|---:|---:|---|
| `TestExecutorSubjectPropagation` | 49 | 1,766 | An identity established through the public login API is observable from a task submitted to a Hadoop-owned executor — across all nine `HadoopExecutors` factories, `HadoopThreadPoolExecutor` (`execute`, `submit`, `invokeAll`, `invokeAny`), `HadoopScheduledThreadPoolExecutor` (all four `schedule*`, periodic task included), both forwarding services, `SemaphoredDelegatingExecutor` and `BlockingThreadPoolExecutorService` — plus the same-worker A-then-B reuse case, a submission carrying no identity, a submission of nothing at all, a task that reaches a second executor already carrying an identity, exception identity through `Future.get()`, and the `unwrap` diagnostics that R6 protects |
| `TestThreadFactorySubjectPropagation` | 17 | 734 | A thread created through Hadoop's thread utilities observes its creator's identity |
| `TestNestedSubjectPropagation` | 5 | 448 | Nested re-entrant execution: the inner identity wins inside the inner scope, the outer one is exactly restored on exit, and `UserGroupInformation.getCurrentUser()` agrees at every level |
| **Total** | **71** | **2,948** | |

None contains `Thread.sleep`, a polling wait, `@Disabled`, or any conditional-skip
annotation; ordering is established by a FIFO barrier on a single-worker pool,
which is exact because such a pool runs `afterExecute` for one task before
dequeuing the next. None contains a reflective construct — see
[R3 in practice - no new reflection](#R3_in_practice_-_no_new_reflection).

### How to reproduce, and what it produced

```
export JAVA_HOME=/path/to/jdk-25
mvn -B -pl hadoop-common-project/hadoop-common surefire:test \
    -Drequire.test.libhadoop=false \
    -Dtest="TestExecutorSubjectPropagation,TestNestedSubjectPropagation,\
TestThreadFactorySubjectPropagation"
```

Then repeat with `JAVA_HOME` pointing at the JDK 17 installation. Delete
`hadoop-common-project/hadoop-common/target/test/data` between the two runs, for
the stale-keystore reason given near the end of
[Build and Verification Environment](#Build_and_Verification_Environment).

| Suite | JDK 25 | JDK 17 |
|---|---|---|
| `TestExecutorSubjectPropagation` | 49 run / 0 failures / 0 errors / 0 skipped | 49 / 0 / 0 / 0 |
| `TestThreadFactorySubjectPropagation` | 17 / 0 / 0 / 0 | 17 / 0 / 0 / 0 |
| `TestNestedSubjectPropagation` | 5 / 0 / 0 / 0 | 5 / 0 / 0 / 0 |

### The negative control: the suite does detect the defect

A test that passes against a broken implementation proves nothing, and the
propagation defect is exactly the kind that a plausible test misses. The suite
therefore has to fail when the fix is taken away, and it does. Disabling the fix
means making both `wrap` overloads in `SubjectPreservingTasks` return their
argument unconditionally, which reduces the utility to a no-op while leaving every
call site and every seam override in place; on JDK 25 that yields:

| Suite | Result with the fix disabled |
|---|---|
| `TestExecutorSubjectPropagation` | 49 run / **45 failures** |
| `TestNestedSubjectPropagation` | 5 run / **4 failures** |
| `TestThreadFactorySubjectPropagation` | 17 run / 0 failures |
| **Total** | **71 run / 49 failures** |

`TestThreadFactorySubjectPropagation` staying green is the expected and desirable
result, not a gap: it covers direct thread creation through
`SubjectInheritingThread`, which does not go through the task-wrapping utility at
all. With the utility as it ships, all 71 pass, on both runtimes, as the table
above records.

### Constraint-by-constraint verdict

The compliance evidence for each of the seven governing constraints is developed
in the sections linked below; this table is the summary an auditor needs first.

| Constraint | Verdict | Where the evidence is |
|---|---|---|
| R1 — every hunk traces to a JDK 25 incompatibility or the propagation defect | Met | [In scope](#In_scope), [Other deliberate deferrals under R1](#Other_deliberate_deferrals_under_R1) |
| R2 — no blanket `--add-opens` / `--add-exports` | Met; zero JVM-flag changes | [R2 in practice: zero JVM-flag changes](#R2_in_practice:_zero_JVM-flag_changes) |
| R3 — no reflection shim keeping SecurityManager-era paths alive | Met; no new reflection anywhere | [R3 in practice - no new reflection](#R3_in_practice_-_no_new_reflection), [Conflict 1](#Conflict_1_-_Reflection-guarded_shims_versus_R3) |
| R4 — no new dependency for security-context handling | Met; zero dependency and zero plugin changes | [Dependency and plugin versions: all frozen](#Dependency_and_plugin_versions:_all_frozen) |
| R5 — no test disabled or excluded | Met; the baseline flaky set is empty and stays empty | [Flaky-Test Exclusion Justification List](#Flaky-Test_Exclusion_Justification_List), [Environmental Test Failures](#Environmental_Test_Failures) |
| R6 — logging formats and configuration schemas frozen | Met; two unwrap obligations discharged, nine artifacts byte-verified | [R6 in practice: the unwrap obligations](#R6_in_practice:_the_unwrap_obligations), [R6 in practice: the frozen configuration artifacts](#R6_in_practice:_the_frozen_configuration_artifacts) |
| R7 — pre-JDK-18 behaviour is the tie-breaker, each resolution documented | Met; ten resolutions and five conflicts recorded | [Behavioural Resolution Register](#Behavioural_Resolution_Register), [Documented Conflict Resolutions](#Documented_Conflict_Resolutions) |

The mandated acceptance criteria map onto this page as follows: the build exiting
zero with no compilation error on JDK 25 is
[The corrected invocation](#The_corrected_invocation); the module suites and their
residual failures are [Environmental Test Failures](#Environmental_Test_Failures);
the three propagation requirements are the three suites above; the zero-hit static
audit is [Static Audit of SecurityManager-Era API](#Static_Audit_of_SecurityManager-Era_API);
and the same build and suites on JDK 17 are
[Proof that none is a JDK 25 regression](#Proof_that_none_is_a_JDK_25_regression).

Environmental Test Failures
---------------------------

Verification observed **15 test failures** across the two audited slices. All 15
are **environmental**, none is a migration regression, and — per R5 — every one
is documented and root-caused rather than suppressed. Fourteen come from the
harness running as the superuser; the fifteenth comes from the absent native
library, and appears only when the workaround described below is omitted.

### Proof that none is a JDK 25 regression

Both slices were run on JDK 17 as well, at the same commit and with the same
command, and **each produced results identical to JDK 25 down to the counts**:

| Slice | JDK 25 | JDK 17 |
|---|---|---|
| `security/**` + `util/**` — 141 test classes | 1030 run / 13 failures / 0 errors / 36 skipped | 1030 / 13 / 0 / 36 |
| `io/**` + `metrics2/**` + `crypto/key/**` — 133 test classes | 1010 run / 1 failure / 0 errors / 155 skipped | 1010 / 1 / 0 / 155 |

The two slices are selected with these `-Dtest` patterns, and the class counts
above are only reproducible against them, since a narrower pattern silently
selects fewer classes:

```
-Dtest="org/apache/hadoop/security/**/Test*.java,org/apache/hadoop/util/**/Test*.java"
-Dtest="org/apache/hadoop/io/**/Test*.java,org/apache/hadoop/metrics2/**/Test*.java,\
org/apache/hadoop/crypto/key/**/Test*.java"
```

Count classes from the `Tests run:` summary line of each
`target/surefire-reports/*.txt`, not by listing that directory: surefire also
writes a `<class>-output.txt` per class that captures stdout, so a plain file
count roughly overstates the number of test classes.

The failing classes are the same on both runtimes, with the same failing
methods: `util/TestDiskChecker`, `util/TestBasicDiskValidator` and
`util/TestReadWriteDiskValidator` account for all 13 in the first slice, and
`metrics2/sink/TestRollingFileSystemSinkWithLocal` for the single one in the
second. Those four are the whole of it under the invocation above; a fifth,
`util/TestNativeCodeLoader`, joins them only when the native-library workaround
below is omitted, which is why the total elsewhere on this page is 15 rather
than 14. A failure that behaves identically on both runtimes cannot have been
introduced by moving to JDK 25. This differential is the evidence, and it is the
reason no exclusion was needed to reach a defensible result.

### 14 failures: the harness runs as uid 0

`id -u` returns **0** in the verification container. Running as the superuser
bypasses discretionary access-control permission checks, so a directory the test
makes unreadable or unwritable remains accessible. An assertion that access
*fails* therefore **cannot** hold, across four test classes:

| Test class | Failures | What it asserts and cannot observe |
|---|---|---|
| `util/TestDiskChecker` | 6 | `checkDir` must reject an unreadable, unwritable or unlistable directory |
| `util/TestBasicDiskValidator` | 6 | the same six cases, through the disk-validator front end |
| `util/TestReadWriteDiskValidator` | 1 | a disk check must fail on a directory it made inaccessible |
| `metrics2/sink/TestRollingFileSystemSinkWithLocal` | 1 | writing metrics must error after `FileUtil.setWritable(dir, false)` |

The bypass was confirmed directly rather than inferred: in this container a write
into a `chmod 000` directory **succeeds**. That makes every assertion above
unfailable whatever the code does, which is also why none of these classes is
touched by this change — `metrics2/sink` in particular contains no hunk of it.

This is a property of the environment, not of the code, and not of the JDK.
Running the same suites as an unprivileged user removes these failures. They are
documented here rather than excluded.

### The 15th failure: `libhadoop.so` is absent

Native code is explicitly out of scope for this migration, so `libhadoop.so` is
never built. The interesting part is *why* its absence produces a failure
rather than a skip, because that is a **latent build-configuration defect
present at the base commit and wholly independent of JDK 25**:

* `require.test.libhadoop` is passed as a surefire `<systemPropertyVariables>` entry in exactly **two** POMs — `hadoop-project/pom.xml:2590` and `hadoop-common-project/hadoop-registry/pom.xml:280` — each in the self-referential form `<require.test.libhadoop>${require.test.libhadoop}</require.test.libhadoop>`.
* A repository-wide search for `require.test.libhadoop` in POM files finds **only those two lines**. The property is therefore **never defined as a `<properties>` entry anywhere in the reactor**, and both references resolve to nothing but themselves.
* Maven consequently passes the **literal string** `"${require.test.libhadoop}"`, which is neither `null` nor `"false"`.
* `hadoop-common/src/test/java/org/apache/hadoop/util/TestNativeCodeLoader.java` gates on exactly those two values, in `requireTestJni()` at `:L33-38`: `String rtj = System.getProperty("require.test.libhadoop");` (`:L34`), `if (rtj == null) return false;` (`:L35`), `if (rtj.compareToIgnoreCase("false") == 0) return false;` (`:L36`), `return true;` (`:L37`). The literal string matches neither guard, so the method returns **`true`** and `testNativeCodeLoaded()` (annotated at `:L40`, declared at `:L41`, with its guard at `:L42`, an informational log at `:L43` and an early `return` at `:L44`) demands a library that was never built.
* `dev-support/bin/hadoop.sh` supplies `-Drequire.test.libhadoop` at `:L28` and `:L254` — the Apache Yetus CI path — which is why continuous integration provides the property while a plain `mvn` invocation does not. `dev-support/**` is out of scope and untouched.

**This defect is root-caused and documented, and deliberately NOT fixed**,
because it traces to neither a JDK 25 incompatibility nor the
Subject-propagation defect and R1 forbids the hunk. The R5-compliant workaround
for a native-less environment is to pass **`-Drequire.test.libhadoop=false` on
the command line** — never to edit a POM.

### Measured results that bound the risk

Every figure below was measured on the change as it stands, on **both** runtimes,
with `-Drequire.test.libhadoop=false` for the test runs.

| Measurement | JDK 25 | JDK 17 |
|---|---|---|
| Corrected thirteen-module reactor build, `clean install -DskipTests` | `BUILD SUCCESS`, exit 0, zero `[ERROR]` lines, 13/13 modules | `BUILD SUCCESS`, exit 0, zero `[ERROR]` lines, 13/13 modules |
| Source warnings from that build | 18, all pre-existing and catalogued below | 8, the same main-source pair (JDK 17's `javac` does not emit the boxed-constructor notes) |
| Emitted bytecode level | `major version: 61` (release 17) | `major version: 61` |
| `util/concurrent` package — 4 test classes (the 3 new suites plus the pre-existing `TestSubjectPropagation`) | 77 run / 0 failures / 0 errors / 0 skipped | 77 / 0 / 0 / 0 |
| `security/**` + `util/**` slice — 141 test classes | 1030 run / 13 failures / 0 errors / 36 skipped | 1030 / 13 / 0 / 36 |
| `io/**` + `metrics2/**` + `crypto/key/**` slice — 133 test classes | 1010 run / 1 failure / 0 errors / 155 skipped | 1010 / 1 / 0 / 155 |
| `hadoop-auth` full suite — 24 test classes | 179 run / 0 failures / 0 errors / 0 skipped | 179 / 0 / 0 / 0 |
| `hadoop-kms` full suite — 7 test classes | 48 run / 0 failures / 0 errors / 0 skipped | 48 / 0 / 0 / 0 |
| `hadoop-registry` full suite — 15 test classes | 164 run / 0 failures / 0 errors / 0 skipped | 164 / 0 / 0 / 0 |
| `hadoop-nfs` full suite — 3 test classes | 21 run / 0 failures / 0 errors / 0 skipped | 21 / 0 / 0 / 0 |

Every failure in those totals is accounted for above: 13 uid-0 failures in the
first slice, 1 in the second, and nothing else inside either. Omitting
`-Drequire.test.libhadoop=false` adds exactly one more, in
`util/TestNativeCodeLoader`, on either runtime. The two slices do not span the
whole module, so a wider `-Dtest` pattern reaches further failures of the same
uid-0 kind in files this change never touches —
`fs/shell/TestPathData.testGlobThrowsExceptionForUnreadableDir` is one, at
12 run / 1 failure / 0 errors / 2 skipped on both runtimes alike — and those are
environmental for exactly the reason given above rather than regressions.

The bytecode check was taken on all six seam classes rather than on a sample:
`HadoopExecutors`, `SubjectPreservingTasks`, `HadoopThreadPoolExecutor`,
`HadoopScheduledThreadPoolExecutor`, `SubjectPreservingExecutorService` and
`SubjectPreservingScheduledExecutorService` each report `major version: 61` when
compiled by the JDK 25 build — Java 17 bytecode from a JDK 25 `javac`, which is
what `maven.compiler.release` staying at 17 means in practice.

### The source warnings, itemized

"All pre-existing" is a claim worth substantiating, so here is the whole of it.
Every one of the 18 is a `javac` note about code this change does not write, and
**not one names a file in the propagation seam**. The counts are exact.

| File | JDK 25 | JDK 17 | Warning |
|---|---:|---:|---|
| `io/FastByteComparisons.java` — `:25`, `:132`, `:139`, `:141` | 4 | 4 | `sun.misc.Unsafe is internal proprietary API and may be removed in a future release` |
| `io/nativeio/NativeIO.java` — `:48`, `:901`, `:903` ×2 | 4 | 4 | the same |
| `security/authentication/server/TestAuthenticationFilter.java` — `:178`, `:360`, `:642` | 3 | 0 | `Long(long) in java.lang.Long has been deprecated and marked for removal` |
| `util/TestIdentityHashStore.java` — `:75`, `:102`, `:103`, `:104` | 4 | 0 | `Integer(int) in java.lang.Integer has been deprecated and marked for removal` |
| `service/TestCompositeService.java` — `:322`, `:340` | 2 | 0 | the same |
| `util/TestGenericsUtil.java` — `:130` | 1 | 0 | the same |
| **Total** | **18** | **8** | |

The two main-source files are the `sun.misc.Unsafe` retentions recorded as entry
(iv) of the
[Behavioural Resolution Register](#Behavioural_Resolution_Register);
`FastByteComparisons` is touched by this change only to inline its privileged
action, several lines away from any of the four warning sites, and `NativeIO` is
not touched at all. The ten boxed-constructor notes are in existing test source
that this change does not open, and JDK 17's `javac` does not emit them, which is
why the two columns differ. Remediating any of the six would be a hunk tracing to
neither defect, so R1 forbids it here.

### An honesty caveat about "BUILD SUCCESS"

`hadoop-project/pom.xml:L37` sets
`<maven.test.failure.ignore>true</maven.test.failure.ignore>`. **Surefire test
failures therefore do not fail the Maven build, and `BUILD SUCCESS` is not
evidence that tests passed.** Anyone verifying this work must either read
`target/surefire-reports/*.txt` directly or pass
`-Dmaven.test.failure.ignore=false` on the command line. Stating this matters
more than it may appear: a reader who trusts the build result alone would draw
the opposite conclusion from the one the evidence supports.

Two related surefire settings are worth knowing when reproducing these numbers:
`<reuseForks>false</reuseForks>` at `hadoop-project/pom.xml:L2564` gives a
fresh JVM per test class, and the fork's `<argLine>` at `:L2566` draws from the
`maven-surefire-plugin.argLine` property described earlier. Kerberos tests read
`<java.security.krb5.conf>` from `:L2588`.

One further reproducibility trap, easily mistaken for a regression: **delete
`hadoop-common-project/hadoop-common/target/test/data` before a run that did not
begin with `clean`.** Repeated `surefire:test` invocations leave test data in
that tree, and a keystore left there beside a stale `RawLocalFileSystem`
checksum sidecar makes `security/alias/TestCredentialProviderFactory.testFactory`
raise `org.apache.hadoop.fs.ChecksumException: Checksum error: … /creds/test.jks`
on the next run. It is a stale-artifact error, not a code failure: with that
directory removed the class passes 7 of 7 on both runtimes, and the class is
untouched by this change.

A second trap, and the more misleading of the two: **`surefire:test` on its own
compiles nothing.** Invoked against a module whose `target/test-classes` is
absent it reports `No tests to run.` and `BUILD SUCCESS`, which reads exactly
like a green suite. For `hadoop-kms` and `hadoop-registry` the counts above were
therefore taken with `test-compile surefire:test`, and any figure reproduced
from a fresh or cleaned tree must be as well. Confirm the run really executed
something by checking that the log contains one `Running org.apache…` line per
test class before reading its totals.

### Third-party warnings that are not Hadoop's, and must not be "fixed"

Both of the following appear during a verification run and neither is a
failure or suppressible from Hadoop:

* **byte-buddy**, once per forked JVM: `WARNING: A terminally deprecated method in sun.misc.Unsafe has been called`, naming `net.bytebuddy.dynamic.loading.ClassInjector$UsingUnsafe$Dispatcher$CreationAction`.
* **Maven's own JVM**: `java.lang.System::load` from `org.fusesource.jansi.internal.JansiLoader` (`jansi-2.4.1.jar`), and `sun.misc.Unsafe::objectFieldOffset` from `com.google.common.util.concurrent.AbstractFuture$UnsafeAtomicHelper` (`guava-33.2.1-jre.jar`). Both originate in Maven's own `lib/` directory, not in the reactor.

Test-Source Comment Corrections
-------------------------------

Five comments in existing test source still describe the SecurityManager
mechanism the JVM has removed. They were **identified and itemized, and the
permitted rewording was deliberately declined.** Rewording them was the only
touch the preservation directives would have allowed to existing test source,
and R1 favours the minimal diff, so the files were left byte-for-byte
unchanged.

This is a record of restraint, not of an edit: no comment text was changed.
`TestSubjectPropagation.java` remains at **191 lines / 5,307 bytes** and
`TestUserGroupInformation.java` remains unchanged, both verified against the
base commit.

| File (under `hadoop-common/src/test/java/org/apache/hadoop/`) | Line | Nature |
|---|---|---|
| `security/TestUserGroupInformation.java` | 880 | javadoc of `testUGIUnderNonHadoopContext` (block `:L878-883`): describes `getCurrentUser()` being called when the **AccessControlContext** has a Subject that Hadoop did not create |
| `util/concurrent/TestSubjectPropagation.java` | 154 | "This would fail for Java 22-23 if the **SecurityManager** would be enabled, …" |
| `util/concurrent/TestSubjectPropagation.java` | 155 | continuation: "…but we don't run tests with the **SecurityManager** enabled." |
| `util/concurrent/TestSubjectPropagation.java` | 185 | the same sentence, first line, in `testThreadRunnable` (declared at `:L161`) |
| `util/concurrent/TestSubjectPropagation.java` | 186 | the same sentence, second line |

All five are **comment text only** and carry **zero assertion change**, which
is what makes the non-touch auditable: a diff over `src/test/**` shows no
assertion change anywhere.

`TestSubjectPropagation`'s runtime-aware branches are also unchanged, and
deliberately so. They read
`if (SubjectUtil.THREAD_INHERITS_SUBJECT) { assertEquals(…) } else {
assertNull(…) }` at `:L149-157` and again at `:L181-188`. Those assertions
**must** remain runtime-aware, because the test exercises a plain
`java.lang.Thread` — constructed at `:L174` by design — whose subject
inheritance genuinely differs between JDK 17 and JDK 25. Collapsing the branch
would break the test on one runtime or the other.

Two pre-existing `Subject.doAs` occurrences in test source are also frozen:
`security/TestUserGroupInformation.java:888` and
`security/token/delegation/web/TestWebDelegationToken.java:724`. The
`Subject.doAs` to `SubjectUtil.doAs` transformation applies only to the three
**main-source** call sites in `hadoop-auth`.

Build and Verification Environment
----------------------------------

### Toolchain

* Eclipse Temurin JDK **25.0.4+7** — the migration target
* Eclipse Temurin JDK **17.0.20+8** — the mandated compatibility runtime
* Apache Maven **3.9.9**
* `protoc` **25.5**, matching `hadoop.protobuf.version` at `hadoop-project/pom.xml:L105`

JDK 25 implementation behaviour was established by reading the JDK's own
sources from `lib/src.zip` in the Temurin 25.0.4+7 installation
(53,030,691 bytes) rather than by inference. Three files carried the load:
`javax/security/auth/Subject.java`, whose `callAs` body is
`ScopedValue.where(SCOPED_SUBJECT, subject).call(action::call)` and which
null-checks only its action — the origin of the `doAs`-not-`callAs` rule and of
the reasoning about an absent subject recorded under
[Conflict 5](#Conflict_5_-_The_identity_fast_paths_versus_the_per-submitter_guarantee); `java/util/concurrent/AbstractExecutorService.java`,
which funnels all seven submission entry points through `execute(Runnable)`;
and `java/util/concurrent/ScheduledThreadPoolExecutor.java`, which routes
`execute` and all three `submit` overloads through `schedule`. Those last two
are why the seam overrides `execute` alone in one class and the four `schedule*`
methods alone in the other — and why it must **never** override both layers,
which would double-wrap and defeat single-level unwrapping.

The propagation defect itself was measured directly on Temurin 25.0.4+7:
`Subject.current()` is non-null inside `Subject.callAs(...)`, **null** inside a
plain `new Thread(...)` started from that action, and **null** inside an
executor task submitted from it.

### A build command that reports success without compiling anything

**`mvn -pl hadoop-common-project -am clean install -DskipTests` is a false
green and must not be used as an acceptance gate.** Re-measured live: it
completes in **seconds rather than minutes** and builds only **four** modules —
`[1/4]` Apache Hadoop Main, `[2/4]` Apache Hadoop Build Tools, `[3/4]` Apache
Hadoop Project POM, `[4/4]` Apache Hadoop Common Project — then reports
`BUILD SUCCESS`. The fourth is the **aggregator POM itself**. **None of
`hadoop-auth`, `hadoop-common`, `hadoop-nfs`, `hadoop-kms`, `hadoop-registry`,
`hadoop-minikdc` or `hadoop-annotations` is compiled.**

The cause is Maven's project-selection semantics: `-pl` selects **only** the
named module, and `-am` adds that module's **upstream dependencies**, never an
aggregator's **children**.

The on-disk proof is that `hadoop-common-project/pom.xml` declares
`<packaging>pom</packaging>` at `:L30` and nothing but a `<modules>` list at
`:L32-41` — eight entries: `hadoop-auth` (`:L33`), `hadoop-auth-examples`
(`:L34`), `hadoop-common` (`:L35`), `hadoop-annotations` (`:L36`),
`hadoop-nfs` (`:L37`), `hadoop-minikdc` (`:L38`), `hadoop-kms` (`:L39`) and
`hadoop-registry` (`:L40`). It has no source directory and no compiler or
surefire plugin configuration, so it genuinely compiles nothing.

How few seconds it takes is host-dependent and is not the point: repeated runs
reported anywhere from **1.2 s to 12 s** as the local repository and the
filesystem cache warmed up. What settles it is the module list — a reactor that
never enters a leaf module cannot have compiled one — so check the selection
rather than the clock.

### The corrected invocation

This is the command that was actually verified, and it builds all thirteen
modules:

```
export JAVA_HOME=/path/to/jdk-25
mvn -pl hadoop-common-project/hadoop-common,hadoop-common-project/hadoop-nfs,\
hadoop-common-project/hadoop-kms,hadoop-common-project/hadoop-registry,\
hadoop-common-project/hadoop-auth-examples \
    -am clean install -DskipTests
```

Verified reactor: `[1/13]` Main, `[2/13]` Build Tools, `[3/13]` Project POM,
`[4/13]` Annotations, `[5/13]` Project Dist POM, `[6/13]` Maven Plugins,
`[7/13]` MiniKDC, `[8/13]` Auth, `[9/13]` Auth Examples, `[10/13]` Common,
`[11/13]` NFS, `[12/13]` KMS, `[13/13]` Registry — `BUILD SUCCESS`, zero
`[ERROR]` lines, 02:24 min against a cold local repository and about a minute
against a warm one — measured warm runs land between 40 s and 01:01 min — on
JDK 25 and on JDK 17 alike.

The same `-pl` list applies to the test invocation, and the whole sequence is
repeated with `JAVA_HOME` pointing at JDK 17 as the compatibility check. In a
native-less environment, add `-Drequire.test.libhadoop=false` for the reason
given under [Environmental Test Failures](#Environmental_Test_Failures).

**Two module counts are in play, and conflating them causes confusion.** The
**root reactor is 14 modules** — the root `pom.xml` `<modules>` list, in order:
`hadoop-project`, `hadoop-project-dist`, `hadoop-assemblies`,
`hadoop-maven-plugins`, `hadoop-common-project`, `hadoop-hdfs-project`,
`hadoop-yarn-project`, `hadoop-mapreduce-project`, `hadoop-tools`,
`hadoop-minicluster`, `hadoop-client-modules`, `hadoop-build-tools`,
`hadoop-cloud-storage-project`, `hadoop-dist`. The **corrected `-pl` reactor
above is 13 modules**. Both figures are correct in their own context.

For per-operating-system JDK installation prerequisites, see `BUILDING.txt` at
the repository root; this page deliberately does not duplicate them.

### Dependency and plugin versions: all frozen

**No dependency was added, removed or re-versioned, and no build-plugin version
was bumped.** The minimum JDK-25-capable version of every plugin on the default
lifecycle **is already the version pinned in this repository**, so there was
nothing to change:

| Coordinate | Version | Declared at |
|---|---|---|
| `maven-compiler-plugin` | 3.10.1 | root `pom.xml:L125` |
| `maven-surefire-plugin` | 3.5.3 | `hadoop-project/pom.xml:L188` |
| `maven-enforcer-plugin` | 3.5.0 | root `pom.xml:L108` |
| `restrict-imports-enforcer-rule` | 2.0.0 | root `pom.xml:L109` |
| `checkstyle` | 8.29 | root `pom.xml:L120` |
| `byte-buddy` | 1.17.6 | `hadoop-project/pom.xml:L158` |

`byte-buddy` **1.17.6** is the decisive pin: it supersedes Mockito 4.11.0's
transitive **1.12.19**, which cannot instrument JDK 25 class files.

**Two changes that JDK 25 appears to require are measurably unnecessary, and
must not be introduced.** First, a Mockito or `byte-buddy` upgrade: the existing
pin works, and `hadoop-common-project` contains **zero** `mockStatic` and
**zero** `mockConstruction` usages, so no dynamic-agent self-attachment is
needed anywhere in this module tree. Scope that count to
`hadoop-common-project`: a repository-wide search finds 5 `mockStatic` files, all
of them under
`hadoop-yarn-project/.../hadoop-yarn-server-timelineservice-documentstore`,
which is out of scope here and unaffected by this change. Second, plugin version
bumps: every plugin version pinned in this repository is already the JDK-25
minimum, as the table above records. Either change would introduce hunks that
trace to no defect, violating R1.

The propagation design draws only on `java.util.concurrent`,
`java.util.Objects`, `java.security.PrivilegedAction` and its relatives,
`javax.security.auth.Subject`, and the existing `SubjectUtil`. **No**
context-propagation library was added — no Micrometer context propagation, no
OpenTelemetry context, no extension of Guava's `ThreadFactoryBuilder`, and no
home-grown `ThreadLocal` framework. Verified frozen and unchanged against the
base commit: the root `pom.xml`, `hadoop-project/pom.xml`, and all eight
`hadoop-common-project/*/pom.xml` module POMs.

### Two build-configuration facts worth knowing

* **The enforcer's import bans run before compilation.** `maven-enforcer-plugin` 3.5.0 with `restrict-imports-enforcer-rule` 2.0.0 runs execution `banned-illegal-imports` (root `pom.xml:L206`) in the **`process-sources`** phase (`:L207`, goal at `:L209`). The "Use JUnit5" block at `:L336-346` sets `<includeTestCode>true</includeTestCode>` (`:L337`), bans `org.junit.**` (`:L340`) and allows only `org.junit.jupiter.**` (`:L343`) and `org.junit.platform.**` (`:L344`). A single `org.junit.Test` import would fail the build before any compilation happens, which is why every test class added here is JUnit 5 Jupiter throughout.
* **`maven-javadoc-plugin` never runs by default.** Its `module-javadocs` execution (`hadoop-project/pom.xml:L2740`) is bound to `package` (`:L2741`, goal at `:L2743`) inside the `dist` profile (`:L2731`), and that profile has **no `<activation>` block**, so it requires an explicit `-Pdist`. Its JDK 25 behaviour is therefore unverified — a release-engineering concern rather than a build one, and recorded as an accepted residual risk. Note that `<doclint>all</doclint>` at `:L2318`, with `<additionalOption>-Xmaxwarns 10000</additionalOption>` at `:L2320`, still governs any javadoc run that *is* invoked.

### Related documentation

* [Apache Hadoop Compatibility](./Compatibility.html) — the compatibility policies this change is required to uphold
* [Hadoop Interface Taxonomy](./InterfaceClassification.html) — the audience and stability classifications referenced above
