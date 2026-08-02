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
* **(b) It closes a silent `javax.security.auth.Subject` propagation hole.** On JDK 18 and later, `Subject.callAs` establishes the current subject as a `ScopedValue` binding. That binding is confined to the dynamic extent of the call on the binding thread and is not seen by a pool worker thread. Code that previously ran as an authenticated user now runs with no subject at all — and it compiles cleanly, throws nothing, and logs nothing.

Defect (b) is the reason this work matters. No compiler diagnoses it and no
pre-existing test detects it. It surfaces later as an authorization denial, an
anonymous-principal audit record, or a `NullPointerException` raised far from
its cause.

### The shape of the fix

A single utility, `org.apache.hadoop.util.concurrent.SubjectPreservingTasks`,
captures `SubjectUtil.current()` **at task submission** — in the decorator's
constructor, which runs on the submitting thread — and re-establishes it inside
the worker. Its public surface is `wrap(Runnable)`, `wrap(Callable<T>)`,
`wrapEach(Collection)` — the form the bulk submission methods need, described
under [Conflict 5](#Conflict_5_-_The_planned_identity_fast_paths_versus_the_measured_leak)
— and `unwrap(Runnable)`.

Two properties of that utility are load-bearing:

* **Re-establishment uses `SubjectUtil.doAs`, never `SubjectUtil.callAs`.** `Subject.callAs` is specified to wrap an escaping exception in a `CompletionException`. `SubjectUtil.doAs` unwraps that and rethrows the original cause, which preserves exception identity for `FutureTask`, for every `Future.get()` caller, and for the JDK-8071638 diagnostic in `ExecutorHelper`. Using `callAs` would change the observed type of every propagated exception.
* **Every submission is prepared, on every runtime, and an absent subject is carried across as deliberately as a present one.** The task is returned unchanged in exactly two cases: when it is `null`, so that the executor it was destined for rejects it exactly as it always has; and when it already came from `wrap`, so that a task an executor is offered a second time — as `ThreadPoolExecutor.DiscardOldestPolicy` offers it — keeps one layer, and keeps the identity of the submission that was rejected rather than picking up whichever identity happens to be current on the retry. `SubjectUtil.THREAD_INHERITS_SUBJECT` is deliberately **not** consulted here; the reasoning, the measurements and the resulting deviation from the plan of record are recorded under [Conflict 5](#Conflict_5_-_The_planned_identity_fast_paths_versus_the_measured_leak).

The capture point is the substance of the fix. The idiom this change supersedes
captured the subject inside `ThreadFactory.newThread`, which runs **once per
worker thread**. In any pool that reuses threads, every task after the first
therefore executed under the *first* submitter's identity. That is a security
defect, not merely a correctness one — privilege confusion with real
authorization and audit consequences — and it is invisible to the obvious test,
because a test that submits from a single identity and asserts the subject is
visible **passes** against the broken implementation. Exposing it requires two
submitters on a thread-reusing pool, which is why the new
`TestExecutorSubjectPropagation` includes an explicit thread-reuse case.

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
`hadoop-auth` with the existing `SubjectUtil` bridge, and thirteen new test
classes — three of them the mandated regression tests for the propagation
defect, the rest proving that each removal still behaves as it did and that each
redirected construction site now holds an instrumented pool.

### Explicit non-goals

* **No behaviour change of any kind.** No RPC wire format, token serialization layout, delegation-token semantic, Kerberos or JAAS authentication outcome, log output format, configuration schema, or public signature in `org.apache.hadoop.security.*` changes.
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
only. It behaves identically on both, because the identity a task runs under is
read from the thread that submits it rather than from anything the runtime does
at a thread boundary. On JDK 17 that is a change in behaviour, and an intended
one: a pooled task there used to run under the identity of whoever first caused
its worker to exist, and it now runs under the identity of its own submitter.
[Conflict 5](#Conflict_5_-_The_planned_identity_fast_paths_versus_the_measured_leak)
records the measurements behind that decision, and the same assertions
therefore hold on both runtimes rather than each runtime having assertions of
its own.

Governing Constraints
---------------------

### Provenance of these constraints

`review_rules` reports **`No user rules provided.`** — including on a
complete-document read. **There is no separate on-disk user rules document for
this project.** The seven binding constraints recorded below are embedded in the
requirements for this work, which is the authoritative source of record for
their exact wording; they are summarized here rather than transcribed. The
absence of a rules file is not treated as licence to lower the bar: standard
Apache Hadoop documentation and engineering practice applies on top of these
seven.

### What each constraint requires

* **R1 — Traceability.** Every hunk must trace to a concrete JDK 25 incompatibility or to the Subject-propagation defect; no opportunistic cleanup, refactoring or modernization may ride along. R1 is largely *subtractive*: it is the reason a long list of defensible improvements was deliberately **not** made, and each of those is catalogued below as a deferral rather than left to look like an oversight.
* **R2 — No blanket `--add-opens` / `--add-exports`** introduced to suppress a problem rather than fix it. Honoured by verification rather than by editing: zero JVM flags were added, and none is needed.
* **R3 — No reflection shim that keeps SecurityManager-era code paths alive.** No such shim exists: every SecurityManager-era site is removed outright rather than guarded, and **no reflective construct is introduced into production code** — nor is any pre-existing one altered, including in the several touched files that are reflection-based by nature. The one reflective construct in the design reflects over the *replacement* API, which is a materially different thing — see Conflict 1 under [Documented Conflict Resolutions](#Documented_Conflict_Resolutions). Four of the new **test** suites do use reflection, to read state this change gives no published way to read; that access is test-only, reaches no SecurityManager-era API, and is registered in full under [R3 in practice - test-only reflective access](#R3_in_practice_-_test-only_reflective_access).
* **R4 — No new dependency for security-context handling**; only the JDK and existing Hadoop utilities. Zero dependencies were added, removed or re-versioned.
* **R5 — No disabling or excluding failing tests**, except tests documented as flaky at baseline, each with a written justification. The baseline flaky set is provably empty, so no exclusion was permissible and none was made.
* **R6 — Logging output formats and configuration file schemas must not change.** This is the constraint with the largest design consequence: it is what turned a plausible implementation into a correct one, by forcing the unwrap obligations described below.
* **R7 — Where correct behaviour is ambiguous, pre-JDK-18 behaviour at the base commit is the tie-breaker, and each resolution must be documented.** **R7 is the sole reason this document exists**, and its register is the [Behavioural Resolution Register](#Behavioural_Resolution_Register).

### R3 in practice - test-only reflective access

R3 governs production code, and in production this change introduces no
reflection whatsoever. Its **test** code is a different matter, and saying "no
new reflection" without that qualification would misdescribe the change. The
whole of it is therefore registered here, so that the claims made for R3 can be
checked against what the diff actually contains rather than taken on trust.

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

**What the tests contain.** Of the 14 test files this change touches, **10 use
no reflection at all**. The remaining four use it as follows, counted exactly:

| Suite | `setAccessible` | `getDeclaredField(s)` | `getDeclaredMethod` / `getMethod` | `getDeclaredConstructor` | `Class.forName` |
|---|---|---|---|---|---|
| `hadoop-auth/.../util/TestPlatformName.java` | 1 | 0 | 1 / 0 | 0 | 0 |
| `hadoop-common/.../io/TestFastByteComparisons.java` | 4 | 3 | 0 / 1 | 0 | 3 |
| `hadoop-common/.../util/concurrent/TestExecutorRedirectSubjectPropagation.java` | 10 | 2 + 1 sweep | 4 / 0 | 3 | 3 |
| `hadoop-kms/.../server/TestKMSExecutorSubjectPropagation.java` | 1 | 1 | 0 / 0 | 0 | 0 |
| **Total** | **16** | **6 + 1 sweep** | **5 / 1** | **3** | **6** |

**Why nothing published reaches what each one reaches.** The test is not whether
a reflective read is convenient, but whether the fact being asserted is
observable without it.

| Suite | What it reaches | Why the published surface cannot reach it |
|---|---|---|
| `TestPlatformName` | `PlatformName.isSystemClassAvailable(String)`, private static — the body the removed privileged block wrapped | Its only published consumer is `IBM_JAVA`, and that consumer short-circuits on `JAVA_VENDOR_NAME.contains("IBM")` before ever calling it. On every runtime this project is built and tested with, the migrated body is therefore never reached through published state, and asserting `IBM_JAVA` would assert a vendor string instead. Without the reflective call the inlined body would have no coverage at all. |
| `TestFastByteComparisons` | `LexicographicalComparerHolder.BEST_COMPARER`, and `theUnsafe` and `BYTE_ARRAY_BASE_OFFSET` inside the private `UnsafeComparer` — the handles the inlined block produces | `FastByteComparisons` is package-private and publishes only `compareTo`, which returns the same answers whether the `Unsafe` comparer initialized or the pure-Java one silently took its place. A silent fallback is the one failure mode the inlining could cause, and it is exactly the one the published surface cannot distinguish from success. |
| `TestExecutorRedirectSubjectPropagation` | the executor each redirected construction site holds: `ReadaheadPool.pool` (private final), `Groups$GroupCacheLoader` (private inner class, private executor), `CopyCommandWithMultiThread.initThreadPoolExecutor` (private), `UserGroupInformation.executeAutoRenewalTask` (private) and the field it populates, plus a field sweep for pools | Not one of the redirected sites publishes its executor, so the proof that a redirect took effect has nowhere else to read. `UserGroupInformation.getKerberosLoginRenewalExecutor()` does exist as a `@VisibleForTesting` accessor, but it is package-private in `org.apache.hadoop.security` and this suite is in `org.apache.hadoop.util.concurrent`; it would not remove the reflection in any case, because the private method that builds that pool has no accessor. A test-only forwarder in the other package was considered and rejected: it reaches the same internal state by a longer route. |
| `TestKMSExecutorSubjectPropagation` | `KMSACLs.executorService` and `KMSAudit.executor`, both private with no accessor | Same absence of a published executor. Here the read is corroborated rather than relied on: the same tests submit under a named identity and assert the identity the work sees, and separately assert that the compiled form of both classes names `HadoopExecutors` and not `java.util.concurrent.Executors`. |

**Why production visibility was not widened instead.** Adding an accessor, or
relaxing a modifier, purely so that a test need not reflect would be a
production hunk tracing to neither a JDK 25 incompatibility nor the
Subject-propagation defect — precisely what R1 forbids — and it would enlarge
the published surface of five production classes for the benefit of test code.
R1 outranks a preference for tidier tests, so the reflection stays and is
recorded here instead of being traded for a rule violation.

**The one suite where something published did reach it.** `TestCleanerUtil`
reached `CleanerUtil.unmapHackImpl()`, the body the first removed privileged
block wrapped, and did not need to: `UNMAP_SUPPORTED`,
`UNMAP_NOT_SUPPORTED_REASON` and `getCleaner()` are all public and all derived
from that one decision during class initialization, which any test in the class
triggers. The reflective call was removed and the assertions reformulated
through those three, with the supported branch additionally releasing a buffer
so that the published cleaner is asserted to work rather than merely to exist.
Nothing was given up in the exchange: a control that makes the decision fail
leaves 4 of that suite's 5 tests failing, with the recorded reason quoted in the
failure message.

**What none of it does.** No reflective read in any of these suites names
`SecurityManager`, `AccessControlContext`, `AccessController` or
`java.security.Policy`, and none re-enters a removed path or keeps one
reachable. R3's substance is untouched by them, and the static audit stands
exactly as recorded under
[Static Audit of SecurityManager-Era API](#Static_Audit_of_SecurityManager-Era_API).

### R6 in practice: the unwrap obligations

A task decorator changes the runtime type of the object the pool sees. Left
unaddressed, that silently changes operator-visible output — which R6 forbids
outright. Three obligations follow, and all three were found by reading the
code rather than by reasoning from the API:

* **`ExecutorHelper` fidelity site 1** — the `LOG.debug("afterExecute in thread: " … ", runnable type: " + r.getClass().getName())` statement at `ExecutorHelper.java:L37-38` as it stood at the base commit. Without unwrapping, every DEBUG line would have named the decorator instead of the real task type.
* **`ExecutorHelper` fidelity site 2** — the JDK-8071638 guard `if (t == null && r instanceof Future<?> && ((Future<?>) r).isDone())` at `ExecutorHelper.java:L46` at the base commit, preceded by its `// Handle JDK-8071638` comment. A decorator is a `Runnable`, not a `Future`, so this condition would have evaluated `false` for every pooled task, the `((Future<?>) r).get()` that follows would never have run, and the `LOG.warn("Caught exception in thread {}  + : ", …)` further down — whose doubled space is preserved verbatim — would have **silently stopped reporting task exceptions**. That is a regression with no compiler signal and no failing test.
* **Both `beforeExecute` overrides** — `HadoopThreadPoolExecutor.java:L82-83` and `HadoopScheduledThreadPoolExecutor.java:L61-62` at the base commit, byte-identical text logging `r.getClass().getName()`.

Two points are worth recording precisely, because they were missed in earlier
analysis and rediscovering them is expensive:

* **`ExecutorHelper` has two fidelity sites, not one.** Earlier analysis identified only the `instanceof Future<?>` guard. A single reassignment placed at the top of `logThrowableFromAfterExecute` covers both sites at once, which is how the fix is implemented; in the post-change file that reassignment is `r = SubjectPreservingTasks.unwrap(r);` at `ExecutorHelper.java:L36`, and it shifts the two sites to `:L38` and `:L47` respectively.
* **R6 is the *only* reason `ExecutorHelper.java` is in scope at all.** Nothing in the functional requirements would have identified it as needing a change.

One further R6 consequence: `HadoopExecutors.shutdown(...)` logs the executor
*object* itself (`"Gracefully shutting down executor service {}. Waiting max
{} {}"`, at `HadoopExecutors.java:L118-119` at the base commit). The new
forwarding executor-service types therefore delegate `toString()`, so that this
line continues to render the delegate rather than the wrapper. Operator-visible
thread names are likewise frozen — for example
`UserGroupInformation.java:L936` at the base commit sets
`"TGT Renewer for " + userName`, and that string is untouched.

A second R6 consequence follows from releasing a cancelled task's identity on
the strength of the cancellation alone, which requires this pool to make the
futures it queues rather than to inherit them. `HadoopThreadPoolExecutor`
therefore overrides both `newTaskFor` methods to return a `FutureTask` of its
own that reclaims its place on the queue once it is known to have been
cancelled. No log format changes, and the task *as submitted* is still what is
named wherever one was handed over directly, because `beforeExecute` still
unwraps first. What does change is the type named for a task submitted **for a
result**: the line reports
`HadoopThreadPoolExecutor$QueueReclaimingFutureTask` where it previously
reported `java.util.concurrent.FutureTask`. Both are the future the pool was
handed and the future its submitter holds, so the line still names the task
rather than the machinery around it, which is the property R6 protects here.
The change is also unavoidable under any formulation of the fix: noticing a
cancellation at all means controlling the future that reports it.

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
`org.apache.hadoop.security.authentication.util`; an earlier inventory omitted
it and consequently listed only nine of the ten files.

### Post-change state, and the single exemption

The audit now returns **9 matching lines in exactly 1 file**:
`hadoop-auth/src/main/java/org/apache/hadoop/security/authentication/util/SubjectUtil.java`.
**That file is the sole documented-shim exemption from the zero-hit
criterion**, and the exemption is recorded in its own javadoc so that anyone
running the audit finds the explanation next to the code.

Because `SubjectUtil` received a javadoc-only update in this change, it grew
from 410 to 516 lines and its residual hits moved. Both sets of line numbers
are given, so the audit is reproducible against either state:

| Nature | Post-change line | Base-commit line | Content |
|---|---|---|---|
| comment | L176 | L100 | `// For 22 and 23 the behavior actually depends on whether the SecurityManager` |
| comment | L181 | L105 | `// SecurityManager warnings to the console.` |
| javadoc | L230 | L154 | `* a method handle for Subject.getSubject(AccessController.getContext())` |
| comment | L238 | L162 | `// Subject.getSubject(AccessControlContext) is deprecated for removal and` |
| javadoc | L255 | L179 | `* Look up the method handle for Subject#getSubject(AccessControlContext)` |
| string literal | L267 | L191 | `.loadClass("java.security.AccessControlContext");` |
| javadoc | L276 | L200 | `* Look up the method handle for AccessController.getAccessControlContext()` |
| comment | L287 | L211 | `// AccessController.` |
| string literal | L291 | L215 | `.loadClass("java.security.AccessControlContext");` |

That is **7 comment or javadoc lines and 2 string-literal lines**. A **third**
`loadClass` literal — `.loadClass("java.security.AccessController");`, at
post-change `:L289` and base-commit `:L213` — names an SecurityManager-era
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

Separately, `hadoop-auth`'s `org/apache/hadoop/util/PlatformName.java` carried
`import java.security.AccessController;` at base-commit `:L21`, with
`import java.security.PrivilegedAction;` at `:L22` and `import
java.util.Arrays;` at `:L23`; the `PrivilegedAction` import is retained only
where it is still referenced after inlining.

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
**not** removed by JEP 486 and remain in wide legitimate use — **61**
occurrences in `hadoop-common` main source alone (26 of `PrivilegedAction` and
35 of `PrivilegedExceptionAction`), including the `@InterfaceAudience.Public`
signature `UserGroupInformation.doAs(PrivilegedAction<T>)`, which is preserved
unchanged. Their continued presence is correct and expected, and is **not** an
audit failure.

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
*de facto* exclusion, so none was added: no `@EnabledOnJre`, `@DisabledOnJre`,
`@EnabledForJreRange`, `@EnabledIf`, `@DisabledIf`, `assumeTrue` or
`assumingThat`. Guarding by runtime would also defeat the requirement that the
new regression tests pass with the **same assertions** on both JDK 17 and
JDK 25, which is what makes them meaningful: they assert propagation
*outcomes*, not the mechanism that produces them.

The observed test failures are handled the only way R5 allows — by root-cause
analysis and documentation. See
[Environmental Test Failures](#Environmental_Test_Failures).

Behavioural Resolution Register
-------------------------------

This register exists because R7 requires every ambiguous behavioural decision
to be documented. Four judgement calls arose. Each was resolved
**conservatively toward pre-JDK-18 semantics**, which is the tie-breaker R7
prescribes.

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

For the avoidance of doubt about where these sites live: the
`hadoop-common-project/hadoop-nfs` tree contains no `Executors.new*` and no
`EventLoopGroup` construction at all — it holds only `mount/` and `nfs/`
packages. Earlier analysis attributed these four sites to `hadoop-nfs`; they
are in `hadoop-common`, at the line numbers above.

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

### Other deliberate deferrals under R1

Recorded for the same reason: so that a later reader can tell a decision from
an omission. None of the following was changed.

* **The `require.test.libhadoop` build-configuration defect** — fully root-caused under [Environmental Test Failures](#Environmental_Test_Failures) and deliberately not fixed.
* **Three call sites that already route correctly** — `util/ShutdownHookManager.java:L80`, `hadoop-registry/.../RegistryAdminService.java:L113` and the cached-pool site in `hadoop-registry/.../RegistryDNS.java` (base-commit `:L172`, now `:L171`) already call `HadoopExecutors`, so the centralized fix reaches them automatically and none received a hunk. Editing them would be a change with no defect behind it. The first two files are untouched entirely; `RegistryDNS` was edited only at its *other* pool site, a raw `Executors.newSingleThreadExecutor()` at base-commit `:L1177`, which is why the cached-pool line above shifted by one.
* **The superseded capture idiom in `UserGroupInformation`** (base-commit `:L934-936`) is left in place; removing it is cleanup, not migration.
* **The `MakeAccessible` class** at `util/dynamic/DynMethods.java:529` is retained and simply invoked directly.
* **Cosmetic and typographic issues**, deliberately untouched: the "Kerbeors" spelling in a `UserGroupInformation` javadoc comment (base-commit `:L946`); the unused `import org.apache.hadoop.util.Shell;` at `TestSubjectPropagation.java:30`; the 114-character line at `util/Daemon.java:36`; and the three coexisting license-header styles inside `util/concurrent` — a clean form in `package-info.java`, a form carrying a stray ` * *` line in the seam classes, and a one-space `/**` form in `SubjectInheritingThread.java` — which are **not** normalized.
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

Four conflicts arose between the technical specifications, the seven
constraints, and the codebase as it actually is. Each is resolved here with its
justification.

### Conflict 1 - Reflection-guarded shims versus R3

The technical specifications permit SecurityManager-era API usage "inside
documented, reflection-guarded compatibility shims". **R3 forbids reflection
shims that keep SecurityManager-era code paths alive.**

**Resolution: R3 controls.** No SecurityManager-era code path is preserved
anywhere; every site is removed by inlining. The only reflection this change
puts to work in production is `SubjectUtil`'s cross-version dispatch of the
**replacement** API, which already existed; the pre-existing reflective code in
the other touched files is left exactly as it was, and the test-only reflective
reads that observe internal state this change publishes no other way are
registered under
[R3 in practice - test-only reflective access](#R3_in_practice_-_test-only_reflective_access).

**Justification.** The two statements reconcile once "shim" is read precisely.
The specifications' permission contemplates shims that keep **old** behaviour
reachable, which is exactly what R3 prohibits. `SubjectUtil` does the opposite:
it makes **new** behaviour reachable from old bytecode. Reading the permission
narrowly costs nothing here, because no site actually needed it, and it
satisfies the stricter of the two constraints — the correct posture whenever a
permission and a prohibition overlap.

Concretely, **this change introduces no reflective construct into production
code, and alters none that was already there**. The new `SubjectPreservingTasks` utility contains no
`MethodHandle`, no `java.lang.reflect` usage and no `java.lang.reflect.Proxy`; a
design based on `Proxy` over the executor interfaces was evaluated and rejected,
since dynamic dispatch would add per-call overhead where a compile-time
forwarding class costs nothing. Four of the new test suites do use reflection,
which is a different claim from the one this section makes: what they reach, why
nothing published reaches it, and the one suite where something published did
are set out under
[R3 in practice - test-only reflective access](#R3_in_practice_-_test-only_reflective_access).

### Conflict 2 - Compiler release 17 versus a JDK 18 API

The requirements permit raising `maven.compiler.release` beyond 17 only if a
JDK-25-only API is *unavoidable*. The replacement API this migration depends on
does not exist at release 17.

Verified evidence: the root `pom.xml` sets `<javac.version>17</javac.version>`
at `:L142`; the `jdk17+` profile activates on `<jdk>[17,)</jdk>` and sets
`maven.compiler.release` from that property — and **that range includes JDK
25**, so building on JDK 25 still targets release 17. Two measurements close
the question:

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
both JDK 17 and JDK 25**, unmodified. The only touch the directives would have
permitted was rewording those five comments; what was actually done with them
is recorded under
[Test-Source Comment Corrections](#Test-Source_Comment_Corrections).

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
conservative `false` is safe: it wraps where wrapping may not be strictly
necessary, which costs a little and breaks nothing.

### Conflict 5 - The planned identity fast paths versus the measured leak

The plan of record specifies two conditions under which the wrapping helper
returns its argument unchanged: when `SubjectUtil.THREAD_INHERITS_SUBJECT` is
`true`, described there as making the change a provable no-op on JDK 17, and
when the captured subject is `null`, described there as preventing a null
binding from masking an outer one. Both were implemented and both were then
**measured to be wrong for a pooled task**, so neither is present in the
delivered code.

**Resolution: the subject is read at every submission, on every runtime, and an
absent subject is carried across as deliberately as a present one.** The only
cases in which a task is handed back unchanged are the two that change nothing
at all: a `null` task, and a task already prepared by this same helper.

**Justification.**

* **`THREAD_INHERITS_SUBJECT` answers a different question.** It reports whether the runtime gives a *newly created* thread the identity of its creator. A pooled task does not cross that boundary: it reaches a worker that already exists. On a runtime where the flag is `true`, the identity such a worker was created with is precisely the stale one that a later submitter's task would observe, so consulting the flag hands the task to the very leak it is meant to prevent. Measured on JDK 17 with the fast path in place, on a one-worker pool: a task submitted by `bob` after a task submitted by `alice` observed **`alice`**, and a task submitted with no identity at all also observed **`alice`**. Running one user's work as another decides authorization and is what an audit record names, so this is a security defect and not a cosmetic one.
* **The plan's own mandated guarantee cannot hold otherwise.** The requirement that a worker reused across submitters run each task under its own submitter, and the regression tests that prove it, must pass on **both** runtimes. With the fast path restored they do not: a negative control run for this page, over the 97 tests of `org/apache/hadoop/util/concurrent` on JDK 17, produced **24** failures in three classes — 15 in `TestExecutorSubjectPropagation`, 8 in `TestExecutorRedirectSubjectPropagation`, 1 in `TestSubjectPreservingTasks` — among them a submission carrying no identity at all observing the *previous* submitter's identity on a retained worker, which is the leak itself rather than merely a missing subject. The only way to make them pass would be to weaken the assertions, which the rules forbid outright.
* **A null subject must be established, not skipped.** The masking argument holds for a task that may run on the thread that prepared it; it does not hold for a task handed to a worker that outlives it. Such a worker may hold an identity of its own — one created by `SubjectInheritingThread`, or copied in by a runtime that does so — and keeps it for as long as it lives. Leaving a subjectless submission alone therefore does not leave it with nothing: it leaves it with whatever its worker was holding. Measured on JDK 17: the subjectless submission above observed `alice`. Establishing the absence gives such a task what its submitter would have observed, which is nothing.
* **The masked-binding concern does not arise at this boundary.** Re-establishment goes through `SubjectUtil.doAs`, and every task prepared here is run by a worker at the top of its own call stack, so there is no enclosing binding on that thread for an absent identity to hide. Where an enclosing binding does exist — a task already prepared, one boundary forwarding to another — the already-prepared case returns the task untouched and nothing is established twice.
* **A worker is not even reliably given its creator's identity.** From JDK 21 a thread pool starts its workers through the thread container that owns them rather than through `Thread.start`, so the capture that `SubjectInheritingThread` performs on start does not run for a pool worker at all. What a worker holds is therefore not knowable by the code preparing a task, which is a second, independent reason for that code to establish the submission's identity unconditionally rather than deciding when it is needed.

**What this deviates from, stated plainly.** The plan's fast-path clause and its
claim that the change costs JDK 17 nothing at all are both superseded. The cost
on JDK 17 is now the same as on JDK 25: one small object per submission to a
Hadoop-owned pool, and one identity established per execution, bounded by the
number of tasks rather than by the work they do. Nothing else about JDK 17
behaviour changes — no wire format, no log format, no configuration, no public
signature — and the pooled-task identity that changes there changes from the
wrong user to the right one.

**Surface added beyond the plan, and why each is required by the above.** Each
addition below was needed to make the resolution correct rather than merely
present, and each is confined to the seam:

* `wrapEach(Collection)` — the bulk submission methods (`invokeAll`, `invokeAny`) set their own deadline and decide for themselves when to hand over the next task. Preparing their tasks by reading the collection through beforehand would move work in front of the deadline and hand every task over at once, so the collection is passed on as a view that prepares each task as the method reaches it.
* `unwrapAll(List)` — a pool stopped at once owes its caller the tasks it never started, as they were submitted. Returning the prepared forms would hand the caller objects it never passed in.
* `ValueQueue`'s refill task prepares itself — a refill is put straight into the backing queue of the filler pool rather than submitted through it, so it never passes the point that prepares a submission. It therefore reads the identity in its own constructor, on the thread whose request made the refill necessary. What goes into the queue is still the same object, so key de-duplication, cancellation and removal by object identity are unaffected.
* **A prepared task names the task that was submitted.** Both task wrappers delegate `toString()`. Not everything that describes a queued task gets the chance to unwrap it first: a pool that turns a submission away names the task in the exception it throws, and that message reaches an operator. This is the same obligation as the unwrap sites recorded under *R6 in practice: the unwrap obligations* above, met where unwrapping is not available to the code doing the describing.
* **Removal, sweeping and an immediate shutdown still work on the task as submitted.** `HadoopThreadPoolExecutor` overrides `remove(Runnable)`, `purge()` and `shutdownNow()` so that a caller naming the task it submitted still finds it on the queue, a cancelled task is still swept off, and a pool stopped at once still hands back what it was given. The plan of record closes this question the other way, by recording that task-identity operations are provably unused because main source contains no `executor.remove(task)` call; that evidence is **wrong**, and the correction is worth stating because the conclusion drawn from it was that no such override is needed. `ValueQueue.drain(String)` calls `executor.remove(e)` for each queued refill it deletes — `ValueQueue.java:L317` as the base commit stands — and these are in any case public methods of a pool this project hands out to callers it does not control. Reclaiming a cancelled entry matters for a second reason as well: an entry left on a queue keeps the subject captured for it, and the credentials in it, reachable for as long as it stays there.
* **A cancelled task lets go of its identity on the strength of the cancellation alone.** Sweeping the queue only reclaims a cancelled entry when somebody asks for a sweep, and nothing in this project asks: a caller that cancels its work and walks away would leave its own subject, and the credentials in it, reachable through the queued entry until something else displaced it — on a pool whose workers are all occupied, or whose queue has stopped draining, never. `HadoopThreadPoolExecutor` therefore also overrides `newTaskFor(Callable)` and `newTaskFor(Runnable, T)`, returning a `FutureTask` of its own that reclaims its place on the queue, through the same `remove(Runnable)` that recognises a prepared task, once it is known to have been cancelled. Nothing is prepared or captured there, so a submission still passes exactly one preparing point and a prepared task still has exactly one layer to be taken off; the plan of record's instruction not to override `newTaskFor` exists to prevent a second layer, and no second layer is added. Its one operator-visible consequence is recorded under *R6 in practice: the unwrap obligations* above. `purge()` is kept for what this cannot reach — a future the caller made itself and handed over as a plain task, which this pool is never told about — rather than as the only thing that reclaims anything. **Every other executor here obtains the same release by construction**: the prepared task is what the cancelled future was given to run, and a future that has completed, cancellation included, lets go of what it was given (`FutureTask.finishCompletion` clears its `callable` after signalling completion, identically on JDK 17 and JDK 25). Only this pool prepares a task *around* a future it was handed, and that is why only this pool needs the override.
* **An identity already in force is not established a second time.** A task can be prepared at more than one boundary — one executor forwarding to another, each adding decoration of its own in between, so that neither can tell by looking that the other has already been there — and every such preparation captured the same identity from the same submitting thread. All but the outermost would therefore establish an identity that is already established, which costs a scoped binding held for the whole of the task and is paid on every run. Each wrapper compares the identity it captured against the one in force, by reference, and runs its task directly when they are the same object; comparing on equality alone would leave a different instance in force from the one that was captured. `SemaphoredDelegatingExecutor` likewise leaves preparation to a delegate that already performs it, rather than adding a layer of its own.

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
| `security/**` + `util/**` — 140 test classes | 1059 run / 13 failures / 0 errors / 36 skipped | 1059 / 13 / 0 / 36 |
| `io/**` + `metrics2/**` + `crypto/key` — 115 test classes | 998 run / 1 failure / 0 errors / 155 skipped | 998 / 1 / 0 / 155 |

The failing classes are the same five on both runtimes, with the same failing
methods. A failure that behaves identically on both cannot have been introduced
by moving to JDK 25. This differential is the evidence, and it is the reason no
exclusion was needed to reach a defensible result.

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
| Propagation-seam suites, `util/concurrent` — 6 classes | 97 run / 0 failures / 0 errors / 0 skipped | 97 / 0 / 0 / 0 |
| `security/**` + `util/**` slice — 140 test classes | 1059 run / 13 failures / 0 errors / 36 skipped | 1059 / 13 / 0 / 36 |
| `io/**` + `metrics2/**` + `crypto/key` slice — 115 test classes | 998 run / 1 failure / 0 errors / 155 skipped | 998 / 1 / 0 / 155 |
| `hadoop-auth` full suite | 183 run / 0 failures / 0 errors / 0 skipped | 183 / 0 / 0 / 0 |
| `hadoop-kms` full suite | 51 run / 0 failures / 0 errors / 0 skipped | 51 / 0 / 0 / 0 |
| `hadoop-registry` full suite | 166 run / 0 failures / 0 errors / 0 skipped | 166 / 0 / 0 / 0 |
| `hadoop-nfs` full suite | 21 run / 0 failures / 0 errors / 0 skipped | 21 / 0 / 0 / 0 |

Every failure in those totals is accounted for above: 13 uid-0 failures in the
first slice, 1 in the second, and nothing anywhere else. Omitting
`-Drequire.test.libhadoop=false` adds exactly one more, in
`util/TestNativeCodeLoader`, on either runtime.

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
[Conflict 5](#Conflict_5_-_The_planned_identity_fast_paths_versus_the_measured_leak); `java/util/concurrent/AbstractExecutorService.java`,
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
completes in roughly **1.2 to 3.2 seconds** and builds only **four** modules —
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
`[ERROR]` lines, 02:24 min against a cold local repository and well under a
minute against a warm one, on JDK 25 and on JDK 17 alike.

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

**Two beliefs were refuted by measurement and must not be reintroduced.** First,
that a Mockito or `byte-buddy` upgrade is required — it is not; the existing
pin works, and the tree contains **zero** `mockStatic` and **zero**
`mockConstruction` usages, so no dynamic-agent self-attachment is needed at
all. Second, that plugin version bumps are required — they are not. Acting on
either belief would introduce hunks that trace to no defect, violating R1.

The propagation design draws only on `java.util.concurrent`,
`java.util.Objects`, `java.security.PrivilegedAction` and its relatives,
`javax.security.auth.Subject`, and the existing `SubjectUtil`. **No**
context-propagation library was added — no Micrometer context propagation, no
OpenTelemetry context, no extension of Guava's `ThreadFactoryBuilder`, and no
home-grown `ThreadLocal` framework. Verified frozen and unchanged against the
base commit: the root `pom.xml`, `hadoop-project/pom.xml`, and all five
`hadoop-common-project/*/pom.xml` module POMs.

### Two build-configuration facts worth knowing

* **The enforcer's import bans run before compilation.** `maven-enforcer-plugin` 3.5.0 with `restrict-imports-enforcer-rule` 2.0.0 runs execution `banned-illegal-imports` (root `pom.xml:L206`) in the **`process-sources`** phase (`:L207`, goal at `:L209`). The "Use JUnit5" block at `:L336-346` sets `<includeTestCode>true</includeTestCode>` (`:L337`), bans `org.junit.**` (`:L340`) and allows only `org.junit.jupiter.**` (`:L343`) and `org.junit.platform.**` (`:L344`). A single `org.junit.Test` import would fail the build before any compilation happens, which is why every test class added here is JUnit 5 Jupiter throughout.
* **`maven-javadoc-plugin` never runs by default.** Its `module-javadocs` execution (`hadoop-project/pom.xml:L2740`) is bound to `package` (`:L2741`, goal at `:L2743`) inside the `dist` profile (`:L2731`), and that profile has **no `<activation>` block**, so it requires an explicit `-Pdist`. Its JDK 25 behaviour is therefore unverified — a release-engineering concern rather than a build one, and recorded as an accepted residual risk. Note that `<doclint>all</doclint>` at `:L2318`, with `<additionalOption>-Xmaxwarns 10000</additionalOption>` at `:L2320`, still governs any javadoc run that *is* invoked.

### Related documentation

* [Apache Hadoop Compatibility](./Compatibility.html) — the compatibility policies this change is required to uphold
* [Hadoop Interface Taxonomy](./InterfaceClassification.html) — the audience and stability classifications referenced above
