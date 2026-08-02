/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.util.concurrent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;

import org.apache.hadoop.crypto.key.kms.ValueQueue;
import org.apache.hadoop.crypto.key.kms.ValueQueue.SyncGenerationPolicy;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.security.authentication.util.SubjectUtil;
import org.apache.hadoop.util.BlockingThreadPoolExecutorService;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.SemaphoredDelegatingExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.function.Executable;

/**
 * Asserts that a task handed to one of Hadoop's own executors runs under the
 * JAAS subject of the thread that submitted it.
 * <p>
 * A pool worker outlives the tasks it runs and serves one submitter after
 * another, so an identity read when the worker was created belongs to whoever
 * happened to cause it to exist rather than to the submitter of the task at
 * hand. These tests therefore submit from a known identity and assert on the
 * identity the task itself observed, covering every factory in
 * {@link HadoopExecutors}, every submission entry point of
 * {@link HadoopThreadPoolExecutor} and
 * {@link HadoopScheduledThreadPoolExecutor}, and the executors in
 * {@code org.apache.hadoop.util} that decorate the tasks they forward.
 * <p>
 * Three cases carry the weight of the suite. A pool that runs one submitter's
 * task and then another's has to run each under its own submitter, which a
 * test using a single identity would not notice. A submission made with no
 * identity at all has to run with no identity of its own, rather than under
 * whatever identity the worker it lands on was left holding, because running
 * one caller's work as another user decides authorization and is what an audit
 * record names. And a task that fails has to reach its future as the exception
 * it threw, with nothing wrapping it.
 * <p>
 * Every assertion here is about an observed identity rather than about how it
 * was carried, so the same assertions hold on every runtime this project
 * supports: where the runtime hands a new thread the identity of its creator
 * and where it hands over nothing, a submitted task observes its submitter
 * either way. Most assertions use a pool created for them, so that no test is
 * answered by work another left behind; the assertions about a reused worker
 * deliberately do the opposite and submit both identities into one pool,
 * because sharing the worker is the whole of what they test. Identities are
 * told apart by their own distinct principal and compared by reference, so
 * that two distinct identities can never satisfy an assertion meant for one of
 * them. Every observation crosses a thread boundary through a future or a
 * latch, and every executor these tests create is shut down afterwards.
 */
public class TestExecutorSubjectPropagation {

  /** Bound on every wait, generous enough to survive a loaded build host. */
  private static final long TIMEOUT_SECONDS = 10;

  /** Interval between the runs of the periodic tasks, in milliseconds. */
  private static final long PERIOD_MILLIS = 20;

  /**
   * How long a hand-written worker waits for work before looking again, in
   * milliseconds. Short enough that a shutdown is noticed promptly even with
   * nothing queued to interrupt.
   */
  private static final long POLL_MILLIS = 20;

  /**
   * How long a deliberately slow collection takes over each task it yields, in
   * milliseconds.
   * <p>
   * One such task takes longer to read than {@link #SHORT_WAIT_MILLIS} allows,
   * so a wait that covers the reading is spent before the second task is
   * reached, while a wait that starts after the reading has all of itself left.
   */
  private static final long SLOW_READ_MILLIS = 400;

  /**
   * The wait given to a timed bulk submission over a slow collection, in
   * milliseconds.
   */
  private static final long SHORT_WAIT_MILLIS = 500;

  /** How many tasks the bulk submissions over a slow collection are given. */
  private static final int SLOW_READ_TASKS = 4;

  /** Result a task returns to prove it ran to completion. */
  private static final String SENTINEL = "task-completed";

  /** What a task calls itself, for assertions about how it is named. */
  private static final String REJECTED_TASK_NAME = "the-task-that-was-refused";

  /**
   * Released when a test ends, freeing any worker a test deliberately tied up.
   * <p>
   * Several assertions here are about a task that is still waiting, which needs
   * the pool's only worker to be busy with something else. That something has to
   * outlast the assertion and end when the test does, or the pool would not shut
   * down.
   */
  private final CountDownLatch releaseOccupiedWorkers = new CountDownLatch(1);

  /** Every executor created by a test, shut down when the test ends. */
  private final List<ExecutorService> pools = new ArrayList<>();

  /**
   * Records an executor so that it is shut down when the test ends.
   *
   * @param <E> the executor's own type, so that a caller keeps it
   * @param pool the executor to shut down later
   * @return {@code pool}
   */
  private <E extends ExecutorService> E register(E pool) {
    pools.add(pool);
    return pool;
  }

  /**
   * Shuts down every executor a test created and waits for each to finish.
   *
   * @throws InterruptedException if this thread is interrupted while waiting
   */
  @AfterEach
  public void shutDownPools() throws InterruptedException {
    releaseOccupiedWorkers.countDown();
    List<ExecutorService> registered = new ArrayList<>(pools);
    pools.clear();
    for (ExecutorService pool : registered) {
      pool.shutdownNow();
    }
    for (ExecutorService pool : registered) {
      assertTrue(pool.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS),
          "an executor created by this test did not terminate");
    }
  }

  /**
   * Returns a subject that no other subject can be mistaken for.
   * <p>
   * A {@link Subject} compares equal to another whose principals and
   * credentials match, so two subjects with nothing in them are equal to each
   * other. Giving each one a principal of its own is what lets an assertion
   * distinguish two identities; the principal name is given in full so that
   * naming it needs no realm configuration.
   *
   * @param principalName a complete principal name, realm included
   * @return a subject holding exactly that principal
   */
  private static Subject newSubject(String principalName) {
    Subject subject = new Subject();
    subject.getPrincipals().add(new KerberosPrincipal(principalName));
    return subject;
  }

  /**
   * Returns a task that reports the subject of the thread that runs it.
   *
   * @return a task yielding the running thread's subject, which may be
   *         {@code null}
   */
  private static Callable<Subject> currentSubject() {
    return new Callable<Subject>() {
      @Override
      public Subject call() {
        return SubjectUtil.current();
      }
    };
  }

  /**
   * Submits a task reporting its own subject to {@code pool}, from inside
   * {@code submitter}'s scope, and returns what that task observed.
   * <p>
   * Only the submission happens inside the scope. Waiting for the result
   * afterwards is what shows that the identity travelled with the task rather
   * than with the stack that submitted it, and the wait itself is what makes
   * the observation visible to this thread.
   *
   * @param submitter the identity to submit under
   * @param pool the executor to submit to
   * @return the subject the task observed, which may be {@code null}
   * @throws Exception if the task fails or the wait times out
   */
  private Subject submissionObserves(final Subject submitter,
      final ExecutorService pool) throws Exception {
    final AtomicReference<Future<Subject>> submitted = new AtomicReference<>();
    SubjectUtil.callAs(submitter, new Callable<Void>() {
      public Void call() {
        submitted.set(pool.submit(currentSubject()));
        return (Void) null;
      }
    });
    return submitted.get().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  /**
   * Submits a task reporting both its own subject and the worker that ran it
   * to {@code pool}, from inside {@code submitter}'s scope.
   * <p>
   * This is {@link #submissionObserves(Subject, ExecutorService)} with the
   * worker reported alongside the identity, so that a test asserting on what a
   * <em>reused</em> worker observed can first establish that the worker really
   * was the same one. Without that, a pool free to replace an idle worker
   * between two submissions would quietly turn the assertion into one about a
   * fresh worker and stop covering reuse at all.
   *
   * @param submitter the identity to submit under
   * @param pool the executor to submit to
   * @return what the task observed
   * @throws Exception if the task fails or the wait times out
   */
  private Observation observeSubmission(final Subject submitter,
      final ExecutorService pool) throws Exception {
    final AtomicReference<Future<Observation>> submitted =
        new AtomicReference<>();
    SubjectUtil.callAs(submitter, new Callable<Void>() {
      @Override
      public Void call() {
        submitted.set(pool.submit(new Callable<Observation>() {
          @Override
          public Observation call() {
            return new Observation(SubjectUtil.current(),
                Thread.currentThread());
          }
        }));
        return (Void) null;
      }
    });
    return submitted.get().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  /**
   * The identity a task observed and the worker thread that ran it.
   */
  private static final class Observation {

    /** The subject in force inside the task; may be {@code null}. */
    private final Subject subject;

    /** The thread the task ran on. */
    private final Thread worker;

    Observation(Subject subject, Thread worker) {
      this.subject = subject;
      this.worker = worker;
    }

    /**
     * Returns the identity in force inside the task.
     *
     * @return the observed subject, or {@code null} if there was none
     */
    Subject subject() {
      return subject;
    }

    /**
     * Returns the worker that ran the task.
     *
     * @return the thread the task ran on
     */
    Thread worker() {
      return worker;
    }

    /**
     * Returns the worker that ran the task, for the assertions that read it
     * under that name.
     *
     * @return the thread the task ran on
     */
    Thread thread() {
      return worker;
    }
  }

  /**
   * Asserts what a task submitted by a second identity observed on a worker a
   * first identity had already used.
   * <p>
   * The identity is carried by reading it at each submission, so the later task
   * observes its own submitter and demonstrably not the earlier one: that is the
   * whole point of reading it per submission rather than once per worker, since
   * running one submitter's work as another user decides authorization and is
   * what an audit record names. The assertion holds on every runtime, because
   * the identity a worker was given when it was created is never consulted.
   *
   * @param first the identity that submitted before
   * @param second the identity that submitted after
   * @param observed what the second identity's task observed
   */
  private static void assertSecondSubmitterObserved(Subject first,
      Subject second, Subject observed) {
    assertSame(second, observed,
        "a task run by a worker an earlier identity had already used did "
            + "not observe its own submitter");
    assertNotSame(first, observed,
        "a worker kept across submissions ran one submitter's task under an "
            + "earlier submitter's identity");
  }

  /**
   * Hands a task reporting its own subject to {@link ExecutorService#execute},
   * from inside {@code submitter}'s scope, and returns what that task
   * observed.
   * <p>
   * This entry point yields no future, so the task publishes its observation
   * through a latch instead.
   *
   * @param submitter the identity to submit under
   * @param pool the executor to hand the task to
   * @return the subject the task observed, which may be {@code null}
   * @throws InterruptedException if this thread is interrupted while waiting
   */
  private Subject executionObserves(final Subject submitter,
      final ExecutorService pool) throws InterruptedException {
    final SubjectRecorder recorder = new SubjectRecorder(1);
    SubjectUtil.callAs(submitter, new Callable<Void>() {
      public Void call() {
        pool.execute(recorder);
        return (Void) null;
      }
    });
    return awaitSingleRun(recorder);
  }

  /**
   * Submits a task reporting both its own subject and the worker running it,
   * from inside {@code submitter}'s scope, and returns what it reported.
   * <p>
   * A {@code null} submitter stands for a submission made with no identity at
   * all, which is made from this thread as it is rather than from a scope
   * establishing nothing, because that is the form such a submission takes in
   * practice.
   *
   * @param submitter the identity to submit under, or {@code null} to submit
   *                  with none
   * @param pool the executor to submit to
   * @return the subject the task observed and the thread that ran it
   * @throws Exception if the task fails or the wait times out
   */
  private RunObservation submissionObservation(final Subject submitter,
      final ExecutorService pool) throws Exception {
    final AtomicReference<Future<RunObservation>> submitted =
        new AtomicReference<>();
    Callable<Void> submission = new Callable<Void>() {
      @Override
      public Void call() {
        submitted.set(pool.submit(new Callable<RunObservation>() {
          @Override
          public RunObservation call() {
            return new RunObservation(SubjectUtil.current(),
                Thread.currentThread());
          }
        }));
        return (Void) null;
      }
    };
    if (submitter == null) {
      submission.call();
    } else {
      SubjectUtil.callAs(submitter, submission);
    }
    return submitted.get().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  /**
   * Asserts that one worker of {@code pool}, used for three submissions in
   * turn, runs each of them under its own submitter.
   * <p>
   * The submissions are made by one identity, then by another, then by none at
   * all, and each is waited for before the next so that they queue up behind
   * the same worker instead of spreading over several. That the worker was
   * indeed the same one every time is asserted first: without it the identity
   * assertions could be satisfied by a worker that had never served an earlier
   * submitter and so had nothing to leak.
   *
   * @param pool a pool of exactly one worker, kept for the whole of this
   * @param what a word naming this pool, so that a failure says which one
   * @throws Exception if a task fails or a wait times out
   */
  private void assertReusedWorkerObservesEachSubmitter(ExecutorService pool,
      String what) throws Exception {
    Subject first = newSubject("first-" + what + "@EXAMPLE.COM");
    Subject second = newSubject("second-" + what + "@EXAMPLE.COM");

    RunObservation byFirst = submissionObservation(first, pool);
    RunObservation bySecond = submissionObservation(second, pool);
    RunObservation byNobody = submissionObservation(null, pool);

    assertSame(byFirst.thread, bySecond.thread,
        "the second task did not run on the worker the first had used, so "
            + "that worker was left holding nothing for it to observe");
    assertSame(byFirst.thread, byNobody.thread,
        "the third task did not run on the worker the others had used, so "
            + "that worker was left holding nothing for it to observe");
    assertSame(first, byFirst.subject,
        "the first task did not observe the identity that submitted it");
    assertSame(second, bySecond.subject,
        "a task run by a worker an earlier identity had used observed that "
            + "earlier identity rather than its own submitter");
    assertNull(byNobody.subject,
        "a task submitted with no identity at all observed an identity that "
            + "an earlier submitter had left on the worker");
  }

  /**
   * Waits for a task that runs once and returns the subject it observed.
   *
   * @param recorder the task to wait for
   * @return the subject that single run observed, which may be {@code null}
   * @throws InterruptedException if this thread is interrupted while waiting
   */
  private Subject awaitSingleRun(SubjectRecorder recorder)
      throws InterruptedException {
    assertTrue(recorder.awaitRuns(), "the task never ran");
    List<Subject> observed = recorder.observed();
    assertEquals(1, observed.size(), "the task ran more than once");
    return observed.get(0);
  }

  /**
   * Asserts that every run of a repeating task observed {@code expected}.
   *
   * @param expected the identity every run has to observe
   * @param recorder the repeating task to wait for
   * @param runs how many runs to wait for
   * @throws InterruptedException if this thread is interrupted while waiting
   */
  private void assertEveryRunObserved(Subject expected,
      SubjectRecorder recorder, int runs) throws InterruptedException {
    assertTrue(recorder.awaitRuns(),
        "the repeating task ran fewer than " + runs + " times");
    List<Subject> observed = recorder.observed();
    assertTrue(observed.size() >= runs,
        "the repeating task ran fewer than " + runs + " times");
    for (Subject seen : observed) {
      assertSame(expected, seen,
          "a run of the repeating task did not observe the subject it was "
              + "scheduled under");
    }
  }

  /**
   * A cached pool runs its task under the subject that submitted it.
   * <p>
   * This factory takes a thread factory and has no overload that does not, so
   * the workers are plain threads made here.
   *
   * @throws Exception if a task fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testCachedThreadPoolRunsTaskUnderSubmitterSubject()
      throws Exception {
    Subject submitter = newSubject("cached@EXAMPLE.COM");
    ExecutorService pool = register(HadoopExecutors.newCachedThreadPool(
        new PlainDaemonThreadFactory("cached")));

    assertSame(submitter, submissionObserves(submitter, pool),
        "a task submitted to a cached pool did not observe its submitter");
  }

  /**
   * A fixed pool runs its task under the subject that submitted it, whether
   * the pool was given a thread factory or left to make its own threads.
   *
   * @throws Exception if a task fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testFixedThreadPoolRunsTaskUnderSubmitterSubject()
      throws Exception {
    Subject submitter = newSubject("fixed@EXAMPLE.COM");

    ExecutorService defaultThreads =
        register(HadoopExecutors.newFixedThreadPool(2));
    assertSame(submitter, submissionObserves(submitter, defaultThreads),
        "a task submitted to a fixed pool did not observe its submitter");

    ExecutorService namedThreads = register(HadoopExecutors.newFixedThreadPool(
        2, new PlainDaemonThreadFactory("fixed")));
    assertSame(submitter, submissionObserves(submitter, namedThreads),
        "a task submitted to a fixed pool with a thread factory did not "
            + "observe its submitter");
  }

  /**
   * A single-thread executor runs its task under the subject that submitted
   * it, through each of the ways it can be given one.
   * <p>
   * This factory hands back the runtime's own single-thread executor with its
   * particular semantics left alone, so each of its submission methods is
   * exercised in turn.
   *
   * @throws Exception if a task fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testSingleThreadExecutorRunsTaskUnderSubmitterSubject()
      throws Exception {
    Subject submitter = newSubject("single@EXAMPLE.COM");

    assertSame(submitter, submissionObserves(submitter,
        register(HadoopExecutors.newSingleThreadExecutor())),
        "a task submitted to a single-thread executor did not observe its "
            + "submitter");

    assertSame(submitter, submissionObserves(submitter,
        register(HadoopExecutors.newSingleThreadExecutor(
            new PlainDaemonThreadFactory("single")))),
        "a task submitted to a single-thread executor with a thread factory "
            + "did not observe its submitter");

    assertSame(submitter, executionObserves(submitter,
        register(HadoopExecutors.newSingleThreadExecutor())),
        "a task handed to execute did not observe its submitter");

    final ExecutorService forRunnable =
        register(HadoopExecutors.newSingleThreadExecutor());
    final SubjectRecorder recorder = new SubjectRecorder(1);
    final AtomicReference<Future<?>> plain = new AtomicReference<>();
    final AtomicReference<Future<String>> withResult = new AtomicReference<>();
    final SubjectRecorder recorderWithResult = new SubjectRecorder(1);
    SubjectUtil.callAs(submitter, new Callable<Void>() {
      public Void call() {
        plain.set(forRunnable.submit(recorder));
        withResult.set(forRunnable.submit(recorderWithResult, SENTINEL));
        return (Void) null;
      }
    });
    plain.get().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    assertSame(submitter, awaitSingleRun(recorder),
        "a runnable submitted for a future did not observe its submitter");
    assertEquals(SENTINEL,
        withResult.get().get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
        "a runnable submitted with a result did not yield that result");
    assertSame(submitter, awaitSingleRun(recorderWithResult),
        "a runnable submitted with a result did not observe its submitter");
  }

  /**
   * A single-thread executor runs the tasks of a bulk submission under the
   * subject that submitted them.
   * <p>
   * Both bulk methods wait for the tasks they submit, so both are called from
   * inside the scope that establishes the identity.
   *
   * @throws Exception if a task fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testBulkSubmissionRunsTasksUnderSubmitterSubject()
      throws Exception {
    Subject submitter = newSubject("bulk@EXAMPLE.COM");
    final ExecutorService invokeAllPool =
        register(HadoopExecutors.newSingleThreadExecutor());
    final ExecutorService invokeAnyPool =
        register(HadoopExecutors.newSingleThreadExecutor());
    final AtomicReference<List<Future<Subject>>> allResults =
        new AtomicReference<>();
    final AtomicReference<Subject> anyResult = new AtomicReference<>();

    SubjectUtil.callAs(submitter, new Callable<Void>() {
      public Void call() throws Exception {
        List<Callable<Subject>> two = new ArrayList<>();
        two.add(currentSubject());
        two.add(currentSubject());
        allResults.set(invokeAllPool.invokeAll(two));
        List<Callable<Subject>> one = new ArrayList<>();
        one.add(currentSubject());
        anyResult.set(invokeAnyPool.invokeAny(one));
        return (Void) null;
      }
    });

    List<Future<Subject>> completed = allResults.get();
    assertEquals(2, completed.size(),
        "invokeAll did not return a result for every task");
    for (Future<Subject> result : completed) {
      assertSame(submitter, result.get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
          "a task submitted through invokeAll did not observe its submitter");
    }
    assertSame(submitter, anyResult.get(),
        "a task submitted through invokeAny did not observe its submitter");
  }

  /**
   * The timed form of each bulk method runs its tasks under the subject that
   * submitted them.
   * <p>
   * A wait of its own is all that separates these two methods from the pair
   * above, and it must not cost a task its identity.
   *
   * @throws Exception if a task fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testTimedBulkSubmissionRunsTasksUnderSubmitterSubject()
      throws Exception {
    Subject submitter = newSubject("timed-bulk@EXAMPLE.COM");
    final ExecutorService invokeAllPool =
        register(HadoopExecutors.newSingleThreadExecutor());
    final ExecutorService invokeAnyPool =
        register(HadoopExecutors.newSingleThreadExecutor());
    final AtomicReference<List<Future<Subject>>> allResults =
        new AtomicReference<>();
    final AtomicReference<Subject> anyResult = new AtomicReference<>();

    SubjectUtil.callAs(submitter, new Callable<Void>() {
      public Void call() throws Exception {
        List<Callable<Subject>> two = new ArrayList<>();
        two.add(currentSubject());
        two.add(currentSubject());
        allResults.set(invokeAllPool.invokeAll(two, TIMEOUT_SECONDS,
            TimeUnit.SECONDS));
        List<Callable<Subject>> one = new ArrayList<>();
        one.add(currentSubject());
        anyResult.set(invokeAnyPool.invokeAny(one, TIMEOUT_SECONDS,
            TimeUnit.SECONDS));
        return (Void) null;
      }
    });

    List<Future<Subject>> completed = allResults.get();
    assertEquals(2, completed.size(),
        "the timed invokeAll did not return a result for every task");
    for (Future<Subject> result : completed) {
      assertSame(submitter, result.get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
          "a task submitted through the timed invokeAll did not observe its "
              + "submitter");
    }
    assertSame(submitter, anyResult.get(),
        "a task submitted through the timed invokeAny did not observe its "
            + "submitter");
  }

  /**
   * A timed bulk submission spends its wait on its tasks and not on being made
   * ready for them.
   * <p>
   * The wait of a timed {@code invokeAll} begins when it is called and covers
   * the reading of the collection it was given, so a collection slow to read
   * leaves less of that wait for the tasks themselves. Reading the whole
   * collection through beforehand would move that reading outside the wait and
   * so grant the submission its wait in full afterwards, letting tasks run that
   * a wait already spent is meant to leave alone. Handing the collection over
   * instead keeps the reading inside the wait, which is what this asserts: a
   * collection slower to read than the wait allows leaves tasks unrun and their
   * futures cancelled.
   *
   * @throws Exception if a wait is interrupted
   */
  @Test
  @Timeout(value = 60)
  public void testTimedBulkSubmissionSpendsItsWaitOnItsTasks()
      throws Exception {
    final ExecutorService pool =
        register(HadoopExecutors.newSingleThreadExecutor());
    final AtomicInteger ran = new AtomicInteger();
    List<Callable<String>> tasks = new ArrayList<>();
    for (int i = 0; i < SLOW_READ_TASKS; i++) {
      tasks.add(new Callable<String>() {
        @Override
        public String call() {
          ran.incrementAndGet();
          return SENTINEL;
        }
      });
    }

    List<Future<String>> results = pool.invokeAll(
        new SlowlyReadTasks(tasks, SLOW_READ_MILLIS), SHORT_WAIT_MILLIS,
        TimeUnit.MILLISECONDS);

    assertEquals(SLOW_READ_TASKS, results.size(),
        "a timed invokeAll did not return a result for every task it was "
            + "given");
    assertTrue(ran.get() < SLOW_READ_TASKS,
        "a timed invokeAll ran every one of its tasks although reading them "
            + "had already spent the whole of its wait");
    assertTrue(results.get(results.size() - 1).isCancelled(),
        "a timed invokeAll left its last task uncancelled although its wait "
            + "was spent before that task could be handed over");
  }

  /**
   * A bulk submission for a single result hands over no more tasks than it
   * needs.
   * <p>
   * {@code invokeAny} hands over one task and takes another only while none has
   * finished, so it reads its collection a task at a time and interleaves that
   * reading with the running of what it has handed over already. Reading the
   * whole collection through beforehand would read every task before the first
   * was handed over, so this asserts the interleaving itself: the second task is
   * not read until the first is running.
   *
   * @throws Exception if a task fails or a wait times out
   */
  @Test
  @Timeout(value = 60)
  public void testBulkSubmissionForOneResultHandsOverOneTaskAtATime()
      throws Exception {
    final ExecutorService pool =
        register(HadoopExecutors.newSingleThreadExecutor());
    final CountDownLatch firstTaskRunning = new CountDownLatch(1);
    final CountDownLatch secondTaskRead = new CountDownLatch(1);
    final AtomicBoolean readWhileFirstRan = new AtomicBoolean();
    List<Callable<String>> tasks = new ArrayList<>();
    tasks.add(new Callable<String>() {
      @Override
      public String call() throws InterruptedException {
        firstTaskRunning.countDown();
        secondTaskRead.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        return SENTINEL;
      }
    });
    tasks.add(new Callable<String>() {
      @Override
      public String call() {
        return SENTINEL;
      }
    });

    String result = pool.invokeAny(new GatedTasks(tasks, firstTaskRunning,
        secondTaskRead, readWhileFirstRan));

    assertEquals(SENTINEL, result,
        "invokeAny did not return the result of a task");
    assertTrue(readWhileFirstRan.get(),
        "invokeAny reached its second task before its first was running, so "
            + "every task had been read before any was handed over");
  }

  /**
   * A bulk submission treats the collection it is given exactly as the executor
   * it wraps would.
   * <p>
   * What reaches that executor holds no tasks of its own: it reports the size of
   * the collection behind it and yields that collection's tasks in its order,
   * and a collection that would have been refused is still refused. A caller
   * cannot tell from any of that whether anything stood in between.
   *
   * @throws Exception if a task fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testBulkSubmissionTreatsItsCollectionAsTheWrappedExecutorWould()
      throws Exception {
    final ExecutorService pool =
        register(HadoopExecutors.newSingleThreadExecutor());
    final List<Callable<String>> none = null;
    final List<Callable<String>> empty = new ArrayList<>();

    assertThrows(NullPointerException.class, new Executable() {
      public void execute() throws Exception {
        pool.invokeAll(none);
      }
    }, "invokeAll accepted no collection at all");
    assertThrows(NullPointerException.class, new Executable() {
      public void execute() throws Exception {
        pool.invokeAll(none, TIMEOUT_SECONDS, TimeUnit.SECONDS);
      }
    }, "the timed invokeAll accepted no collection at all");
    assertThrows(NullPointerException.class, new Executable() {
      public void execute() throws Exception {
        pool.invokeAny(none);
      }
    }, "invokeAny accepted no collection at all");
    assertThrows(NullPointerException.class, new Executable() {
      public void execute() throws Exception {
        pool.invokeAny(none, TIMEOUT_SECONDS, TimeUnit.SECONDS);
      }
    }, "the timed invokeAny accepted no collection at all");
    assertThrows(IllegalArgumentException.class, new Executable() {
      public void execute() throws Exception {
        pool.invokeAny(empty);
      }
    }, "invokeAny accepted a collection holding no task");

    List<Callable<String>> three = new ArrayList<>();
    three.add(new NamedCallable("first"));
    three.add(new NamedCallable("second"));
    three.add(new NamedCallable("third"));
    List<Future<String>> results = pool.invokeAll(three);

    assertEquals(3, three.size(),
        "invokeAll altered the collection it was given");
    assertEquals(3, results.size(),
        "invokeAll did not return a result for every task it was given");
    assertEquals("first", results.get(0).get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
        "invokeAll returned the results out of the order of its tasks");
    assertEquals("second",
        results.get(1).get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
        "invokeAll returned the results out of the order of its tasks");
    assertEquals("third", results.get(2).get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
        "invokeAll returned the results out of the order of its tasks");
  }

  /**
   * A scheduled pool runs its task under the subject that submitted it,
   * whether the pool was given a thread factory or left to make its own
   * threads.
   *
   * @throws Exception if a task fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testScheduledThreadPoolRunsTaskUnderSubmitterSubject()
      throws Exception {
    Subject submitter = newSubject("scheduled@EXAMPLE.COM");

    ScheduledExecutorService defaultThreads =
        register(HadoopExecutors.newScheduledThreadPool(1));
    assertSame(submitter, submissionObserves(submitter, defaultThreads),
        "a task submitted to a scheduled pool did not observe its submitter");

    ScheduledExecutorService namedThreads =
        register(HadoopExecutors.newScheduledThreadPool(
            1, new PlainDaemonThreadFactory("scheduled")));
    assertSame(submitter, submissionObserves(submitter, namedThreads),
        "a task submitted to a scheduled pool with a thread factory did not "
            + "observe its submitter");
  }

  /**
   * A single-thread scheduled executor runs its task under the subject that
   * submitted it, through submission and through both ways of scheduling one.
   *
   * @throws Exception if a task fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testSingleThreadScheduledExecutorRunsTaskUnderSubmitterSubject()
      throws Exception {
    Subject submitter = newSubject("single-scheduled@EXAMPLE.COM");

    assertSame(submitter, submissionObserves(submitter,
        register(HadoopExecutors.newSingleThreadScheduledExecutor())),
        "a task submitted to a single-thread scheduled executor did not "
            + "observe its submitter");

    assertSame(submitter, submissionObserves(submitter,
        register(HadoopExecutors.newSingleThreadScheduledExecutor(
            new PlainDaemonThreadFactory("single-scheduled")))),
        "a task submitted to a single-thread scheduled executor with a "
            + "thread factory did not observe its submitter");

    final ScheduledExecutorService pool =
        register(HadoopExecutors.newSingleThreadScheduledExecutor());
    final SubjectRecorder recorder = new SubjectRecorder(1);
    final AtomicReference<ScheduledFuture<?>> scheduledRunnable =
        new AtomicReference<>();
    final AtomicReference<ScheduledFuture<Subject>> scheduledCallable =
        new AtomicReference<>();
    SubjectUtil.callAs(submitter, new Callable<Void>() {
      public Void call() {
        scheduledRunnable.set(
            pool.schedule(recorder, 0, TimeUnit.MILLISECONDS));
        scheduledCallable.set(
            pool.schedule(currentSubject(), 0, TimeUnit.MILLISECONDS));
        return (Void) null;
      }
    });
    scheduledRunnable.get().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    assertSame(submitter, awaitSingleRun(recorder),
        "a runnable scheduled on a single-thread scheduled executor did not "
            + "observe its submitter");
    assertSame(submitter,
        scheduledCallable.get().get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
        "a callable scheduled on a single-thread scheduled executor did not "
            + "observe its submitter");
  }

  /**
   * {@link HadoopThreadPoolExecutor} runs its task under the subject that
   * submitted it, whichever way the task was given to it.
   * <p>
   * Handing work to this pool directly and submitting it for a future are
   * different entry points that the runtime routes through the same one, so
   * both are exercised: covering only one would leave the other untested.
   *
   * @throws Exception if a task fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testThreadPoolExecutorRunsTaskUnderSubmitterSubject()
      throws Exception {
    Subject submitter = newSubject("pool-executor@EXAMPLE.COM");

    assertSame(submitter, executionObserves(submitter,
        register(new HadoopThreadPoolExecutor(1, 1, 0L,
            TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>()))),
        "a task handed to a Hadoop thread pool did not observe its submitter");

    assertSame(submitter, submissionObserves(submitter,
        register(new HadoopThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(),
            new PlainDaemonThreadFactory("pool-executor")))),
        "a task submitted to a Hadoop thread pool did not observe its "
            + "submitter");
  }

  /**
   * {@link HadoopScheduledThreadPoolExecutor} runs its task under the subject
   * that submitted it, whichever way the task was given to it.
   * <p>
   * This pool prepares a task where it is scheduled, and the runtime routes
   * handing work over and submitting it for a future through exactly those
   * methods, so all four entry points are exercised together to show that none
   * of them is left out.
   *
   * @throws Exception if a task fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testScheduledExecutorRunsTaskUnderSubmitterSubject()
      throws Exception {
    Subject submitter = newSubject("scheduled-executor@EXAMPLE.COM");

    assertSame(submitter, executionObserves(submitter,
        register(new HadoopScheduledThreadPoolExecutor(1))),
        "a task handed to a Hadoop scheduled pool did not observe its "
            + "submitter");

    assertSame(submitter, submissionObserves(submitter,
        register(new HadoopScheduledThreadPoolExecutor(
            1, new PlainDaemonThreadFactory("scheduled-executor")))),
        "a task submitted to a Hadoop scheduled pool did not observe its "
            + "submitter");

    final HadoopScheduledThreadPoolExecutor pool =
        register(new HadoopScheduledThreadPoolExecutor(1));
    final SubjectRecorder recorder = new SubjectRecorder(1);
    final AtomicReference<ScheduledFuture<?>> scheduledRunnable =
        new AtomicReference<>();
    final AtomicReference<ScheduledFuture<Subject>> scheduledCallable =
        new AtomicReference<>();
    SubjectUtil.callAs(submitter, new Callable<Void>() {
      public Void call() {
        scheduledRunnable.set(
            pool.schedule(recorder, 0, TimeUnit.MILLISECONDS));
        scheduledCallable.set(
            pool.schedule(currentSubject(), 0, TimeUnit.MILLISECONDS));
        return (Void) null;
      }
    });
    scheduledRunnable.get().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    assertSame(submitter, awaitSingleRun(recorder),
        "a runnable scheduled on a Hadoop scheduled pool did not observe its "
            + "submitter");
    assertSame(submitter,
        scheduledCallable.get().get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
        "a callable scheduled on a Hadoop scheduled pool did not observe its "
            + "submitter");
  }

  /**
   * A repeating task runs under the subject that scheduled it on every one of
   * its runs, not only the first.
   * <p>
   * The schedule was created under that identity, so each run of it belongs to
   * that identity as well. Both ways of repeating a task are covered, and each
   * repeating task is cancelled once enough of its runs have been seen.
   *
   * @throws Exception if a task fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testRepeatingTaskRunsUnderSchedulingSubjectEveryTime()
      throws Exception {
    Subject scheduler = newSubject("repeating@EXAMPLE.COM");
    final ScheduledExecutorService atFixedRatePool =
        register(HadoopExecutors.newScheduledThreadPool(
            1, new PlainDaemonThreadFactory("fixed-rate")));
    final ScheduledExecutorService withFixedDelayPool =
        register(HadoopExecutors.newScheduledThreadPool(
            1, new PlainDaemonThreadFactory("fixed-delay")));
    final SubjectRecorder atFixedRate = new SubjectRecorder(3);
    final SubjectRecorder withFixedDelay = new SubjectRecorder(2);
    final AtomicReference<ScheduledFuture<?>> rated = new AtomicReference<>();
    final AtomicReference<ScheduledFuture<?>> delayed = new AtomicReference<>();

    SubjectUtil.callAs(scheduler, new Callable<Void>() {
      public Void call() {
        rated.set(atFixedRatePool.scheduleAtFixedRate(
            atFixedRate, 0, PERIOD_MILLIS, TimeUnit.MILLISECONDS));
        delayed.set(withFixedDelayPool.scheduleWithFixedDelay(
            withFixedDelay, 0, PERIOD_MILLIS, TimeUnit.MILLISECONDS));
        return (Void) null;
      }
    });

    assertEveryRunObserved(scheduler, atFixedRate, 3);
    assertEveryRunObserved(scheduler, withFixedDelay, 2);

    rated.get().cancel(true);
    delayed.get().cancel(true);
    assertTrue(rated.get().isCancelled(),
        "the task scheduled at a fixed rate was not cancelled");
    assertTrue(delayed.get().isCancelled(),
        "the task scheduled with a fixed delay was not cancelled");
  }

  /**
   * The scheduled executor Hadoop's single-thread factory hands back runs a
   * task repeating at a fixed rate under the subject that scheduled it, on
   * every one of its runs, through either of that factory's two overloads.
   * <p>
   * That factory has semantics of its own which Hadoop chose not to reproduce,
   * so what it returns forwards each scheduling on to an executor of the
   * runtime's own rather than extending one. Forwarding is therefore a second
   * place, quite apart from the pool Hadoop does extend, where a repeating
   * schedule has to carry the identity that created it, and neither overload of
   * the factory is covered by the other.
   *
   * @throws Exception if a task fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testForwardedFixedRateTaskRunsUnderSchedulingSubjectEveryTime()
      throws Exception {
    Subject scheduler = newSubject("forwarded-fixed-rate@EXAMPLE.COM");
    final ScheduledExecutorService madeItsOwnThread =
        register(HadoopExecutors.newSingleThreadScheduledExecutor());
    final ScheduledExecutorService givenAThreadFactory =
        register(HadoopExecutors.newSingleThreadScheduledExecutor(
            new PlainDaemonThreadFactory("forwarded-fixed-rate")));
    final SubjectRecorder onItsOwnThread = new SubjectRecorder(3);
    final SubjectRecorder onAFactoryThread = new SubjectRecorder(3);
    final AtomicReference<ScheduledFuture<?>> own = new AtomicReference<>();
    final AtomicReference<ScheduledFuture<?>> given = new AtomicReference<>();

    SubjectUtil.callAs(scheduler, new Callable<Void>() {
      public Void call() {
        own.set(madeItsOwnThread.scheduleAtFixedRate(
            onItsOwnThread, 0, PERIOD_MILLIS, TimeUnit.MILLISECONDS));
        given.set(givenAThreadFactory.scheduleAtFixedRate(
            onAFactoryThread, 0, PERIOD_MILLIS, TimeUnit.MILLISECONDS));
        return (Void) null;
      }
    });

    assertEveryRunObserved(scheduler, onItsOwnThread, 3);
    assertEveryRunObserved(scheduler, onAFactoryThread, 3);

    own.get().cancel(true);
    given.get().cancel(true);
    assertTrue(own.get().isCancelled(),
        "the fixed-rate task of the executor that made its own thread was not "
            + "cancelled");
    assertTrue(given.get().isCancelled(),
        "the fixed-rate task of the executor given a thread factory was not "
            + "cancelled");
  }

  /**
   * The scheduled executor Hadoop's single-thread factory hands back runs a
   * task repeating with a fixed delay under the subject that scheduled it, on
   * every one of its runs, through either of that factory's two overloads.
   * <p>
   * This is the fixed-delay counterpart of
   * {@link #testForwardedFixedRateTaskRunsUnderSchedulingSubjectEveryTime()}.
   * The two ways of repeating a task are separate methods of the forwarding
   * executor, so each is covered in its own right.
   *
   * @throws Exception if a task fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testForwardedFixedDelayTaskRunsUnderSchedulingSubjectEveryTime()
      throws Exception {
    Subject scheduler = newSubject("forwarded-fixed-delay@EXAMPLE.COM");
    final ScheduledExecutorService madeItsOwnThread =
        register(HadoopExecutors.newSingleThreadScheduledExecutor());
    final ScheduledExecutorService givenAThreadFactory =
        register(HadoopExecutors.newSingleThreadScheduledExecutor(
            new PlainDaemonThreadFactory("forwarded-fixed-delay")));
    final SubjectRecorder onItsOwnThread = new SubjectRecorder(3);
    final SubjectRecorder onAFactoryThread = new SubjectRecorder(3);
    final AtomicReference<ScheduledFuture<?>> own = new AtomicReference<>();
    final AtomicReference<ScheduledFuture<?>> given = new AtomicReference<>();

    SubjectUtil.callAs(scheduler, new Callable<Void>() {
      public Void call() {
        own.set(madeItsOwnThread.scheduleWithFixedDelay(
            onItsOwnThread, 0, PERIOD_MILLIS, TimeUnit.MILLISECONDS));
        given.set(givenAThreadFactory.scheduleWithFixedDelay(
            onAFactoryThread, 0, PERIOD_MILLIS, TimeUnit.MILLISECONDS));
        return (Void) null;
      }
    });

    assertEveryRunObserved(scheduler, onItsOwnThread, 3);
    assertEveryRunObserved(scheduler, onAFactoryThread, 3);

    own.get().cancel(true);
    given.get().cancel(true);
    assertTrue(own.get().isCancelled(),
        "the fixed-delay task of the executor that made its own thread was "
            + "not cancelled");
    assertTrue(given.get().isCancelled(),
        "the fixed-delay task of the executor given a thread factory was not "
            + "cancelled");
  }

  /**
   * {@link SemaphoredDelegatingExecutor} runs its task under the subject that
   * submitted it, through each of the four ways it accepts one.
   * <p>
   * This executor already decorates every task it forwards, so carrying an
   * identity has to compose with that rather than replace it. Its bulk
   * submission methods are unsupported by design and are therefore not called.
   *
   * @throws Exception if a task fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testSemaphoredDelegatingExecutorRunsTaskUnderSubmitterSubject()
      throws Exception {
    Subject submitter = newSubject("semaphored@EXAMPLE.COM");

    assertSame(submitter, submissionObserves(submitter, newSemaphored("call")),
        "a callable submitted to a semaphored executor did not observe its "
            + "submitter");

    assertSame(submitter, executionObserves(submitter, newSemaphored("exec")),
        "a task handed to a semaphored executor did not observe its "
            + "submitter");

    assertSame(submitter, runnableSubmissionObserves(submitter,
        newSemaphored("run")),
        "a runnable submitted to a semaphored executor did not observe its "
            + "submitter");

    assertSame(submitter, resultSubmissionObserves(submitter,
        newSemaphored("result")),
        "a runnable submitted with a result to a semaphored executor did not "
            + "observe its submitter");
  }

  /**
   * A semaphored executor gets every permit back once its tasks are done, a
   * task that fails included.
   * <p>
   * Carrying an identity into a task adds a step around it, and that step must
   * not disturb the accounting that lets this executor limit how much work is
   * outstanding: a permit held onto after a task ended would eventually stop
   * the executor accepting anything at all.
   *
   * @throws Exception if a task fails unexpectedly or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testSemaphoredDelegatingExecutorReleasesEveryPermit()
      throws Exception {
    Subject submitter = newSubject("permits@EXAMPLE.COM");
    final SemaphoredDelegatingExecutor pool = newSemaphored("permits");
    int permits = pool.getPermitCount();
    assertEquals(permits, pool.getAvailablePermits(),
        "a fresh semaphored executor had permits already taken");

    assertSame(submitter, submissionObserves(submitter, pool),
        "a callable submitted to a semaphored executor did not observe its "
            + "submitter");
    assertEquals(permits, pool.getAvailablePermits(),
        "a permit was not returned after a task completed");

    final IllegalStateException failure =
        new IllegalStateException("a failing task releases its permit");
    final AtomicReference<Future<Subject>> failed = new AtomicReference<>();
    SubjectUtil.callAs(submitter, new Callable<Void>() {
      public Void call() {
        failed.set(pool.submit(new Callable<Subject>() {
          @Override
          public Subject call() {
            throw failure;
          }
        }));
        return (Void) null;
      }
    });
    ExecutionException thrown = assertThrows(ExecutionException.class,
        () -> failed.get().get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
        "a task that threw did not fail its future");
    assertSame(failure, thrown.getCause(),
        "a failing task did not reach its future as the exception it threw");
    assertEquals(permits, pool.getAvailablePermits(),
        "a permit was not returned after a task failed");
    assertEquals(0, pool.getWaitingCount(),
        "a semaphored executor was left with callers waiting for a permit");
  }

  /**
   * A semaphored executor forwarding to a blocking pool runs its task under the
   * submitter, through each of the four ways it accepts one.
   * <p>
   * This is the composition the object-store filesystems build: a semaphored
   * executor bounding how much work one stream has outstanding, forwarding to a
   * shared blocking pool that bounds how much the filesystem has outstanding
   * altogether. Both layers prepare the tasks they forward on their own account
   * and each adds a wrapper of its own in between, so neither can see what the
   * other did by looking at what it was handed. What a task observes must not
   * depend on how many layers it passed through, and this asserts it over the
   * stack that actually ships.
   *
   * @throws Exception if a task fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testNestedSemaphoredOverBlockingRunsTaskUnderSubmitterSubject()
      throws Exception {
    Subject submitter = newSubject("nested-blocking@EXAMPLE.COM");

    assertSame(submitter, submissionObserves(submitter,
        newSemaphoredOverBlocking("nested-call")),
        "a callable submitted through a semaphored executor over a blocking "
            + "pool did not observe its submitter");
    assertSame(submitter, executionObserves(submitter,
        newSemaphoredOverBlocking("nested-exec")),
        "a task handed to a semaphored executor over a blocking pool did not "
            + "observe its submitter");
    assertSame(submitter, runnableSubmissionObserves(submitter,
        newSemaphoredOverBlocking("nested-run")),
        "a runnable submitted through a semaphored executor over a blocking "
            + "pool did not observe its submitter");
    assertSame(submitter, resultSubmissionObserves(submitter,
        newSemaphoredOverBlocking("nested-result")),
        "a runnable submitted with a result through a semaphored executor over "
            + "a blocking pool did not observe its submitter");
  }

  /**
   * A semaphored executor forwarding to a blocking pool gets every permit back
   * in both layers, a failing task included.
   * <p>
   * Each layer counts what it has outstanding with permits of its own, and each
   * releases its permit from a wrapper it puts around the task. Preparing the
   * task for its submitter adds another wrapper in the same place, so a mistake
   * about which wrapper goes where would strand a permit; a layer that had lost
   * permits would in time stop accepting work altogether, and the outer layer
   * would be the one to appear stuck. Both layers are therefore read back to
   * full, after a task that succeeded and again after one that threw.
   *
   * @throws Exception if a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testNestedSemaphoredOverBlockingReleasesPermitsInBothLayers()
      throws Exception {
    Subject submitter = newSubject("nested-permits@EXAMPLE.COM");
    final BlockingThreadPoolExecutorService inner =
        newBlocking("nested-permits");
    final SemaphoredDelegatingExecutor outer =
        register(new SemaphoredDelegatingExecutor(inner, 3, false));
    int outerPermits = outer.getPermitCount();
    int innerPermits = inner.getPermitCount();

    assertSame(submitter, submissionObserves(submitter, outer),
        "a callable submitted through the nested stack did not observe its "
            + "submitter");
    assertPermitsRestored(outer, outerPermits, inner, innerPermits,
        "a task completed");

    final IllegalStateException failure =
        new IllegalStateException("a failing task releases both permits");
    final AtomicReference<Future<Subject>> failed = new AtomicReference<>();
    SubjectUtil.callAs(submitter, new Callable<Void>() {
      @Override
      public Void call() {
        failed.set(outer.submit(new Callable<Subject>() {
          @Override
          public Subject call() {
            throw failure;
          }
        }));
        return (Void) null;
      }
    });
    ExecutionException thrown = assertThrows(ExecutionException.class,
        () -> failed.get().get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
        "a task that threw did not fail its future");
    assertSame(failure, thrown.getCause(),
        "a task that threw through the nested stack did not reach its future "
            + "as the exception it threw");
    assertPermitsRestored(outer, outerPermits, inner, innerPermits,
        "a task failed");
  }

  /**
   * A semaphored executor forwarding through an opaque decorator to another
   * semaphored executor still runs its task under the submitter, exactly once
   * under that identity.
   * <p>
   * Guava's listenable decorator forwards every submission to an executor it
   * holds privately, and it is an ordinary executor as far as anything above it
   * can tell. A layer above it therefore cannot recognise what lies below and
   * prepares the task itself, while the layer below prepares it again. That
   * makes this the composition in which two preparations are unavoidable, and it
   * is why the preparation itself declines to establish an identity that is
   * already in force rather than relying on being able to spot the situation in
   * advance. Filesystems in this project put such a decorator both above and
   * below a semaphored executor, so both orders occur.
   *
   * @throws Exception if a task fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testNestedSemaphoredThroughOpaqueDecoratorObservesSubmitter()
      throws Exception {
    Subject submitter = newSubject("nested-opaque@EXAMPLE.COM");
    ExecutorService throughDecorator = register(
        new SemaphoredDelegatingExecutor(
            MoreExecutors.listeningDecorator(
                newSemaphoredOverBlocking("opaque-below")),
            3, false));
    ExecutorService decoratorOnTop = register(
        MoreExecutors.listeningDecorator(
            newSemaphoredOverBlocking("opaque-above")));

    assertSame(submitter, submissionObserves(submitter, throughDecorator),
        "a callable submitted through an opaque decorator between two "
            + "semaphored executors did not observe its submitter");
    assertSame(submitter, submissionObserves(submitter, decoratorOnTop),
        "a callable submitted through an opaque decorator above a semaphored "
            + "executor did not observe its submitter");
  }

  /**
   * A task prepared for an identity observes it when that identity is already in
   * force, exactly as when it is not.
   * <p>
   * Preparing a task means arranging for its identity to be in force while it
   * runs, and a task crossing more than one executor is prepared more than once,
   * so the innermost preparation runs with that identity already established by
   * an outer one. Establishing it again would change nothing and is skipped, and
   * this is what says that skipping it changes nothing either: the same prepared
   * task is run directly inside its own identity's scope and submitted to a pool
   * from inside that scope, where the pool prepares it a second time, and both
   * have to observe the identity it was prepared for.
   *
   * @throws Exception if a task fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testPreparedTaskObservesItsSubjectWhenAlreadyInForce()
      throws Exception {
    final Subject prepared = newSubject("already-in-force@EXAMPLE.COM");
    final ExecutorService pool = register(HadoopExecutors.newFixedThreadPool(
        1, new PlainDaemonThreadFactory("already-in-force")));
    final AtomicReference<Callable<Subject>> byHand = new AtomicReference<>();
    final AtomicReference<Future<Subject>> byPool = new AtomicReference<>();

    Subject observedDirectly =
        SubjectUtil.callAs(prepared, new Callable<Subject>() {
          @Override
          public Subject call() throws Exception {
            byHand.set(SubjectPreservingTasks.wrap(
                currentSubject()));
            byPool.set(pool.submit(byHand.get()));
            return byHand.get().call();
          }
        });

    assertSame(prepared, observedDirectly,
        "a prepared task run inside the scope it was prepared in did not "
            + "observe the identity it was prepared for");
    assertSame(prepared, byPool.get().get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
        "an already-prepared task submitted to a pool, which prepares it "
            + "again, did not observe the identity it was prepared for");
  }


  /**
   * {@link BlockingThreadPoolExecutorService} runs its task under the subject
   * that submitted it, through each of the four ways it accepts one.
   * <p>
   * This executor has no accessible constructor and is built through its
   * factory method; the workers it makes for itself hold an identity of their
   * own, which is exactly why the identity of each submission has to be
   * carried with the task.
   *
   * @throws Exception if a task fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testBlockingThreadPoolRunsTaskUnderSubmitterSubject()
      throws Exception {
    Subject submitter = newSubject("blocking@EXAMPLE.COM");

    assertSame(submitter, submissionObserves(submitter, newBlocking("call")),
        "a callable submitted to a blocking pool did not observe its "
            + "submitter");

    assertSame(submitter, executionObserves(submitter, newBlocking("exec")),
        "a task handed to a blocking pool did not observe its submitter");

    assertSame(submitter,
        runnableSubmissionObserves(submitter, newBlocking("run")),
        "a runnable submitted to a blocking pool did not observe its "
            + "submitter");

    assertSame(submitter,
        resultSubmissionObserves(submitter, newBlocking("result")),
        "a runnable submitted with a result to a blocking pool did not "
            + "observe its submitter");
  }

  /**
   * A worker kept between submissions runs each task under the submitter of
   * that task, whether that submitter is the one before it, a different one, or
   * none at all.
   * <p>
   * Both pools here keep a single worker, and both make workers of the kind
   * that hold an identity of their own for as long as they live, read from
   * whoever caused the worker to exist. Every submission therefore lands on a
   * worker already holding the first submitter's identity, which is exactly the
   * arrangement under which reading an identity when a worker is made, rather
   * than when a task is submitted, runs one caller's work as another user. The
   * three submissions are made in turn and each is waited for before the next,
   * so the worker really is the same one throughout, which the assertions on
   * the worker itself establish rather than assume.
   * <p>
   * Both worker kinds Hadoop supplies to this factory are covered, since the
   * identity a worker holds comes from the worker and not from the pool.
   *
   * @throws Exception if a task fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testReusedWorkerRunsEachTaskUnderItsOwnSubmitterSubject()
      throws Exception {
    assertReusedWorkerObservesEachSubmitter(
        register(HadoopExecutors.newFixedThreadPool(1,
            BlockingThreadPoolExecutorService.newDaemonThreadFactory("reuse"))),
        "reuse");
    assertReusedWorkerObservesEachSubmitter(
        register(HadoopExecutors.newFixedThreadPool(1,
            new Daemon.DaemonFactory())),
        "daemon-reuse");
  }

  /**
   * A blocking pool's worker, kept between submissions, runs each task under
   * the submitter of that task.
   * <p>
   * This executor lets an idle worker be reclaimed, so it is given room for one
   * task at a time and an idle period long enough that the worker is certain to
   * still be there for the submissions that follow: a worker replaced in
   * between would hide the very leak this asserts against, which is why the
   * worker each task ran on is asserted on as well.
   *
   * @throws Exception if a task fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testBlockingPoolReusedWorkerRunsEachTaskUnderItsOwnSubmitter()
      throws Exception {
    Subject first = newSubject("blocking-first@EXAMPLE.COM");
    Subject second = newSubject("blocking-second@EXAMPLE.COM");
    ExecutorService reused = register(BlockingThreadPoolExecutorService
        .newInstance(1, 1, 10, TimeUnit.MINUTES, "blocking-reuse"));

    Observation byFirst = observeSubmission(first, reused);
    Observation byFirstAgain = observeSubmission(first, reused);
    Observation bySecond = observeSubmission(second, reused);

    assertSame(byFirst.worker(), byFirstAgain.worker(),
        "the second submission ran on a different blocking-pool worker, so "
            + "this asserts nothing about a worker being kept");
    assertSame(byFirst.worker(), bySecond.worker(),
        "the second identity's task ran on a different blocking-pool worker, "
            + "so this asserts nothing about a worker being kept");
    assertSame(first, byFirst.subject(),
        "the first task submitted did not observe its own submitter");
    assertSame(first, byFirstAgain.subject(),
        "a second task run by the same blocking-pool worker did not observe "
            + "its own submitter");
    assertSecondSubmitterObserved(first, second, bySecond.subject());
  }

  /**
   * A key queue refills as the caller whose request asked for the values, even
   * when a second caller's request reaches the same filler thread.
   * <p>
   * {@link ValueQueue} does not hand its refill task to its executor. It puts
   * the task straight into the executor's backing queue, so that it controls
   * how the task enters that queue, which means the task never passes the point
   * where an executor would establish the queueing thread's identity on it. A
   * refill fetches key material through a provider that authenticates as the
   * current user, so a refill running as the wrong user asks a key server for
   * one user's keys as another.
   * <p>
   * The filler thread is created by the first request, which makes the second
   * request the one that matters: a runtime that hands a new thread the
   * identity of its creator has already given that thread the first caller's
   * identity, so a refill that took its identity from the thread rather than
   * from the request would serve the second caller's request as the first
   * caller. Each request here therefore uses a key of its own, so that neither
   * is discarded as a duplicate of the other, and each asserts the identity its
   * own refill observed.
   *
   * @throws Exception if a refill fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testKeyQueueRefillObservesTheSubjectThatAskedForValues()
      throws Exception {
    Subject first = newSubject("value-queue-first@EXAMPLE.COM");
    Subject second = newSubject("value-queue-second@EXAMPLE.COM");
    RecordingRefiller refiller = new RecordingRefiller();
    ValueQueue<String> values = new ValueQueue<>(10, 0.5f, 30000, 1,
        SyncGenerationPolicy.ALL, refiller);
    try {
      Subject observedForFirst =
          refillObserves(first, "first-key", values, refiller);
      Subject observedForSecond =
          refillObserves(second, "second-key", values, refiller);

      assertSame(first, observedForFirst,
          "a refill did not observe the identity that asked for the values");
      assertSame(second, observedForSecond,
          "a refill reaching the filler thread that an earlier request created "
              + "did not observe the identity that asked for the values");
    } finally {
      values.shutdown();
    }
  }

  /**
   * Asks a key queue for a value as {@code asker} and returns the subject the
   * refill that follows observed on the filler thread.
   *
   * @param asker the identity to ask for values as
   * @param keyName the key to ask for, one per request so that no request is
   *                discarded as a duplicate of another
   * @param values the queue to ask
   * @param refiller the refiller backing {@code values}
   * @return the subject the refill observed, which may be {@code null}
   * @throws Exception if the request fails or the wait runs out
   */
  private Subject refillObserves(final Subject asker, final String keyName,
      final ValueQueue<String> values, final RecordingRefiller refiller)
      throws Exception {
    final CountDownLatch refilled = refiller.expectRefillOnFillerThread();
    SubjectUtil.callAs(asker, new Callable<Void>() {
      public Void call() throws Exception {
        values.getAtMost(keyName, 1);
        return (Void) null;
      }
    });
    assertTrue(refilled.await(TIMEOUT_SECONDS, TimeUnit.SECONDS),
        "a key queue did not refill on its filler thread");
    return refiller.subjectOnFillerThread();
  }

  /**
   * Returns the identity a task observes when the worker that runs it was
   * retained from an earlier submission made by somebody else.
   * <p>
   * A worker outlives the tasks it runs, so the identity in force on it between
   * tasks belongs to whoever's submission caused it to exist. That identity is
   * never what a later task should observe, so the identity of each submission
   * is read as its task is prepared and established again around the run: a task
   * observes its own submitter whichever worker picks it up, and a submission
   * that carried no identity observes none rather than whatever its worker was
   * left holding. The answer is therefore the later submitter on every runtime,
   * and this method exists to say so once for the assertions that need it.
   * <p>
   * How much a worker is actually holding varies more than it looks, and the
   * answer is worth stating because it decides which pools can show a leak at
   * all. A thread pool does not start its workers by the method a subclass would
   * override to be told about it: it hands the thread to the container that owns
   * it, which starts it by a path of its own. A worker that would have taken its
   * creator's identity had it been started in the ordinary way therefore takes
   * nothing when a pool starts it, and on the runtimes that hand a thread
   * nothing such a worker holds nothing to lend. What a pool worker holds is
   * consequently not something the code preparing a task can know, which is the
   * reason that code establishes the submission's identity whether or not there
   * was one to establish rather than deciding when it is needed. Workers reached
   * through an executor given to Hadoop from outside, and workers started in the
   * ordinary way, do hold their creator's identity, and those are where leaving
   * a task alone is visibly wrong.
   *
   * @param creator the identity whose submission caused the worker to exist
   * @param submitter the identity making the later submission, or {@code null}
   *        if that submission carried no identity at all
   * @return the identity the later task has to observe, which is always the
   *         later submitter or, where there was none, no identity at all
   */
  private static Subject retainedWorkerServes(Subject creator,
      Subject submitter) {
    return submitter;
  }

  /**
   * Returns a task that reports the thread that ran it and the identity in
   * force there.
   *
   * @return a task yielding its own thread and that thread's subject
   */
  private static Callable<Observation> currentObservation() {
    return new Callable<Observation>() {
      @Override
      public Observation call() {
        return new Observation(SubjectUtil.current(), Thread.currentThread());
      }
    };
  }

  /**
   * Submits a task reporting its thread and identity to {@code pool} from
   * inside {@code submitter}'s scope, and returns what that task observed.
   * <p>
   * A {@code null} submitter submits from this thread directly, with no identity
   * established around the submission at all. That is a case an executor has to
   * keep apart from every other, so it is reached here by genuinely having none
   * rather than by naming an empty one.
   *
   * @param submitter the identity to submit under, or {@code null} to submit
   *        with none
   * @param pool the executor to submit to
   * @return what the task observed
   * @throws Exception if the task fails or the wait times out
   */
  private Observation observationOf(final Subject submitter,
      final ExecutorService pool) throws Exception {
    final AtomicReference<Future<Observation>> submitted =
        new AtomicReference<>();
    Callable<Void> submission = new Callable<Void>() {
      @Override
      public Void call() {
        submitted.set(pool.submit(currentObservation()));
        return (Void) null;
      }
    };
    if (submitter == null) {
      submission.call();
    } else {
      SubjectUtil.callAs(submitter, submission);
    }
    return submitted.get().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  /**
   * Submits from {@code creator} and then from {@code later} into a pool with
   * room for exactly one worker, and asserts what each task observed.
   * <p>
   * The first submission is what causes the worker to exist, so it establishes
   * the identity that a leak would carry forward to the second. Waiting for its
   * result before submitting again leaves that worker idle rather than replaced,
   * and both tasks are asked which thread ran them, so that a pool which handed
   * the second task to a worker of its own fails the assertion outright instead
   * of passing it without ever reaching the case it is about.
   *
   * @param pool a pool with room for exactly one worker
   * @param creator the identity to make the first submission under
   * @param later the identity to make the second submission under, or
   *        {@code null} to make that submission with no identity at all
   * @throws Exception if a task fails or a wait times out
   */
  private void assertRetainedWorkerServes(ExecutorService pool,
      Subject creator, Subject later) throws Exception {
    Observation first = observationOf(creator, pool);
    Observation second = observationOf(later, pool);

    assertSame(first.thread(), second.thread(),
        "the pool replaced its worker between the two submissions, so this "
            + "assertion never reached the case it is about");
    assertSame(creator, first.subject(),
        "the submission that caused the worker to exist did not observe its "
            + "own submitter");
    assertSame(retainedWorkerServes(creator, later), second.subject(),
        "a task run by a worker retained from an earlier submitter did not "
            + "observe the identity this runtime owes it");
  }

  /**
   * One worker serving two identities in turn runs the second identity's task
   * under that second identity, where the workers are plain threads.
   * <p>
   * The other assertions about a reused worker submit twice from one identity
   * and give the second identity a pool of its own, which leaves the case where
   * a single worker serves two identities one after the other untested. This
   * submits both into the same one-worker pool and proves the worker was kept
   * across them, so a task running as the wrong user has nowhere to hide.
   *
   * @throws Exception if a task fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testRetainedPlainWorkerServesASecondIdentity() throws Exception {
    Subject creator = newSubject("retained-plain-creator@EXAMPLE.COM");
    Subject later = newSubject("retained-plain-later@EXAMPLE.COM");

    assertRetainedWorkerServes(register(HadoopExecutors.newFixedThreadPool(
        1, new PlainDaemonThreadFactory("retained-plain"))), creator, later);
  }

  /**
   * One worker that an identity's submission created runs a later submission
   * that carried no identity under none, where the workers are plain threads.
   * <p>
   * A submission carrying nothing is the case an executor is likeliest to get
   * wrong, because leaving such a task alone looks like the safe thing to do
   * and costs nothing to do. It is not safe: the worker it lands on is still
   * holding whoever created it, so a task left alone runs as that identity
   * instead of as nobody, and it does so silently.
   *
   * @throws Exception if a task fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testRetainedPlainWorkerServesASubjectlessSubmission()
      throws Exception {
    Subject creator = newSubject("subjectless-plain-creator@EXAMPLE.COM");

    assertRetainedWorkerServes(register(HadoopExecutors.newFixedThreadPool(
        1, new PlainDaemonThreadFactory("subjectless-plain"))), creator, null);
  }

  /**
   * One worker serving two identities in turn runs the second identity's task
   * under that second identity, where the workers hold the identity they were
   * created with for as long as they live.
   * <p>
   * This is the factory that the blocking pool and the disk services are built
   * on, so it is the shape most of Hadoop's own pools actually have, and a
   * thread it makes keeps an identity in force around every task it runs rather
   * than only around the first. Which identity that is depends on how the thread
   * was started, and a pool starts its workers by a path that tells the thread
   * nothing; that is measured on its own further down. Either way the assertion
   * here is the same one, because it is about the identity each submitter's task
   * observed and not about how the worker came by what it was holding.
   *
   * @throws Exception if a task fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testRetainedInheritingWorkerServesASecondIdentity()
      throws Exception {
    Subject creator = newSubject("retained-inheriting-creator@EXAMPLE.COM");
    Subject later = newSubject("retained-inheriting-later@EXAMPLE.COM");

    assertRetainedWorkerServes(register(HadoopExecutors.newFixedThreadPool(1,
        BlockingThreadPoolExecutorService.newDaemonThreadFactory(
            "retained-inheriting"))), creator, later);
  }

  /**
   * One worker that an identity's submission created runs a later submission
   * that carried no identity under none, where the worker holds the identity it
   * was created with for as long as it lives.
   * <p>
   * A worker of this kind keeps an identity in force around every task it runs,
   * so a submission that carried nothing can find one waiting for it that is not
   * its own. Which identity a pool's worker holds is settled by how the pool
   * starts it, and the pool does not start it by the path that would tell it
   * about its creator, so this asserts what such a submission observes without
   * assuming the answer; the case where the worker demonstrably does hold its
   * creator's identity is asserted separately, over an executor handed in from
   * outside. Getting either wrong costs nothing visible at the time: nothing
   * fails, throws or is logged, and the work simply runs as the wrong user,
   * which is the one outcome an audit record cannot be asked to explain
   * afterwards.
   *
   * @throws Exception if a task fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testRetainedInheritingWorkerServesASubjectlessSubmission()
      throws Exception {
    Subject creator = newSubject("subjectless-inheriting@EXAMPLE.COM");

    assertRetainedWorkerServes(register(HadoopExecutors.newFixedThreadPool(1,
        BlockingThreadPoolExecutorService.newDaemonThreadFactory(
            "subjectless-inheriting"))), creator, null);
  }

  /**
   * Returns an executor whose one worker holds {@code holder}'s identity in
   * force around everything it runs, wrapped in the semaphored executor that
   * Hadoop hands caller-supplied executors to.
   * <p>
   * An executor given to Hadoop from outside brings its own workers, started
   * however that executor sees fit, and one started in the ordinary way takes
   * the identity of whoever started it and keeps it for as long as it lives.
   * That is the situation in which leaving a task alone is not a harmless
   * economy: the worker has an identity to lend, and a task that was not given
   * one of its own borrows it. Every executor Hadoop wraps for a caller is
   * reached this way, so this is a real shape rather than a contrived one.
   * <p>
   * The delegate is started from inside {@code holder}'s scope, which is what
   * gives its worker that identity, and both it and the semaphored executor over
   * it are shut down when the test ends.
   *
   * @param holder the identity the delegate's worker is to hold
   * @param name names the worker thread
   * @return a semaphored executor forwarding to that delegate
   * @throws Exception if the delegate cannot be started
   */
  private ExecutorService newOverIdentityCarryingDelegate(final Subject holder,
      final String name) throws Exception {
    final AtomicReference<ExecutorService> delegate = new AtomicReference<>();
    SubjectUtil.callAs(holder, new Callable<Void>() {
      @Override
      public Void call() {
        delegate.set(new IdentityCarryingExecutor(name));
        return (Void) null;
      }
    });
    return register(new SemaphoredDelegatingExecutor(
        register(delegate.get()), 2, false));
  }

  /**
   * A worker that really does hold an identity does not lend it to a submission
   * that carried none.
   * <p>
   * This is the assertion the whole treatment of an identity-less submission
   * exists for, and the only one here that fails if such a submission is handed
   * on untouched. Its worker holds an identity that is demonstrably not the
   * submitter's -- the first assertion below reads it back through the same
   * executor -- so a task left alone runs as that identity, and one given its
   * own submission's identity runs as nobody, which is what it was submitted as.
   * <p>
   * The difference between those two outcomes is a user, and nothing about
   * getting it wrong is visible at the time: the work succeeds either way.
   *
   * @throws Exception if a task fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testCarriedWorkerIdentityIsNotLentToASubjectlessSubmission()
      throws Exception {
    Subject creator = newSubject("carrying-subjectless@EXAMPLE.COM");

    assertRetainedWorkerServes(
        newOverIdentityCarryingDelegate(creator, "carrying-subjectless"),
        creator, null);
  }

  /**
   * A worker that really does hold one identity runs a second identity's task
   * under that second identity.
   * <p>
   * Same worker and same executor as the assertion above, with an identity in
   * place of the absent one, so that carrying an identity is shown to work where
   * the worker had a different one of its own to offer instead. Taken together
   * the two say that what a task observes is its own submission's identity and
   * never the worker's, whether the submission had one or not.
   *
   * @throws Exception if a task fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testCarriedWorkerIdentityIsNotLentToASecondIdentity()
      throws Exception {
    Subject creator = newSubject("carrying-creator@EXAMPLE.COM");
    Subject later = newSubject("carrying-later@EXAMPLE.COM");

    assertRetainedWorkerServes(
        newOverIdentityCarryingDelegate(creator, "carrying-second"),
        creator, later);
  }

  /**
   * A task prepared for a submitter that carries no identity runs with none of
   * its own, whatever identity is in force where it is run.
   * <p>
   * Carrying an identity into a task means establishing the submitter's own for
   * as long as the task runs, and a submitter that holds none has exactly that
   * to establish. Doing so is what keeps such a task from running as whichever
   * user its worker was left holding, which is the case a worker that serves
   * one submitter after another creates and which decides authorization and is
   * what an audit record names. Both of the kinds of task an executor accepts
   * are covered.
   * <p>
   * Preparing each task from a thread carrying nothing and then running it
   * inside a scope that does carry an identity is what makes the difference
   * visible: the identity in force where the task runs stands in for the one a
   * reused worker would have been left with, and a task that reports it would
   * be reporting an identity that was never its submitter's.
   * <p>
   * This is direct, supplemental coverage of the prepared task itself. The
   * pooled form of the same case is asserted through live one-worker pools by
   * {@link #testReusedWorkerRunsEachTaskUnderItsOwnSubmitterSubject()} and
   * {@link #testBlockingPoolReusedWorkerRunsEachTaskUnderItsOwnSubmitter()}.
   *
   * @throws Exception if a task fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testSubjectlessSubmissionRunsWithoutAnIdentityOfItsOwn()
      throws Exception {
    Subject enclosing = newSubject("enclosing-identity@EXAMPLE.COM");
    final Callable<Subject> preparedCallable =
        SubjectPreservingTasks.wrap(currentSubject());
    final SubjectRecorder recorder = new SubjectRecorder(1);
    final Runnable preparedRunnable = SubjectPreservingTasks.wrap(recorder);

    Subject observedByCallable =
        SubjectUtil.callAs(enclosing, new Callable<Subject>() {
          @Override
          public Subject call() throws Exception {
            return preparedCallable.call();
          }
        });
    SubjectUtil.callAs(enclosing, new Callable<Void>() {
      @Override
      public Void call() {
        preparedRunnable.run();
        return (Void) null;
      }
    });

    assertNull(observedByCallable,
        "a callable prepared with no identity ran under an identity that was "
            + "not its submitter's");
    assertNull(awaitSingleRun(recorder),
        "a task prepared with no identity ran under an identity that was not "
            + "its submitter's");
  }

  /**
   * A prepared task describes the task it was given, so that a message naming a
   * rejected task still names the task its submitter passed in.
   * <p>
   * A pool whose queue is full and whose rejection policy refuses the task puts
   * the task's own description into the exception it throws. Carrying an
   * identity puts something of its own between the pool and the task, so unless
   * that something describes the task it stands for, an operator reading the
   * rejection is told about the machinery instead of about the work that was
   * refused. Both of the kinds of task an executor accepts are covered, and the
   * pool's own message is checked as a whole rather than the description alone.
   *
   * @throws Exception if a task fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testRejectedTaskIsNamedInTheRejectionMessage() throws Exception {
    Subject submitter = newSubject("rejected-task@EXAMPLE.COM");
    final Runnable named = new NamedTask("the-runnable-that-was-rejected");
    final Callable<String> namedCallable =
        new NamedCallable("the-callable-that-was-rejected");

    assertEquals(named.toString(),
        SubjectUtil.doAs(submitter, new PrivilegedAction<String>() {
          @Override
          public String run() {
            return SubjectPreservingTasks.wrap(named).toString();
          }
        }), "a prepared runnable did not describe the task it was given");
    assertEquals(namedCallable.toString(),
        SubjectUtil.doAs(submitter, new PrivilegedAction<String>() {
          @Override
          public String run() {
            return SubjectPreservingTasks.wrap(namedCallable).toString();
          }
        }), "a prepared callable did not describe the task it was given");

    final ThreadPoolExecutor full = register(new HadoopThreadPoolExecutor(1, 1,
        0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(1),
        new PlainDaemonThreadFactory("rejecting"),
        new ThreadPoolExecutor.AbortPolicy()));
    final CountDownLatch occupied = new CountDownLatch(1);
    full.execute(new Runnable() {
      @Override
      public void run() {
        try {
          assertTrue(occupied.await(TIMEOUT_SECONDS, TimeUnit.SECONDS),
              "the task holding the only worker was never released");
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    });
    full.execute(new NamedTask("the-task-that-fills-the-queue"));

    try {
      RejectedExecutionException rejected =
          assertThrows(RejectedExecutionException.class, new Executable() {
            @Override
            public void execute() {
              SubjectUtil.doAs(submitter, new PrivilegedAction<Void>() {
                @Override
                public Void run() {
                  full.execute(named);
                  return (Void) null;
                }
              });
            }
          }, "a full pool with an abort policy accepted a task");
      assertTrue(String.valueOf(rejected.getMessage()).contains(named.toString()),
          "the rejection did not name the task that was refused: "
              + rejected.getMessage());
    } finally {
      occupied.countDown();
    }
  }

  /**
   * A pool used entirely without an identity runs its tasks to completion and
   * they observe none.
   * <p>
   * Carrying an identity has to leave the ordinary case alone: a submission
   * made with nothing to carry still runs, and still reports that there was
   * nothing to carry.
   *
   * @throws Exception if a task fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testPoolUsedWithoutASubjectRunsTasksWithoutOne()
      throws Exception {
    final AtomicReference<Subject> observed = new AtomicReference<>();
    ExecutorService pool = register(HadoopExecutors.newFixedThreadPool(1));

    String result = pool.submit(new Callable<String>() {
      @Override
      public String call() {
        observed.set(SubjectUtil.current());
        return SENTINEL;
      }
    }).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

    assertEquals(SENTINEL, result, "the task did not run to completion");
    assertNull(observed.get(),
        "a task submitted with no identity observed one");
  }

  /**
   * A task that fails reaches its future as the exception it threw.
   * <p>
   * Carrying an identity into a task means running it inside something else,
   * and that must not replace what a caller reading the result sees: neither an
   * unchecked failure nor a checked one may arrive wrapped in anything, or the
   * caller would have to unwrap an implementation detail to find out what went
   * wrong.
   *
   * @throws Exception if a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testFailingTaskReachesItsFutureUnchanged() throws Exception {
    Subject submitter = newSubject("failing@EXAMPLE.COM");
    final IllegalStateException unchecked =
        new IllegalStateException("an unchecked task failure");
    final IOException checked = new IOException("a checked task failure");
    final ExecutorService pool =
        register(HadoopExecutors.newFixedThreadPool(1,
            new PlainDaemonThreadFactory("failing")));
    final AtomicReference<Future<Subject>> uncheckedResult =
        new AtomicReference<>();
    final AtomicReference<Future<Subject>> checkedResult =
        new AtomicReference<>();

    SubjectUtil.callAs(submitter, new Callable<Void>() {
      public Void call() {
        uncheckedResult.set(pool.submit(new Callable<Subject>() {
          @Override
          public Subject call() {
            throw unchecked;
          }
        }));
        checkedResult.set(pool.submit(new Callable<Subject>() {
          @Override
          public Subject call() throws IOException {
            throw checked;
          }
        }));
        return (Void) null;
      }
    });

    ExecutionException uncheckedFailure =
        assertThrows(ExecutionException.class,
            () -> uncheckedResult.get().get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
            "a task that threw did not fail its future");
    assertSame(unchecked, uncheckedFailure.getCause(),
        "an unchecked failure did not reach its future as the exception the "
            + "task threw");

    ExecutionException checkedFailure =
        assertThrows(ExecutionException.class,
            () -> checkedResult.get().get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
            "a task that threw did not fail its future");
    assertSame(checked, checkedFailure.getCause(),
        "a checked failure did not reach its future as the exception the task "
            + "threw");
  }

  /**
   * A task waiting on the queue is taken off it when named as it was submitted.
   * <p>
   * Preparing a task for its submitter means the queue holds something other
   * than what the caller handed over, and a caller asking for its task back
   * knows only what it submitted. Removal therefore has to recognise the task
   * standing for the one named, or a caller that decided not to go ahead would
   * be unable to withdraw its work and it would run anyway.
   *
   * @throws Exception if a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testQueuedTaskIsRemovedWhenNamedAsSubmitted() throws Exception {
    Subject submitter = newSubject("remove@EXAMPLE.COM");
    final ThreadPoolExecutor pool = registerPool("remove");
    final Runnable waiting = new SubjectRecorder(1);

    occupyTheWorker(submitter, pool);
    submitFromScope(submitter, pool, waiting);
    assertEquals(1, pool.getQueue().size(),
        "the task did not end up waiting on the queue");

    assertTrue(pool.remove(waiting),
        "a task waiting on the queue was not removed when named as submitted");
    assertEquals(0, pool.getQueue().size(),
        "the queue still held the task after it was removed");
    assertFalse(pool.remove(waiting),
        "removing a task that is no longer waiting reported that it was");
  }

  /**
   * Cancelled tasks are dropped from the queue, which is what lets go of the
   * identities they were holding.
   * <p>
   * A cancelled task says so through the future it was submitted as, and the
   * queue holds the prepared form of that future rather than the future itself,
   * which the pool's own sweep does not recognise. Each such task keeps a
   * reference to its submitter's subject, and to the credentials in it, for as
   * long as it sits there -- on a queue that has stopped draining, indefinitely.
   * Reclaiming the room is therefore what releases them, and an empty queue is
   * what says it happened.
   *
   * @throws Exception if a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testPurgeDropsCancelledTasksAndReleasesTheirSubjects()
      throws Exception {
    Subject submitter = newSubject("purge@EXAMPLE.COM");
    final ThreadPoolExecutor pool = registerPool("purge");
    final List<Future<?>> cancelled = new ArrayList<>();

    occupyTheWorker(submitter, pool);
    SubjectUtil.callAs(submitter, new Callable<Void>() {
      @Override
      public Void call() {
        for (int i = 0; i < 3; i++) {
          cancelled.add(pool.submit(new SubjectRecorder(1)));
        }
        return (Void) null;
      }
    });
    assertEquals(3, pool.getQueue().size(),
        "the tasks did not end up waiting on the queue");
    for (Future<?> task : cancelled) {
      assertTrue(task.cancel(false), "a queued task refused to be cancelled");
    }
    assertEquals(3, pool.getQueue().size(),
        "cancelling alone was expected to leave the queue occupied");

    pool.purge();

    assertEquals(0, pool.getQueue().size(),
        "cancelled tasks were left occupying the queue, so the subjects they "
            + "captured were never released");
  }

  /**
   * Stopping a pool at once hands back the tasks that never started, as they
   * were submitted.
   * <p>
   * A caller shutting a pool down is owed the work it handed over, so that it
   * can run it elsewhere or report on it. Handing back the prepared form instead
   * would leave every such caller holding tasks it could not identify, and would
   * make undoing a detail of the pool a condition of using it.
   *
   * @throws Exception if a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testShutdownNowReturnsTasksAsTheyWereSubmitted()
      throws Exception {
    Subject submitter = newSubject("shutdown-now@EXAMPLE.COM");
    ThreadPoolExecutor pool = registerPool("shutdown-now");
    Runnable neverStarted = new SubjectRecorder(1);

    occupyTheWorker(submitter, pool);
    submitFromScope(submitter, pool, neverStarted);

    List<Runnable> handedBack = pool.shutdownNow();

    assertEquals(1, handedBack.size(),
        "stopping the pool did not hand back the one task that never started");
    assertSame(neverStarted, handedBack.get(0),
        "stopping the pool handed back something other than the task that was "
            + "submitted");
  }

  /**
   * The forwarding service also hands back the tasks that never started, as they
   * were submitted.
   * <p>
   * It forwards to a service whose own tasks were prepared here, so the list it
   * receives is in the prepared form and returning it unchanged would push the
   * same problem onto its callers. This covers the services
   * {@link HadoopExecutors} obtains from the runtime, which is the only place
   * that forwarding is used.
   *
   * @throws Exception if a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testForwardingServiceShutdownNowReturnsTasksAsSubmitted()
      throws Exception {
    Subject submitter = newSubject("forwarding-shutdown@EXAMPLE.COM");
    ExecutorService pool = HadoopExecutors.newSingleThreadExecutor(
        new PlainDaemonThreadFactory("forwarding-shutdown"));
    Runnable neverStarted = new SubjectRecorder(1);
    try {
      occupyTheWorker(submitter, pool);
      submitFromScope(submitter, pool, neverStarted);

      List<Runnable> handedBack = pool.shutdownNow();

      assertEquals(1, handedBack.size(),
          "the forwarding service did not hand back the one task that never "
              + "started");
      assertSame(neverStarted, handedBack.get(0),
          "the forwarding service handed back something other than the task "
              + "that was submitted");
    } finally {
      pool.shutdownNow();
      assertTrue(pool.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS),
          "the forwarding service did not terminate");
    }
  }

  /**
   * A submission the pool turns away names the task as it was submitted.
   * <p>
   * The exception a pool throws when it cannot accept a task carries that task's
   * own description, and it reaches an operator as a log line or a stack trace.
   * That description arrives without anything having had the chance to undo the
   * preparation first, so the prepared task has to describe itself as the task
   * it was given, or a rejection would report an internal class name in place of
   * the work that was refused.
   *
   * @throws Exception if a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testRejectedSubmissionNamesTheTaskAsItWasSubmitted()
      throws Exception {
    Subject submitter = newSubject("rejected@EXAMPLE.COM");
    final ThreadPoolExecutor pool = register(new HadoopThreadPoolExecutor(
        1, 1, 0L, TimeUnit.MILLISECONDS, new SynchronousQueue<Runnable>(),
        new PlainDaemonThreadFactory("rejected")));
    final Runnable named = new Runnable() {
      @Override
      public void run() {
      }

      @Override
      public String toString() {
        return REJECTED_TASK_NAME;
      }
    };

    occupyTheWorker(submitter, pool);
    RejectedExecutionException refused = assertThrows(
        RejectedExecutionException.class,
        () -> submitFromScope(submitter, pool, named),
        "a pool with nowhere to put the task accepted it anyway");

    assertTrue(refused.getMessage().contains(REJECTED_TASK_NAME),
        "the rejection named something other than the task submitted: "
            + refused.getMessage());
  }

  /**
   * A task the pool re-offers to itself is not prepared a second time.
   * <p>
   * One of the ways a pool can deal with having nowhere to put a task is to drop
   * the task that has been waiting longest and offer the new one again, which
   * this project uses for readahead. The task offered again has already been
   * prepared, and preparing it again would bury the submitted task one level
   * deeper than anything looking for it expects: undoing the preparation is
   * deliberately one level and no more, so a task wrapped twice would come back
   * from a shutdown still wrapped once.
   * <p>
   * Asking the pool for the task after the re-offer is therefore what says the
   * re-offer added nothing, and it says so through the same method a caller would
   * use rather than by looking inside.
   *
   * @throws Exception if a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testReofferedTaskIsNotPreparedTwice() throws Exception {
    Subject submitter = newSubject("re-offered@EXAMPLE.COM");
    final ThreadPoolExecutor pool = register(new HadoopThreadPoolExecutor(
        1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(1),
        new PlainDaemonThreadFactory("re-offered"),
        new ThreadPoolExecutor.DiscardOldestPolicy()));
    Runnable displaced = new SubjectRecorder(1);
    Runnable reoffered = new SubjectRecorder(1);

    occupyTheWorker(submitter, pool);
    submitFromScope(submitter, pool, displaced);
    // Nowhere left to put this one, so the policy drops the waiting task and
    // offers this one to the pool again.
    submitFromScope(submitter, pool, reoffered);

    List<Runnable> handedBack = pool.shutdownNow();

    assertEquals(1, handedBack.size(),
        "the pool did not end up holding exactly the re-offered task");
    assertSame(reoffered, handedBack.get(0),
        "a task the pool re-offered to itself was prepared a second time, so "
            + "it did not come back as it was submitted");
  }


  /**
   * Returns a fresh semaphored executor over a pool of its own.
   * <p>
   * The pool it forwards to is a plain one, made here rather than through any of
   * Hadoop's factories and given plain threads, so it neither carries an
   * identity with a task nor hands one to a worker it creates. That leaves the
   * semaphored executor's own handling of the task as the only thing an
   * assertion about the identity a task observed can be reading, which is what
   * makes such an assertion a statement about this executor rather than about
   * whatever it happens to forward to.
   * <p>
   * Both the executor and the pool it forwards to are shut down when the test
   * ends, and each caller gets its own so that a worker is always created by
   * the submission being asserted on.
   *
   * @param prefix names the threads of the pool being forwarded to
   * @return a semaphored executor ready for a single test's use
   */
  private SemaphoredDelegatingExecutor newSemaphored(String prefix) {
    return register(new SemaphoredDelegatingExecutor(
        register(new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(),
            new PlainDaemonThreadFactory("semaphored-" + prefix))),
        2, false));
  }

  /**
   * Returns a pool with one worker and an unbounded queue, shut down when the
   * test ends.
   *
   * @param prefix names the worker thread
   * @return a pool ready for a single test's use
   */
  private ThreadPoolExecutor registerPool(String prefix) {
    return register(new HadoopThreadPoolExecutor(1, 1, 0L,
        TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(),
        new PlainDaemonThreadFactory(prefix)));
  }

  /**
   * Ties up the pool's only worker until the test ends, so that anything
   * submitted afterwards waits on the queue instead of running.
   * <p>
   * Assertions about a task that is still waiting need it to stay waiting for as
   * long as the assertion takes, which only a busy worker gives them. The task
   * used for this is submitted from {@code submitter}'s scope like any other, so
   * the pool is in the same state it would be in during ordinary use, and it
   * finishes when the test does.
   *
   * @param submitter the identity to submit under
   * @param pool the pool whose worker is to be tied up
   * @throws InterruptedException if this thread is interrupted while waiting
   */
  private void occupyTheWorker(Subject submitter, final ExecutorService pool)
      throws InterruptedException {
    final CountDownLatch running = new CountDownLatch(1);
    submitFromScope(submitter, pool, new Runnable() {
      @Override
      public void run() {
        running.countDown();
        try {
          releaseOccupiedWorkers.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    });
    assertTrue(running.await(TIMEOUT_SECONDS, TimeUnit.SECONDS),
        "the task meant to tie up the pool's worker never started");
  }

  /**
   * Hands {@code task} to {@code pool} from inside {@code submitter}'s scope.
   * <p>
   * Anything the pool throws in refusing the task is thrown from here as the pool
   * threw it, so that an assertion about a refusal can be made on this call.
   * Establishing an identity by calling rather than by acting reports a failure
   * inside the scope as the cause of an exception of its own on the runtimes that
   * have that method, which would hide the pool's refusal behind something else;
   * acting instead re-throws what was thrown, on every runtime. That is the same
   * distinction the tasks themselves are prepared with, and the reason for it is
   * the same.
   *
   * @param submitter the identity to submit under
   * @param pool the pool to submit to
   * @param task the task to submit
   */
  private void submitFromScope(Subject submitter, final ExecutorService pool,
      final Runnable task) {
    SubjectUtil.doAs(submitter, new PrivilegedAction<Void>() {
      @Override
      public Void run() {
        pool.execute(task);
        return (Void) null;
      }
    });
  }

  /**
   * Returns a fresh semaphored executor forwarding to a blocking pool of its
   * own, which is the composition the object-store filesystems build.
   * <p>
   * Both layers bound how much work is outstanding and both decorate what they
   * forward, so a task submitted here passes through two executors that each
   * prepare it and each wrap it again afterwards. Everything created is shut down
   * when the test ends, and each caller gets its own stack.
   *
   * @param prefix names the threads the blocking pool makes for itself
   * @return a semaphored executor over a blocking pool
   */
  private ExecutorService newSemaphoredOverBlocking(String prefix) {
    return register(new SemaphoredDelegatingExecutor(
        newBlocking("nested-" + prefix), 3, false));
  }

  /**
   * Asserts that both layers of a nested stack hold all of their permits and
   * have nobody waiting for one.
   *
   * @param outer the outer semaphored executor
   * @param outerPermits how many permits the outer layer has in total
   * @param inner the blocking pool it forwards to
   * @param innerPermits how many permits the inner layer has in total
   * @param after what had just happened, to name in a failure
   */
  private void assertPermitsRestored(SemaphoredDelegatingExecutor outer,
      int outerPermits, BlockingThreadPoolExecutorService inner,
      int innerPermits, String after) {
    assertEquals(outerPermits, outer.getAvailablePermits(),
        "the outer layer did not get every permit back after " + after);
    assertEquals(innerPermits, inner.getAvailablePermits(),
        "the inner layer did not get every permit back after " + after);
    assertEquals(0, outer.getWaitingCount(),
        "the outer layer was left with callers waiting for a permit after "
            + after);
    assertEquals(0, inner.getWaitingCount(),
        "the inner layer was left with callers waiting for a permit after "
            + after);
  }

  /**
   * Returns a fresh blocking pool with room for one task at a time.
   *
   * @param prefix names the threads the pool makes for itself
   * @return a blocking pool ready for a single test's use
   */
  private BlockingThreadPoolExecutorService newBlocking(String prefix) {
    return register(BlockingThreadPoolExecutorService.newInstance(
        1, 1, 10, TimeUnit.MINUTES, "blocking-" + prefix));
  }

  /**
   * Submits a runnable reporting its own subject, from inside
   * {@code submitter}'s scope, and returns what it observed.
   *
   * @param submitter the identity to submit under
   * @param pool the executor to submit to
   * @return the subject the runnable observed, which may be {@code null}
   * @throws Exception if the task fails or a wait times out
   */
  private Subject runnableSubmissionObserves(final Subject submitter,
      final ExecutorService pool) throws Exception {
    final SubjectRecorder recorder = new SubjectRecorder(1);
    final AtomicReference<Future<?>> submitted = new AtomicReference<>();
    SubjectUtil.callAs(submitter, new Callable<Void>() {
      public Void call() {
        submitted.set(pool.submit(recorder));
        return (Void) null;
      }
    });
    submitted.get().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    return awaitSingleRun(recorder);
  }

  /**
   * Submits a runnable reporting its own subject together with a result, from
   * inside {@code submitter}'s scope, and returns what it observed.
   * <p>
   * The result is checked as well, because this entry point exists to yield
   * one and carrying an identity must not disturb it.
   *
   * @param submitter the identity to submit under
   * @param pool the executor to submit to
   * @return the subject the runnable observed, which may be {@code null}
   * @throws Exception if the task fails or a wait times out
   */
  private Subject resultSubmissionObserves(final Subject submitter,
      final ExecutorService pool) throws Exception {
    final SubjectRecorder recorder = new SubjectRecorder(1);
    final AtomicReference<Future<String>> submitted = new AtomicReference<>();
    SubjectUtil.callAs(submitter, new Callable<Void>() {
      public Void call() {
        submitted.set(pool.submit(recorder, SENTINEL));
        return (Void) null;
      }
    });
    assertEquals(SENTINEL,
        submitted.get().get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
        "a runnable submitted with a result did not yield that result");
    return awaitSingleRun(recorder);
  }

  /**
   * Makes named daemon threads of the plainest kind available.
   * <p>
   * A thread of this kind is handed nothing when it is created on the runtimes
   * that carry nothing across a thread boundary, so a pool built on it can
   * only report an identity that arrived with the task itself. That is what
   * makes it the strictest vehicle for these assertions.
   */
  private static final class PlainDaemonThreadFactory
      implements ThreadFactory {

    private final String prefix;
    private final AtomicInteger created = new AtomicInteger(1);

    PlainDaemonThreadFactory(String prefix) {
      this.prefix = prefix;
    }

    @Override
    public Thread newThread(Runnable r) {
      Thread thread =
          new Thread(r, prefix + "-plain-" + created.getAndIncrement());
      thread.setDaemon(true);
      return thread;
    }
  }

  /**
   * An executor with one worker that holds, for as long as it lives, whatever
   * identity was in force when the executor was made.
   * <p>
   * This stands for an executor handed to Hadoop from outside, whose workers are
   * started by that executor rather than by Hadoop and are therefore started in
   * the ordinary way. A worker started that way takes its creator's identity and
   * keeps it in force around every task it takes off the queue, on every runtime
   * this project supports: on the ones that hand a thread its creator's identity
   * because the thread is given it, and on the ones that hand a thread nothing
   * because {@link SubjectInheritingThread} reads it when started and
   * establishes it again around the work. That makes this the one shape in which
   * a worker demonstrably has an identity to lend a task that arrived without
   * one, which is what the assertions using it are about.
   * <p>
   * It is deliberately the smallest thing that can be one: a queue, a worker
   * draining it, and the lifecycle methods a test needs to shut it down.
   * Inheriting {@link AbstractExecutorService} means every way of submitting
   * reaches the queue through {@link #execute}, exactly as it does for the pools
   * Hadoop builds itself.
   */
  private static final class IdentityCarryingExecutor
      extends AbstractExecutorService {

    private final BlockingQueue<Runnable> queued =
        new LinkedBlockingQueue<Runnable>();
    private final Thread worker;
    private volatile boolean stopped;

    IdentityCarryingExecutor(String name) {
      worker = new SubjectInheritingThread(new Runnable() {
        @Override
        public void run() {
          drainUntilStopped();
        }
      }, name + "-carrying-worker");
      worker.setDaemon(true);
      worker.start();
    }

    /**
     * Runs whatever is queued until this executor is shut down.
     * <p>
     * The wait is bounded so that a shutdown is noticed even when nothing is
     * queued to interrupt, and an interrupt ends the worker at once.
     */
    private void drainUntilStopped() {
      while (!stopped) {
        try {
          Runnable next = queued.poll(POLL_MILLIS, TimeUnit.MILLISECONDS);
          if (next != null) {
            next.run();
          }
        } catch (InterruptedException e) {
          return;
        }
      }
    }

    @Override
    public void execute(Runnable command) {
      queued.add(command);
    }

    @Override
    public void shutdown() {
      stopped = true;
      worker.interrupt();
    }

    @Override
    public List<Runnable> shutdownNow() {
      shutdown();
      List<Runnable> pending = new ArrayList<>();
      queued.drainTo(pending);
      return pending;
    }

    @Override
    public boolean isShutdown() {
      return stopped;
    }

    @Override
    public boolean isTerminated() {
      return stopped && !worker.isAlive();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit)
        throws InterruptedException {
      worker.join(unit.toMillis(timeout));
      return !worker.isAlive();
    }
  }

  /**
   * Records the subject of the thread that runs it, once for every run.
   * <p>
   * The recorded subjects are published to whoever is waiting through a latch,
   * and held in a list that tolerates being added to from a pool worker while
   * being copied here, so that a repeating task can be asked about all of its
   * runs.
   */
  private static final class SubjectRecorder implements Runnable {

    private final List<Subject> seen =
        Collections.synchronizedList(new ArrayList<Subject>());
    private final CountDownLatch runs;

    SubjectRecorder(int expectedRuns) {
      this.runs = new CountDownLatch(expectedRuns);
    }

    @Override
    public void run() {
      seen.add(SubjectUtil.current());
      runs.countDown();
    }

    /**
     * Waits for this task to have run as often as was expected of it.
     *
     * @return whether it did so before the wait ran out
     * @throws InterruptedException if this thread is interrupted while waiting
     */
    boolean awaitRuns() throws InterruptedException {
      return runs.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * Returns the subject each run of this task observed, in order.
     *
     * @return a copy of what has been recorded so far
     */
    List<Subject> observed() {
      return new ArrayList<>(seen);
    }
  }

  /**
   * A task that describes itself by a name of its own, so that an assertion can
   * tell whether a message about a task describes the task or the machinery
   * around it.
   */
  private static final class NamedTask implements Runnable {

    private final String name;

    NamedTask(String name) {
      this.name = name;
    }

    @Override
    public void run() {
    }

    @Override
    public String toString() {
      return name;
    }
  }

  /**
   * What one task observed of the identity it ran under and of the worker that
   * ran it.
   */
  private static final class RunObservation {

    private final Subject subject;
    private final Thread thread;

    RunObservation(Subject subject, Thread thread) {
      this.subject = subject;
      this.thread = thread;
    }
  }

  /**
   * A collection of tasks that takes its time over every task it yields.
   * <p>
   * Reading it is what a bulk submission does with the collection it is given,
   * so making the reading slow is what shows whether that reading falls inside
   * or outside the wait such a submission was given.
   */
  private static final class SlowlyReadTasks
      extends AbstractCollection<Callable<String>> {

    private final List<Callable<String>> tasks;
    private final long delayMillis;

    SlowlyReadTasks(List<Callable<String>> tasks, long delayMillis) {
      this.tasks = tasks;
      this.delayMillis = delayMillis;
    }

    @Override
    public Iterator<Callable<String>> iterator() {
      final Iterator<Callable<String>> source = tasks.iterator();
      return new Iterator<Callable<String>>() {
        @Override
        public boolean hasNext() {
          return source.hasNext();
        }

        @Override
        public Callable<String> next() {
          try {
            Thread.sleep(delayMillis);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while reading", e);
          }
          return source.next();
        }
      };
    }

    @Override
    public int size() {
      return tasks.size();
    }
  }

  /**
   * A collection of tasks that yields its first task at once and holds every
   * later one back until a gate has opened, recording whether it opened in
   * time.
   * <p>
   * The gate is opened by the first task itself, once it is running, so the
   * recorded outcome says whether the reading of the collection was interleaved
   * with the running of what had been read already or was finished before any
   * of it was handed over.
   */
  private static final class GatedTasks
      extends AbstractCollection<Callable<String>> {

    private final List<Callable<String>> tasks;
    private final CountDownLatch gate;
    private final CountDownLatch read;
    private final AtomicBoolean openedInTime;

    GatedTasks(List<Callable<String>> tasks, CountDownLatch gate,
        CountDownLatch read, AtomicBoolean openedInTime) {
      this.tasks = tasks;
      this.gate = gate;
      this.read = read;
      this.openedInTime = openedInTime;
    }

    @Override
    public Iterator<Callable<String>> iterator() {
      final Iterator<Callable<String>> source = tasks.iterator();
      return new Iterator<Callable<String>>() {
        private boolean firstYielded;

        @Override
        public boolean hasNext() {
          return source.hasNext();
        }

        @Override
        public Callable<String> next() {
          if (firstYielded) {
            try {
              openedInTime.set(gate.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new IllegalStateException("interrupted at the gate", e);
            }
            read.countDown();
          }
          firstYielded = true;
          return source.next();
        }
      };
    }

    @Override
    public int size() {
      return tasks.size();
    }
  }

  /**
   * The {@link Callable} counterpart of {@link NamedTask}.
   */
  private static final class NamedCallable implements Callable<String> {

    private final String name;

    NamedCallable(String name) {
      this.name = name;
    }

    @Override
    public String call() {
      return name;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  /**
   * Fills a key queue on demand and records the subject it was filled under,
   * but only for the fills that happen on a filler thread.
   * <p>
   * A key queue also fills on the asking thread itself, both when it first
   * loads a key's queue and when it has too few values left to answer a
   * request. Those fills observe the asking thread's own identity whether or
   * not that identity is carried anywhere, so recording them would report
   * success without a thread boundary having been crossed. Only the fills that
   * reach another thread are recorded.
   */
  private static final class RecordingRefiller
      implements ValueQueue.QueueRefiller<String> {

    /** The thread that asks for values, whose own fills prove nothing. */
    private final Thread askingThread = Thread.currentThread();
    private final AtomicReference<Subject> onFillerThread =
        new AtomicReference<>();
    private volatile CountDownLatch refilled = new CountDownLatch(0);

    @Override
    public void fillQueueForKey(String keyName, Queue<String> keyQueue,
        int numValues) {
      for (int i = 0; i < numValues; i++) {
        keyQueue.add(keyName + "-" + i);
      }
      if (Thread.currentThread() == askingThread) {
        return;
      }
      onFillerThread.set(SubjectUtil.current());
      refilled.countDown();
    }

    /**
     * Arms this refiller for one fill on a filler thread.
     *
     * @return a latch that counts down when that fill has happened
     */
    CountDownLatch expectRefillOnFillerThread() {
      onFillerThread.set(null);
      CountDownLatch latch = new CountDownLatch(1);
      refilled = latch;
      return latch;
    }

    /**
     * Returns the subject the last fill on a filler thread observed.
     *
     * @return that subject, which may be {@code null}
     */
    Subject subjectOnFillerThread() {
      return onFillerThread.get();
    }
  }
}
