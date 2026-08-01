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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;

import org.apache.hadoop.security.authentication.util.SubjectUtil;
import org.apache.hadoop.util.BlockingThreadPoolExecutorService;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.SemaphoredDelegatingExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

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
 * identity at all has to run with none, rather than adopting whatever its
 * worker was left holding, because running one caller's work as another user
 * decides authorization and is what an audit record names. And a task that
 * fails has to reach its future as the exception it threw, with nothing
 * wrapping it.
 * <p>
 * Every assertion here is about an observed identity rather than about how it
 * was carried, so the same assertions hold on every runtime this project
 * supports: where the runtime hands a new thread the identity of its creator
 * and where it hands over nothing, a submitted task observes its submitter
 * either way. Each assertion uses a pool created for it, whose first
 * submission is made from inside the establishing scope, and identities are
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

  /** Result a task returns to prove it ran to completion. */
  private static final String SENTINEL = "task-completed";

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
   * A pool that reuses its worker runs each task under its own submitter, not
   * under the one whose task caused the worker to exist.
   * <p>
   * The worker here is of the kind that holds an identity of its own for as
   * long as it lives, and this pool keeps it between the two submissions, so a
   * pool that read an identity only when it made a worker would run the second
   * task as the first submitter. Waiting for the first task before submitting
   * the second is what makes the reuse certain, and the two identities carry
   * principals of their own so that neither can pass for the other.
   *
   * @throws Exception if a task fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testReusedWorkerRunsEachTaskUnderItsOwnSubmitterSubject()
      throws Exception {
    Subject first = newSubject("first-submitter@EXAMPLE.COM");
    Subject second = newSubject("second-submitter@EXAMPLE.COM");
    ExecutorService pool = register(HadoopExecutors.newFixedThreadPool(
        1, BlockingThreadPoolExecutorService.newDaemonThreadFactory("reuse")));

    Subject observedByFirst = submissionObserves(first, pool);
    Subject observedBySecond = submissionObserves(second, pool);

    assertSame(first, observedByFirst,
        "the first task submitted did not observe its own submitter");
    assertSame(second, observedBySecond,
        "a task run by a reused worker observed the identity of an earlier "
            + "submitter instead of its own");
  }

  /**
   * A blocking pool that reuses its worker runs each task under its own
   * submitter, not under the one whose task caused the worker to exist.
   * <p>
   * This pool lets an idle worker be reclaimed, so it is given room for one
   * task at a time and a long enough idle period that the worker is certain to
   * still be there for the second submission: a worker replaced in between
   * would hide the very leak this asserts against.
   *
   * @throws Exception if a task fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testBlockingPoolReusedWorkerRunsEachTaskUnderItsOwnSubmitter()
      throws Exception {
    Subject first = newSubject("blocking-first@EXAMPLE.COM");
    Subject second = newSubject("blocking-second@EXAMPLE.COM");
    ExecutorService pool = register(BlockingThreadPoolExecutorService
        .newInstance(1, 1, 10, TimeUnit.MINUTES, "blocking-reuse"));

    Subject observedByFirst = submissionObserves(first, pool);
    Subject observedBySecond = submissionObserves(second, pool);

    assertSame(first, observedByFirst,
        "the first task submitted did not observe its own submitter");
    assertSame(second, observedBySecond,
        "a task run by a reused blocking-pool worker observed the identity of "
            + "an earlier submitter instead of its own");
  }

  /**
   * A task submitted with no identity runs with none, rather than adopting the
   * identity its worker was left holding.
   * <p>
   * The worker is created inside a scope that establishes an identity, so the
   * worker carries it for as long as it lives; the second submission is then
   * made from a thread carrying nothing. Letting that task run as the worker
   * would run one caller's work as another user, which decides authorization
   * and is what an audit record names, so the absence of an identity is
   * carried across just as deliberately as an identity is. Both kinds of
   * worker that hold an identity of their own are covered.
   *
   * @throws Exception if a task fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testSubjectlessSubmissionDoesNotAdoptTheWorkerSubject()
      throws Exception {
    Subject worker = newSubject("worker-identity@EXAMPLE.COM");

    ExecutorService inheriting = register(HadoopExecutors.newFixedThreadPool(
        1, BlockingThreadPoolExecutorService.newDaemonThreadFactory("held")));
    assertSame(worker, submissionObserves(worker, inheriting),
        "the task that created the worker did not observe its own submitter");
    assertNull(inheriting.submit(currentSubject())
            .get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
        "a task submitted with no identity adopted the identity its worker "
            + "was holding");

    ExecutorService daemons = register(HadoopExecutors.newFixedThreadPool(
        1, new Daemon.DaemonFactory()));
    assertSame(worker, submissionObserves(worker, daemons),
        "the task that created the daemon worker did not observe its own "
            + "submitter");
    assertNull(daemons.submit(currentSubject())
            .get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
        "a task submitted with no identity adopted the identity its daemon "
            + "worker was holding");
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
   * Returns a fresh semaphored executor over a pool of its own.
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
        register(HadoopExecutors.newFixedThreadPool(
            1, new PlainDaemonThreadFactory("semaphored-" + prefix))),
        2, false));
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
}
