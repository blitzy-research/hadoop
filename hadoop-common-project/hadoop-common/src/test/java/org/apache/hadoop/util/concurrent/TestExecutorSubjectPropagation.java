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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.SubjectUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.BlockingThreadPoolExecutorService;
import org.apache.hadoop.util.SemaphoredDelegatingExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * Asserts what identity a task handed to one of Hadoop's own executors runs
 * under.
 * <p>
 * Every test submits from a known identity and asserts on the identity the task
 * itself observed, across every factory in {@link HadoopExecutors}, every
 * submission entry point of {@link HadoopThreadPoolExecutor} and
 * {@link HadoopScheduledThreadPoolExecutor}, the two forwarding services those
 * factories return, and the executors in {@code org.apache.hadoop.util} that
 * decorate the tasks they forward.
 * <p>
 * What is expected of a runtime depends on that runtime. Where
 * {@link SubjectUtil#THREAD_INHERITS_SUBJECT} is {@code false} -- Java 24 and
 * later, JDK 25 among them -- the subject is read on the submitting thread and
 * re-established around the task, so a task runs under the identity of its own
 * submitter even on a worker somebody else brought into existence. Where the
 * flag is {@code true} -- Java 21 and earlier, JDK 17 among them -- the task is
 * handed on unchanged and observes whatever its worker holds, which is the
 * identity in force when that worker was created. Most tests here submit into a
 * pool of their own, whose worker can hold nothing but the submitter's identity,
 * so one expectation covers both runtimes. Two cases cannot be written that way,
 * and each states its expectation in terms of that flag rather than hiding the
 * difference: a worker that has already served one submitter and then serves
 * another, where the flag being {@code true} means the second submitter's task
 * observes the first one's identity; and a submission that carries no identity
 * at all.
 * <p>
 * A subject observed is necessary but not sufficient, so the boundary is read a
 * second time through {@link UserGroupInformation}, which is what production
 * code authorizes and audits against. It falls back to whoever the process
 * logged in as whenever it finds no subject, so a task that lost its submitter's
 * identity outright would still report a perfectly valid user and would have its
 * work authorized and recorded against that user instead. These tests therefore
 * install a login user that no task is ever submitted by and require every
 * observed user to be the submitting one, which turns that fall back into a
 * failure rather than a silent pass.
 * <p>
 * Identities are told apart by a principal of their own and compared by
 * reference; every observation crosses the thread boundary through a future or a
 * latch rather than through a sleep, every wait is bounded, and every executor a
 * test creates is shut down when the test ends.
 */
public class TestExecutorSubjectPropagation {

  /** Bound on every wait, generous enough to survive a loaded build host. */
  private static final long TIMEOUT_SECONDS = 10;

  private static final long PERIOD_MILLIS = 20;

  private static final int PERIODIC_RUNS = 3;

  private static final String SENTINEL = "task-completed";

  private static final String OTHER_SENTINEL = "other-task-completed";

  private static final String ALICE = "alice@EXAMPLE.COM";

  private static final String BOB = "bob@EXAMPLE.COM";

  /**
   * A user no task here is ever submitted by, so a task observing it can only
   * mean that its submitter's identity failed to reach it and
   * {@link UserGroupInformation#getCurrentUser()} fell back to the login.
   */
  private static final String LOGIN_USER = "sentinel-login-user";

  /** Released when a test ends, freeing a worker a test tied up. */
  private final CountDownLatch releaseOccupiedWorkers = new CountDownLatch(1);

  private final List<ExecutorService> pools = new ArrayList<>();

  private <E extends ExecutorService> E register(E pool) {
    pools.add(pool);
    return pool;
  }

  /**
   * Installs a known login user before each test, so that a fall back to it is
   * recognisable. The identity machinery keeps process-wide state, so it is put
   * into a known state here rather than being taken as found.
   */
  @BeforeEach
  public void setUpTheLoginUser() {
    UserGroupInformation.reset();
    UserGroupInformation.setConfiguration(new Configuration());
    UserGroupInformation.setLoginUser(
        UserGroupInformation.createRemoteUser(LOGIN_USER));
  }

  @AfterEach
  public void forgetTheLoginUser() {
    UserGroupInformation.setLoginUser(null);
    UserGroupInformation.reset();
  }

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
   * Returns a subject that no other subject can be mistaken for. A
   * {@link Subject} compares equal to another whose principals and credentials
   * match, so two subjects with nothing in them are equal; a principal of its
   * own is what lets an assertion distinguish two identities.
   */
  private static Subject newSubject(String principalName) {
    Subject subject = new Subject();
    subject.getPrincipals().add(new KerberosPrincipal(principalName));
    return subject;
  }

  /**
   * Runs an action under {@code subject} on this thread. Every submission in
   * this class is made from inside such a scope, and only the submission is:
   * waiting for the result happens afterwards, outside it, so that a passing
   * assertion shows the identity travelled with the task rather than with the
   * stack that submitted it.
   */
  private static <T> T as(Subject subject, PrivilegedAction<T> action) {
    return SubjectUtil.doAs(subject, action);
  }

  private static <T> T asChecked(Subject subject,
      PrivilegedExceptionAction<T> action) throws Exception {
    try {
      return SubjectUtil.doAs(subject, action);
    } catch (PrivilegedActionException e) {
      Exception cause = e.getException();
      throw cause != null ? cause : e;
    }
  }

  private static Callable<Subject> currentSubject() {
    return new Callable<Subject>() {
      @Override
      public Subject call() {
        return SubjectUtil.current();
      }
    };
  }

  private static Callable<Observation> currentObservation() {
    return new Callable<Observation>() {
      @Override
      public Observation call() throws IOException {
        return Observation.here();
      }
    };
  }

  private Subject submissionObserves(Subject submitter, ExecutorService pool)
      throws Exception {
    return submitObserving(submitter, pool).get(TIMEOUT_SECONDS,
        TimeUnit.SECONDS);
  }

  private Future<Subject> submitObserving(Subject submitter,
      ExecutorService pool) {
    return as(submitter, new PrivilegedAction<Future<Subject>>() {
      @Override
      public Future<Subject> run() {
        return pool.submit(currentSubject());
      }
    });
  }

  private Observation observeSubmission(Subject submitter, ExecutorService pool)
      throws Exception {
    Future<Observation> submitted =
        as(submitter, new PrivilegedAction<Future<Observation>>() {
          @Override
          public Future<Observation> run() {
            return pool.submit(currentObservation());
          }
        });
    return submitted.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  /**
   * Hands a task to {@link ExecutorService#execute}, from inside
   * {@code submitter}'s scope. That entry point yields no future, so the task
   * publishes its observation through a latch instead.
   */
  private Observation executionObserves(Subject submitter, ExecutorService pool)
      throws InterruptedException {
    Recorder recorder = new Recorder();
    as(submitter, new PrivilegedAction<Void>() {
      @Override
      public Void run() {
        pool.execute(recorder);
        return null;
      }
    });
    return recorder.awaitOne();
  }

  /**
   * What one task observed about the identity it ran under.
   */
  private static final class Observation {

    private final Subject subject;

    private final UserGroupInformation user;

    private final Thread worker;

    private Observation(Subject observedSubject, UserGroupInformation observedUser,
        Thread observedOn) {
      this.subject = observedSubject;
      this.user = observedUser;
      this.worker = observedOn;
    }

    static Observation here() throws IOException {
      return new Observation(SubjectUtil.current(),
          UserGroupInformation.getCurrentUser(), Thread.currentThread());
    }

    Subject subject() {
      return subject;
    }

    UserGroupInformation user() {
      return user;
    }

    Thread worker() {
      return worker;
    }
  }

  /**
   * A task that records what it observed and releases a latch.
   */
  private static final class Recorder implements Runnable {

    private final CountDownLatch ran = new CountDownLatch(1);

    /** What the run observed, published through {@link #ran}. */
    private final AtomicReference<Observation> observed =
        new AtomicReference<>();

    /** A failure taking the observation, rather than a lost observation. */
    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    @Override
    public void run() {
      try {
        observed.set(Observation.here());
      } catch (Throwable t) {
        failure.set(t);
      } finally {
        ran.countDown();
      }
    }

    Observation awaitOne() throws InterruptedException {
      assertTrue(ran.await(TIMEOUT_SECONDS, TimeUnit.SECONDS),
          "the task handed to the executor did not run");
      assertNull(failure.get(),
          "the task failed while reading the identity it ran under");
      Observation observation = observed.get();
      assertNotNull(observation, "the task recorded no observation");
      return observation;
    }
  }

  private static void assertObserved(Subject submitter, Subject observed,
      String what) {
    assertSame(submitter, observed,
        "a task submitted through " + what + " did not run under the subject "
            + "of the thread that submitted it");
  }

  /**
   * Asserts that a task observed the user of the identity it was submitted
   * under, and not the process login it would fall back to.
   */
  private static void assertObservedUser(UserGroupInformation expected,
      UserGroupInformation observed, String what) {
    assertEquals(expected, observed,
        "a task submitted through " + what + " would have been authorized as "
            + "the wrong user");
    assertNotEquals(LOGIN_USER, observed.getUserName(),
        "a task submitted through " + what + " fell back to the process login, "
            + "so it lost the identity of its submitter without failing");
  }

  /**
   * Asserts what a task submitted by a second identity observed on a worker a
   * first identity had already used.
   * <p>
   * This is the one case a suite using a single identity would never notice, and
   * the one whose observable outcome differs between runtimes. Where
   * {@link SubjectUtil#THREAD_INHERITS_SUBJECT} is {@code true} the worker was
   * given the first identity when the first submission brought it into existence
   * and goes on holding it, and the utility that carries a subject across a
   * thread boundary deliberately stays out of that runtime's way, so the second
   * identity's task observes the first. Where the flag is {@code false} the
   * subject is read at each submission, so the second identity's task observes
   * its own submitter and demonstrably not the earlier one.
   */
  private static void assertReusedWorkerObserved(Subject first, Subject second,
      Subject observed) {
    if (SubjectUtil.THREAD_INHERITS_SUBJECT) {
      assertSame(first, observed,
          "on a runtime that hands a new thread its creator's subject, a reused "
              + "worker is expected to keep the identity it was created with");
    } else {
      assertSame(second, observed,
          "a task run by a worker an earlier identity had already used did not "
              + "observe its own submitter");
      assertNotSame(first, observed,
          "a worker kept across submissions ran one submitter's task under an "
              + "earlier submitter's identity");
    }
  }

  /**
   * Returns a thread factory whose threads are named after {@code prefix}. They
   * are plain {@link Thread}s, deliberately: that is what the shaded builder
   * used throughout production code produces, so a pool built here reaches a
   * worker the same way a production pool does.
   */
  private static ThreadFactory namedFactory(String prefix) {
    final AtomicInteger created = new AtomicInteger();
    return new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        return new Thread(r, prefix + "-" + created.incrementAndGet());
      }
    };
  }

  /**
   * Returns a Hadoop pool with one worker and an unbounded queue. One worker is
   * what makes reuse observable: a second submission has no other thread it
   * could be given to.
   */
  private static HadoopThreadPoolExecutor singleThreadPool(String prefix) {
    return new HadoopThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<Runnable>(), namedFactory(prefix));
  }

  private static HadoopScheduledThreadPoolExecutor singleThreadScheduledPool() {
    return new HadoopScheduledThreadPoolExecutor(1,
        namedFactory("scheduled-pool"));
  }

  /**
   * Returns the forwarding service the single-thread factories hand back, built
   * over a plain JDK service exactly as
   * {@link HadoopExecutors#newSingleThreadExecutor()} builds it, so that what is
   * under test is the forwarding rather than the pool underneath.
   */
  private static SubjectPreservingExecutorService forwardingService() {
    return new SubjectPreservingExecutorService(
        Executors.newSingleThreadExecutor(namedFactory("forwarding")));
  }

  private static SubjectPreservingScheduledExecutorService
      forwardingScheduledService() {
    return new SubjectPreservingScheduledExecutorService(
        Executors.newSingleThreadScheduledExecutor(
            namedFactory("forwarding-scheduled")));
  }

  /**
   * Submits to a blocking pool of its very own, so that no worker can be holding
   * anyone else's identity and what the task observes can only have come from
   * the submission.
   */
  private Subject submissionObservesWithoutReuse(Subject submitter)
      throws Exception {
    BlockingThreadPoolExecutorService pool =
        register(BlockingThreadPoolExecutorService.newInstance(1, 4, 60,
            TimeUnit.SECONDS, "blocking-subject-own"));
    return submissionObserves(submitter, pool);
  }

  /**
   * Waits until a pool with a single worker has finished with everything handed
   * to it before this call, the pool's own after-execute reporting included.
   * <p>
   * A worker takes its next task only after the reporting hook for the previous
   * one has returned, and a pool with one worker takes them in the order they
   * were handed over, so a task submitted here completing is proof that every
   * earlier task has been run and reported on. That makes reading a log line or
   * a permit count written by the pool itself exact rather than a matter of
   * waiting long enough.
   */
  private static void awaitQuiescence(ExecutorService pool) throws Exception {
    Future<String> barrier = pool.submit(new Callable<String>() {
      @Override
      public String call() {
        return SENTINEL;
      }
    });
    assertSame(SENTINEL, barrier.get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
        "the task used to wait for the pool to catch up did not run");
  }

  /**
   * A task that records what every one of its runs observed. A periodic task
   * runs until it is cancelled, so the runs are collected and a latch counts
   * down the first few; the collection is read as a snapshot once that latch has
   * been released, which is why it tolerates being added to at the same time.
   */
  private static final class RepeatingRecorder implements Runnable {

    private final CountDownLatch ran;

    private final int expectedRuns;

    private final List<Observation> observations = new CopyOnWriteArrayList<>();

    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    RepeatingRecorder(int runs) {
      this.expectedRuns = runs;
      this.ran = new CountDownLatch(runs);
    }

    @Override
    public void run() {
      try {
        observations.add(Observation.here());
      } catch (Throwable t) {
        failure.set(t);
      } finally {
        ran.countDown();
      }
    }

    List<Observation> awaitAll() throws InterruptedException {
      assertTrue(ran.await(TIMEOUT_SECONDS, TimeUnit.SECONDS),
          "the periodic task ran fewer than " + expectedRuns + " times");
      assertNull(failure.get(),
          "a run of the periodic task failed while reading the identity it "
              + "ran under");
      List<Observation> runs = new ArrayList<>(observations);
      assertTrue(runs.size() >= expectedRuns,
          "the periodic task recorded fewer observations than it had runs");
      return runs;
    }
  }


  // Every factory in HadoopExecutors: two of them build a Hadoop pool directly,
  // four wrap a service obtained from java.util.concurrent.Executors because its
  // semantics are ones Hadoop chose not to reproduce, and the rest build a
  // Hadoop pool with a caller-supplied thread factory.

  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testCachedThreadPoolCarriesTheSubmittersSubject()
      throws Exception {
    Subject alice = newSubject(ALICE);
    ExecutorService pool = register(HadoopExecutors.newCachedThreadPool(
        namedFactory("cached")));
    assertObserved(alice, submissionObserves(alice, pool),
        "HadoopExecutors.newCachedThreadPool(ThreadFactory)");
  }

  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testFixedThreadPoolCarriesTheSubmittersSubject()
      throws Exception {
    Subject alice = newSubject(ALICE);
    ExecutorService pool = register(HadoopExecutors.newFixedThreadPool(1));
    assertObserved(alice, submissionObserves(alice, pool),
        "HadoopExecutors.newFixedThreadPool(int)");
  }

  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testFixedThreadPoolWithFactoryCarriesTheSubmittersSubject()
      throws Exception {
    Subject alice = newSubject(ALICE);
    ExecutorService pool = register(HadoopExecutors.newFixedThreadPool(1,
        namedFactory("fixed")));
    assertObserved(alice, submissionObserves(alice, pool),
        "HadoopExecutors.newFixedThreadPool(int, ThreadFactory)");
  }

  /**
   * That factory delegates to {@link Executors} for semantics Hadoop does not
   * reproduce, so what it returns is a forwarding service rather than a Hadoop
   * pool; this is the assertion that the forwarding still carries the identity.
   */
  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testSingleThreadExecutorCarriesTheSubmittersSubject()
      throws Exception {
    Subject alice = newSubject(ALICE);
    ExecutorService pool = register(HadoopExecutors.newSingleThreadExecutor());
    assertObserved(alice, submissionObserves(alice, pool),
        "HadoopExecutors.newSingleThreadExecutor()");
  }

  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testSingleThreadExecutorWithFactoryCarriesTheSubject()
      throws Exception {
    Subject alice = newSubject(ALICE);
    ExecutorService pool = register(HadoopExecutors.newSingleThreadExecutor(
        namedFactory("single")));
    assertObserved(alice, submissionObserves(alice, pool),
        "HadoopExecutors.newSingleThreadExecutor(ThreadFactory)");
  }

  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testScheduledThreadPoolCarriesTheSubmittersSubject()
      throws Exception {
    Subject alice = newSubject(ALICE);
    ExecutorService pool = register(HadoopExecutors.newScheduledThreadPool(1));
    assertObserved(alice, submissionObserves(alice, pool),
        "HadoopExecutors.newScheduledThreadPool(int)");
  }

  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testScheduledThreadPoolWithFactoryCarriesTheSubject()
      throws Exception {
    Subject alice = newSubject(ALICE);
    ExecutorService pool = register(HadoopExecutors.newScheduledThreadPool(1,
        namedFactory("scheduled")));
    assertObserved(alice, submissionObserves(alice, pool),
        "HadoopExecutors.newScheduledThreadPool(int, ThreadFactory)");
  }

  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testSingleThreadScheduledExecutorCarriesTheSubject()
      throws Exception {
    Subject alice = newSubject(ALICE);
    ScheduledExecutorService pool =
        register(HadoopExecutors.newSingleThreadScheduledExecutor());
    assertObserved(alice, submissionObserves(alice, pool),
        "HadoopExecutors.newSingleThreadScheduledExecutor()");
  }

  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testSingleThreadScheduledExecutorWithFactoryCarriesTheSubject()
      throws Exception {
    Subject alice = newSubject(ALICE);
    ScheduledExecutorService pool = register(
        HadoopExecutors.newSingleThreadScheduledExecutor(
            namedFactory("single-scheduled")));
    assertObserved(alice, submissionObserves(alice, pool),
        "HadoopExecutors.newSingleThreadScheduledExecutor(ThreadFactory)");
  }

  // Every submission entry point of HadoopThreadPoolExecutor. The pool prepares
  // a task in execute(Runnable) alone, on the grounds that the JDK routes all
  // three submit overloads and both bulk forms through it, so each entry point
  // is exercised in its own right rather than those grounds being taken on
  // trust.

  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testPoolExecuteCarriesTheSubmittersSubject() throws Exception {
    Subject alice = newSubject(ALICE);
    ExecutorService pool = register(singleThreadPool("execute"));
    Observation observed = executionObserves(alice, pool);
    assertObserved(alice, observed.subject(),
        "HadoopThreadPoolExecutor.execute(Runnable)");
  }

  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testPoolSubmitCallableCarriesTheSubmittersSubject()
      throws Exception {
    Subject alice = newSubject(ALICE);
    ExecutorService pool = register(singleThreadPool("submit-callable"));
    assertObserved(alice, submissionObserves(alice, pool),
        "HadoopThreadPoolExecutor.submit(Callable)");
  }

  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testPoolSubmitRunnableCarriesTheSubmittersSubject()
      throws Exception {
    Subject alice = newSubject(ALICE);
    ExecutorService pool = register(singleThreadPool("submit-runnable"));
    Recorder recorder = new Recorder();
    Future<?> submitted = as(alice, new PrivilegedAction<Future<?>>() {
      @Override
      public Future<?> run() {
        return pool.submit(recorder);
      }
    });
    assertNull(submitted.get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
        "a runnable submitted without a result should yield null");
    assertObserved(alice, recorder.awaitOne().subject(),
        "HadoopThreadPoolExecutor.submit(Runnable)");
  }

  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testPoolSubmitRunnableWithResultCarriesTheSubject()
      throws Exception {
    Subject alice = newSubject(ALICE);
    ExecutorService pool = register(singleThreadPool("submit-result"));
    Recorder recorder = new Recorder();
    Future<String> submitted =
        as(alice, new PrivilegedAction<Future<String>>() {
          @Override
          public Future<String> run() {
            return pool.submit(recorder, SENTINEL);
          }
        });
    assertSame(SENTINEL, submitted.get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
        "a runnable submitted with a result should yield that result");
    assertObserved(alice, recorder.awaitOne().subject(),
        "HadoopThreadPoolExecutor.submit(Runnable, T)");
  }

  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testPoolInvokeAllCarriesTheSubmittersSubject() throws Exception {
    Subject alice = newSubject(ALICE);
    ExecutorService pool = register(singleThreadPool("invoke-all"));
    List<Future<Subject>> futures =
        asChecked(alice, new PrivilegedExceptionAction<List<Future<Subject>>>() {
          @Override
          public List<Future<Subject>> run() throws InterruptedException {
            return pool.invokeAll(
                Arrays.asList(currentSubject(), currentSubject()));
          }
        });
    assertEquals(2, futures.size(), "invokeAll returned the wrong number of "
        + "futures");
    for (Future<Subject> future : futures) {
      assertObserved(alice, future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
          "HadoopThreadPoolExecutor.invokeAll(Collection)");
    }
  }

  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testPoolTimedInvokeAllCarriesTheSubmittersSubject()
      throws Exception {
    Subject alice = newSubject(ALICE);
    ExecutorService pool = register(singleThreadPool("timed-invoke-all"));
    List<Future<Subject>> futures =
        asChecked(alice, new PrivilegedExceptionAction<List<Future<Subject>>>() {
          @Override
          public List<Future<Subject>> run() throws InterruptedException {
            return pool.invokeAll(
                Arrays.asList(currentSubject(), currentSubject()),
                TIMEOUT_SECONDS, TimeUnit.SECONDS);
          }
        });
    assertEquals(2, futures.size(), "a timed invokeAll returned the wrong "
        + "number of futures");
    for (Future<Subject> future : futures) {
      assertObserved(alice, future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
          "HadoopThreadPoolExecutor.invokeAll(Collection, long, TimeUnit)");
    }
  }

  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testPoolInvokeAnyCarriesTheSubmittersSubject() throws Exception {
    Subject alice = newSubject(ALICE);
    ExecutorService pool = register(singleThreadPool("invoke-any"));
    Subject observed = asChecked(alice, new PrivilegedExceptionAction<Subject>() {
      @Override
      public Subject run() throws InterruptedException, ExecutionException {
        return pool.invokeAny(Arrays.asList(currentSubject(), currentSubject()));
      }
    });
    assertObserved(alice, observed,
        "HadoopThreadPoolExecutor.invokeAny(Collection)");
  }

  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testPoolTimedInvokeAnyCarriesTheSubmittersSubject()
      throws Exception {
    Subject alice = newSubject(ALICE);
    ExecutorService pool = register(singleThreadPool("timed-invoke-any"));
    Subject observed = asChecked(alice, new PrivilegedExceptionAction<Subject>() {
      @Override
      public Subject run() throws Exception {
        return pool.invokeAny(Arrays.asList(currentSubject(), currentSubject()),
            TIMEOUT_SECONDS, TimeUnit.SECONDS);
      }
    });
    assertObserved(alice, observed,
        "HadoopThreadPoolExecutor.invokeAny(Collection, long, TimeUnit)");
  }


  // Every scheduling entry point of HadoopScheduledThreadPoolExecutor. That
  // pool prepares a task in its four scheduling methods and nowhere else,
  // because the JDK routes execute and all three submit overloads through
  // schedule; preparing at both layers would leave two layers where the code
  // that inspects a queued task takes one off.

  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testScheduleRunnableCarriesTheSchedulingSubject()
      throws Exception {
    Subject alice = newSubject(ALICE);
    ScheduledExecutorService pool = register(singleThreadScheduledPool());
    Recorder recorder = new Recorder();
    ScheduledFuture<?> scheduled =
        as(alice, new PrivilegedAction<ScheduledFuture<?>>() {
          @Override
          public ScheduledFuture<?> run() {
            return pool.schedule(recorder, 0, TimeUnit.MILLISECONDS);
          }
        });
    assertNull(scheduled.get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
        "a scheduled runnable should yield null");
    assertObserved(alice, recorder.awaitOne().subject(),
        "HadoopScheduledThreadPoolExecutor.schedule(Runnable, long, TimeUnit)");
  }

  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testScheduleCallableCarriesTheSchedulingSubject()
      throws Exception {
    Subject alice = newSubject(ALICE);
    ScheduledExecutorService pool = register(singleThreadScheduledPool());
    ScheduledFuture<Subject> scheduled =
        as(alice, new PrivilegedAction<ScheduledFuture<Subject>>() {
          @Override
          public ScheduledFuture<Subject> run() {
            return pool.schedule(currentSubject(), 0, TimeUnit.MILLISECONDS);
          }
        });
    assertObserved(alice, scheduled.get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
        "HadoopScheduledThreadPoolExecutor.schedule(Callable, long, TimeUnit)");
  }

  /**
   * A repeating task is where an identity read once could most easily be lost by
   * the second run, and it is what a credential-maintaining task depends on, so
   * several runs are observed rather than one.
   */
  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testScheduleAtFixedRateCarriesTheSchedulingSubjectEveryRun()
      throws Exception {
    Subject alice = newSubject(ALICE);
    ScheduledExecutorService pool = register(singleThreadScheduledPool());
    RepeatingRecorder recorder = new RepeatingRecorder(PERIODIC_RUNS);
    ScheduledFuture<?> scheduled =
        as(alice, new PrivilegedAction<ScheduledFuture<?>>() {
          @Override
          public ScheduledFuture<?> run() {
            return pool.scheduleAtFixedRate(recorder, 0, PERIOD_MILLIS,
                TimeUnit.MILLISECONDS);
          }
        });
    List<Observation> runs = recorder.awaitAll();
    scheduled.cancel(false);
    assertEquals(PERIODIC_RUNS, runs.size(),
        "the periodic task did not run the expected number of times");
    for (Observation run : runs) {
      assertObserved(alice, run.subject(),
          "HadoopScheduledThreadPoolExecutor.scheduleAtFixedRate");
    }
  }

  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testScheduleWithFixedDelayCarriesTheSchedulingSubjectEveryRun()
      throws Exception {
    Subject alice = newSubject(ALICE);
    ScheduledExecutorService pool = register(singleThreadScheduledPool());
    RepeatingRecorder recorder = new RepeatingRecorder(PERIODIC_RUNS);
    ScheduledFuture<?> scheduled =
        as(alice, new PrivilegedAction<ScheduledFuture<?>>() {
          @Override
          public ScheduledFuture<?> run() {
            return pool.scheduleWithFixedDelay(recorder, 0, PERIOD_MILLIS,
                TimeUnit.MILLISECONDS);
          }
        });
    List<Observation> runs = recorder.awaitAll();
    scheduled.cancel(false);
    assertEquals(PERIODIC_RUNS, runs.size(),
        "the periodic task did not run the expected number of times");
    for (Observation run : runs) {
      assertObserved(alice, run.subject(),
          "HadoopScheduledThreadPoolExecutor.scheduleWithFixedDelay");
    }
  }

  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testScheduledPoolExecuteCarriesTheSubmittersSubject()
      throws Exception {
    Subject alice = newSubject(ALICE);
    ScheduledExecutorService pool = register(singleThreadScheduledPool());
    assertObserved(alice, executionObserves(alice, pool).subject(),
        "HadoopScheduledThreadPoolExecutor.execute(Runnable)");
  }

  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testScheduledPoolSubmitCarriesTheSubmittersSubject()
      throws Exception {
    Subject alice = newSubject(ALICE);
    ScheduledExecutorService pool = register(singleThreadScheduledPool());
    assertObserved(alice, submissionObserves(alice, pool),
        "HadoopScheduledThreadPoolExecutor.submit(Callable)");
  }

  // The two forwarding services, exercised in their own right. The single-thread
  // factories hand back a service that forwards to one of the JDK's own, so
  // every intercepted entry point is covered here directly, on a delegate
  // obtained the same way those factories obtain theirs.

  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testForwardingServiceExecuteCarriesTheSubmittersSubject()
      throws Exception {
    Subject alice = newSubject(ALICE);
    ExecutorService pool = register(forwardingService());
    assertObserved(alice, executionObserves(alice, pool).subject(),
        "SubjectPreservingExecutorService.execute(Runnable)");
  }

  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testForwardingServiceSubmitCarriesTheSubmittersSubject()
      throws Exception {
    Subject alice = newSubject(ALICE);
    ExecutorService pool = register(forwardingService());
    assertObserved(alice, submissionObserves(alice, pool),
        "SubjectPreservingExecutorService.submit(Callable)");
  }

  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testForwardingServiceSubmitRunnableCarriesTheSubject()
      throws Exception {
    Subject alice = newSubject(ALICE);
    ExecutorService pool = register(forwardingService());
    Recorder recorder = new Recorder();
    Future<String> withResult =
        as(alice, new PrivilegedAction<Future<String>>() {
          @Override
          public Future<String> run() {
            return pool.submit(recorder, OTHER_SENTINEL);
          }
        });
    assertSame(OTHER_SENTINEL, withResult.get(TIMEOUT_SECONDS,
        TimeUnit.SECONDS), "a forwarded runnable lost its result");
    assertObserved(alice, recorder.awaitOne().subject(),
        "SubjectPreservingExecutorService.submit(Runnable, T)");
  }

  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testForwardingServiceInvokeAllCarriesTheSubmittersSubject()
      throws Exception {
    Subject alice = newSubject(ALICE);
    ExecutorService pool = register(forwardingService());
    List<Future<Subject>> untimed =
        asChecked(alice, new PrivilegedExceptionAction<List<Future<Subject>>>() {
          @Override
          public List<Future<Subject>> run() throws InterruptedException {
            return pool.invokeAll(
                Arrays.asList(currentSubject(), currentSubject()));
          }
        });
    for (Future<Subject> future : untimed) {
      assertObserved(alice, future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
          "SubjectPreservingExecutorService.invokeAll(Collection)");
    }
    List<Future<Subject>> timed =
        asChecked(alice, new PrivilegedExceptionAction<List<Future<Subject>>>() {
          @Override
          public List<Future<Subject>> run() throws InterruptedException {
            return pool.invokeAll(
                Arrays.asList(currentSubject(), currentSubject()),
                TIMEOUT_SECONDS, TimeUnit.SECONDS);
          }
        });
    for (Future<Subject> future : timed) {
      assertObserved(alice, future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
          "SubjectPreservingExecutorService.invokeAll(Collection, long, "
              + "TimeUnit)");
    }
  }

  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testForwardingServiceInvokeAnyCarriesTheSubmittersSubject()
      throws Exception {
    Subject alice = newSubject(ALICE);
    ExecutorService pool = register(forwardingService());
    Subject untimed = asChecked(alice, new PrivilegedExceptionAction<Subject>() {
      @Override
      public Subject run() throws InterruptedException, ExecutionException {
        return pool.invokeAny(Arrays.asList(currentSubject(), currentSubject()));
      }
    });
    assertObserved(alice, untimed,
        "SubjectPreservingExecutorService.invokeAny(Collection)");
    Subject timed = asChecked(alice, new PrivilegedExceptionAction<Subject>() {
      @Override
      public Subject run() throws Exception {
        return pool.invokeAny(Arrays.asList(currentSubject(), currentSubject()),
            TIMEOUT_SECONDS, TimeUnit.SECONDS);
      }
    });
    assertObserved(alice, timed,
        "SubjectPreservingExecutorService.invokeAny(Collection, long, "
            + "TimeUnit)");
  }

  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testForwardingScheduledServiceCarriesTheSchedulingSubject()
      throws Exception {
    Subject alice = newSubject(ALICE);
    ScheduledExecutorService pool = register(forwardingScheduledService());

    ScheduledFuture<Subject> callable =
        as(alice, new PrivilegedAction<ScheduledFuture<Subject>>() {
          @Override
          public ScheduledFuture<Subject> run() {
            return pool.schedule(currentSubject(), 0, TimeUnit.MILLISECONDS);
          }
        });
    assertObserved(alice, callable.get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
        "SubjectPreservingScheduledExecutorService.schedule(Callable, long, "
            + "TimeUnit)");

    Recorder once = new Recorder();
    as(alice, new PrivilegedAction<ScheduledFuture<?>>() {
      @Override
      public ScheduledFuture<?> run() {
        return pool.schedule(once, 0, TimeUnit.MILLISECONDS);
      }
    });
    assertObserved(alice, once.awaitOne().subject(),
        "SubjectPreservingScheduledExecutorService.schedule(Runnable, long, "
            + "TimeUnit)");

    RepeatingRecorder atRate = new RepeatingRecorder(PERIODIC_RUNS);
    ScheduledFuture<?> rated =
        as(alice, new PrivilegedAction<ScheduledFuture<?>>() {
          @Override
          public ScheduledFuture<?> run() {
            return pool.scheduleAtFixedRate(atRate, 0, PERIOD_MILLIS,
                TimeUnit.MILLISECONDS);
          }
        });
    for (Observation run : atRate.awaitAll()) {
      assertObserved(alice, run.subject(),
          "SubjectPreservingScheduledExecutorService.scheduleAtFixedRate");
    }
    rated.cancel(false);

    RepeatingRecorder withDelay = new RepeatingRecorder(PERIODIC_RUNS);
    ScheduledFuture<?> delayed =
        as(alice, new PrivilegedAction<ScheduledFuture<?>>() {
          @Override
          public ScheduledFuture<?> run() {
            return pool.scheduleWithFixedDelay(withDelay, 0, PERIOD_MILLIS,
                TimeUnit.MILLISECONDS);
          }
        });
    for (Observation run : withDelay.awaitAll()) {
      assertObserved(alice, run.subject(),
          "SubjectPreservingScheduledExecutorService.scheduleWithFixedDelay");
    }
    delayed.cancel(false);
  }

  // The executors in org.apache.hadoop.util that decorate what they forward.
  // SemaphoredDelegatingExecutor already wraps each task it submits, to release
  // the permit it took, so the identity has to be carried in composition with
  // that existing decoration rather than instead of it. Its bulk methods refuse
  // to run at all and are left as they are, and BlockingThreadPoolExecutorService
  // submits through it, so one composition covers both.

  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testSemaphoredExecutorCarriesTheSubmittersSubject()
      throws Exception {
    Subject alice = newSubject(ALICE);
    ExecutorService delegate = register(singleThreadPool("semaphored"));
    SemaphoredDelegatingExecutor pool =
        new SemaphoredDelegatingExecutor(delegate, 2, false);

    assertObserved(alice, submissionObserves(alice, pool),
        "SemaphoredDelegatingExecutor.submit(Callable)");
    assertObserved(alice, executionObserves(alice, pool).subject(),
        "SemaphoredDelegatingExecutor.execute(Runnable)");

    Recorder plain = new Recorder();
    Future<?> withoutResult = as(alice, new PrivilegedAction<Future<?>>() {
      @Override
      public Future<?> run() {
        return pool.submit(plain);
      }
    });
    assertNull(withoutResult.get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
        "a runnable submitted without a result should yield null");
    assertObserved(alice, plain.awaitOne().subject(),
        "SemaphoredDelegatingExecutor.submit(Runnable)");

    Recorder withValue = new Recorder();
    Future<String> withResult =
        as(alice, new PrivilegedAction<Future<String>>() {
          @Override
          public Future<String> run() {
            return pool.submit(withValue, SENTINEL);
          }
        });
    assertSame(SENTINEL, withResult.get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
        "a runnable submitted with a result lost it");
    assertObserved(alice, withValue.awaitOne().subject(),
        "SemaphoredDelegatingExecutor.submit(Runnable, T)");

    // A permit is given back inside the task, which for a submission happens
    // just after the result is published, so the delegate is let catch up first;
    // it has one worker, so that is exact.
    awaitQuiescence(delegate);
    assertEquals(2, pool.getAvailablePermits(),
        "the semaphored executor did not give back the permits it took, so "
            + "carrying the identity broke its permit accounting");
  }

  /**
   * The blocking pool's workers are threads that take an identity of their own
   * when they are created, so this is the pool where a task could most plausibly
   * appear to observe its submitter while in fact observing its worker. The
   * identity asserted on is one that never created a worker, which rules that
   * out.
   */
  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testBlockingThreadPoolCarriesTheSubmittersSubject()
      throws Exception {
    Subject alice = newSubject(ALICE);
    Subject bob = newSubject(BOB);
    BlockingThreadPoolExecutorService pool = register(
        BlockingThreadPoolExecutorService.newInstance(1, 4, 60,
            TimeUnit.SECONDS, "blocking-subject"));

    assertObserved(alice, submissionObserves(alice, pool),
        "BlockingThreadPoolExecutorService.submit(Callable)");

    Observation byBob = observeSubmission(bob, pool);
    assertReusedWorkerObserved(alice, bob, byBob.subject());
    assertObserved(bob, submissionObservesWithoutReuse(bob),
        "BlockingThreadPoolExecutorService on a pool of its own");
  }

  // A worker that serves one submitter and then another: the case a suite using
  // a single identity would never notice, and the reason the identity is read at
  // each submission, wherever it is read at all, rather than once per worker.
  // Both submissions deliberately go into one pool, and the worker is asserted to
  // be the same one first: a pool free to replace an idle worker between two
  // submissions would otherwise turn the assertion into one about a fresh worker
  // and stop covering reuse.

  /**
   * A single worker of a Hadoop pool, having run one identity's task, runs the
   * next identity's task under the identity the runtime makes observable.
   */
  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testReusedWorkerRunsTheSecondSubmittersTask() throws Exception {
    Subject alice = newSubject(ALICE);
    Subject bob = newSubject(BOB);
    ExecutorService pool = register(singleThreadPool("reuse"));

    Observation byAlice = observeSubmission(alice, pool);
    assertObserved(alice, byAlice.subject(), "the first submission");

    Observation byBob = observeSubmission(bob, pool);
    assertSame(byAlice.worker(), byBob.worker(),
        "the two submissions did not share a worker, so this assertion no "
            + "longer covers a reused worker at all");
    assertReusedWorkerObserved(alice, bob, byBob.subject());
  }

  /**
   * A reused worker of the forwarding service behaves the same way, which
   * matters because that service is what the single-thread factories return and
   * a single-thread service reuses its one worker by definition.
   */
  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testReusedWorkerOfTheForwardingServiceRunsTheSecondSubmitter()
      throws Exception {
    Subject alice = newSubject(ALICE);
    Subject bob = newSubject(BOB);
    ExecutorService pool = register(HadoopExecutors.newSingleThreadExecutor(
        namedFactory("reuse-forwarding")));

    Observation byAlice = observeSubmission(alice, pool);
    assertObserved(alice, byAlice.subject(), "the first submission");

    Observation byBob = observeSubmission(bob, pool);
    assertSame(byAlice.worker(), byBob.worker(),
        "a single-thread service ran the two submissions on different workers");
    assertReusedWorkerObserved(alice, bob, byBob.subject());
  }

  // A submission that carries no identity. Establishing an absent identity is
  // not the same as leaving one out: the replacement API accepts a null subject
  // and binds it, which would hide an identity in force where the task runs.
  // Such a submission is therefore passed on untouched, and what it observes is
  // whatever its worker holds.

  /**
   * A task submitted with no identity, to a pool whose worker was never given
   * one either, observes no identity.
   */
  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testSubjectlessSubmissionToASubjectlessWorkerObservesNothing()
      throws Exception {
    ExecutorService pool = register(singleThreadPool("no-subject"));
    Observation observed = observeSubmission(null, pool);
    assertNull(observed.subject(),
        "a task submitted with no identity, on a worker that was given none, "
            + "observed an identity from somewhere");
    assertEquals(LOGIN_USER, observed.user().getUserName(),
        "a task with no identity should be authorized as the process login, "
            + "which is the documented fall back");
  }

  /**
   * A task submitted with no identity, on a worker an earlier identity brought
   * into existence, observes what that runtime makes observable and never an
   * identity the utility supplied.
   */
  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testSubjectlessSubmissionObservesOnlyItsWorkersOwnIdentity()
      throws Exception {
    Subject alice = newSubject(ALICE);
    ExecutorService pool = register(singleThreadPool("no-subject-reuse"));

    Observation byAlice = observeSubmission(alice, pool);
    assertObserved(alice, byAlice.subject(), "the first submission");

    Observation withoutSubject = observeSubmission(null, pool);
    assertSame(byAlice.worker(), withoutSubject.worker(),
        "the two submissions did not share a worker, so this assertion no "
            + "longer covers what a reused worker was left holding");
    if (SubjectUtil.THREAD_INHERITS_SUBJECT) {
      assertSame(alice, withoutSubject.subject(),
          "on a runtime that hands a new thread its creator's subject, a "
              + "worker keeps the identity it was created with");
    } else {
      assertNull(withoutSubject.subject(),
          "a worker of this pool is given no identity of its own, so a "
              + "submission carrying none should observe none");
    }
  }

  // A task that waits in the queue. The identity is read when a task is handed
  // over, not when a worker finally gets to it, and the difference only shows
  // where the two are far apart: here the only worker is deliberately tied up,
  // so the task under test is still in the queue when the scope it was submitted
  // in has been left.

  /**
   * A task still queued when its submitting scope ends observes the identity the
   * runtime makes observable: its own submitter's where the subject is read at
   * submission, and its worker's where the runtime hands a new thread the
   * identity of its creator.
   */
  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testTheIdentityIsReadAtSubmissionAndNotWhenTheTaskRuns()
      throws Exception {
    Subject alice = newSubject(ALICE);
    Subject bob = newSubject(BOB);
    ExecutorService pool = register(singleThreadPool("queued"));
    CountDownLatch occupied = new CountDownLatch(1);

    // Tie the only worker up, from inside bob's scope, so that the worker
    // itself is the one a runtime that inherits would have given bob's identity.
    as(bob, new PrivilegedAction<Void>() {
      @Override
      public Void run() {
        pool.execute(new Runnable() {
          @Override
          public void run() {
            occupied.countDown();
            try {
              releaseOccupiedWorkers.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }
        });
        return null;
      }
    });
    assertTrue(occupied.await(TIMEOUT_SECONDS, TimeUnit.SECONDS),
        "the task meant to tie the only worker up never started");

    Future<Subject> queued = submitObserving(alice, pool);
    assertFalse(queued.isDone(),
        "the task under test ran while the only worker was still busy, so this "
            + "no longer covers a task outliving the scope it was submitted in");

    releaseOccupiedWorkers.countDown();
    assertReusedWorkerObserved(bob, alice,
        queued.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
  }

  // What a failing task hands back. The replacement identity API is specified to
  // wrap anything thrown out of the action in a CompletionException, and
  // establishing an identity around a task must not change what a caller of
  // Future#get sees, nor what the pool's own after-execute reporting sees, so
  // the exception is asserted on by reference.

  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testACheckedFailureReachesTheCallerUnchanged() throws Exception {
    Subject alice = newSubject(ALICE);
    ExecutorService pool = register(singleThreadPool("checked-failure"));
    IOException thrown = new IOException("the task could not read");

    Future<Object> submitted =
        as(alice, new PrivilegedAction<Future<Object>>() {
          @Override
          public Future<Object> run() {
            return pool.submit(new Callable<Object>() {
              @Override
              public Object call() throws IOException {
                throw thrown;
              }
            });
          }
        });

    ExecutionException failed = assertThrows(ExecutionException.class,
        () -> submitted.get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
        "a task that threw should have failed its future");
    assertSame(thrown, failed.getCause(),
        "the exception the task threw did not reach the caller as itself, so "
            + "establishing an identity around a task changed what callers see");
  }

  /**
   * The forwarding service establishes the identity around the task the caller
   * passed in rather than around the future built from it, so it is a separate
   * path and gets its own reading.
   */
  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testForwardingServiceFailureReachesTheCallerUnchanged()
      throws Exception {
    Subject alice = newSubject(ALICE);
    ExecutorService pool = register(forwardingService());
    IOException thrown = new IOException("the forwarded task could not read");

    Future<Object> submitted =
        as(alice, new PrivilegedAction<Future<Object>>() {
          @Override
          public Future<Object> run() {
            return pool.submit(new Callable<Object>() {
              @Override
              public Object call() throws IOException {
                throw thrown;
              }
            });
          }
        });

    ExecutionException failed = assertThrows(ExecutionException.class,
        () -> submitted.get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
        "a task that threw should have failed its future");
    assertSame(thrown, failed.getCause(),
        "the exception the forwarded task threw did not reach the caller as "
            + "itself");
  }

  // What the pool reports about the task it ran. A task that has had an identity
  // established around it is no longer an object of the class the submitter
  // passed in, and no longer a Future either. Both are read: the pool names the
  // task in its debug line, and its after-execute reporting recognises a
  // completed Future in order to report the exception a submitted task threw,
  // which nothing else reports. Every such reading takes the added layer back
  // off first, and these tests assert on what an operator would actually see.

  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testDebugLoggingNamesTheTaskThatWasSubmitted() throws Exception {
    Subject alice = newSubject(ALICE);
    ExecutorService pool = register(singleThreadPool("debug-log"));
    Logger poolLog = LoggerFactory.getLogger(HadoopThreadPoolExecutor.class);
    Logger helperLog = LoggerFactory.getLogger(ExecutorHelper.class);
    GenericTestUtils.setLogLevel(poolLog, Level.DEBUG);
    GenericTestUtils.setLogLevel(helperLog, Level.DEBUG);
    GenericTestUtils.LogCapturer poolLines =
        GenericTestUtils.LogCapturer.captureLogs(poolLog);
    GenericTestUtils.LogCapturer helperLines =
        GenericTestUtils.LogCapturer.captureLogs(helperLog);
    try {
      Observation observed = executionObserves(alice, pool);
      assertObserved(alice, observed.subject(),
          "HadoopThreadPoolExecutor.execute(Runnable) with debug logging on");

      // The after-execute line is written once the task has returned, so the
      // pool is let catch up before its output is read.
      awaitQuiescence(pool);

      String submitted = Recorder.class.getName();
      assertTrue(poolLines.getOutput().contains("beforeExecute")
              && helperLines.getOutput().contains("afterExecute"),
          "the pool wrote neither of the debug lines it writes per task");
      assertTrue(poolLines.getOutput().contains(submitted),
          "the before-execute line did not name the submitted task's class: "
              + poolLines.getOutput());
      assertFalse(poolLines.getOutput().contains("SubjectPreservingTasks"),
          "the before-execute line named the class used to carry the identity "
              + "instead of the task, changing what operators read: "
              + poolLines.getOutput());
      assertTrue(helperLines.getOutput().contains(submitted),
          "the after-execute line did not name the submitted task's class: "
              + helperLines.getOutput());
      assertFalse(helperLines.getOutput().contains("SubjectPreservingTasks"),
          "the after-execute line named the class used to carry the identity "
              + "instead of the task: " + helperLines.getOutput());
    } finally {
      poolLines.stopCapturing();
      helperLines.stopCapturing();
      GenericTestUtils.setLogLevel(poolLog, Level.INFO);
      GenericTestUtils.setLogLevel(helperLog, Level.INFO);
    }
  }

  /**
   * The exception a submitted task threw is still reported by the pool.
   * <p>
   * Nothing else reports it: a submission puts the exception in its future and a
   * caller that never reads that future would otherwise learn nothing, which is
   * exactly the case this reporting exists for. It works by recognising a
   * completed future among the tasks the pool ran, so it only keeps working if
   * the layer that carries the identity is taken off before that test.
   */
  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testTheExceptionOfASubmittedTaskIsStillReported()
      throws Exception {
    Subject alice = newSubject(ALICE);
    ExecutorService pool = register(singleThreadPool("failure-log"));
    Logger helperLog = LoggerFactory.getLogger(ExecutorHelper.class);
    GenericTestUtils.LogCapturer helperLines =
        GenericTestUtils.LogCapturer.captureLogs(helperLog);
    try {
      IllegalStateException thrown =
          new IllegalStateException("the submitted task refused to run");
      Future<?> submitted = as(alice, new PrivilegedAction<Future<?>>() {
        @Override
        public Future<?> run() {
          return pool.submit(new Runnable() {
            @Override
            public void run() {
              throw thrown;
            }
          });
        }
      });

      ExecutionException failed = assertThrows(ExecutionException.class,
          () -> submitted.get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
          "a task that threw should have failed its future");
      assertSame(thrown, failed.getCause(),
          "the exception the task threw did not reach the caller as itself");

      // The report is written once the task has returned, so the pool is let
      // catch up before its output is read.
      awaitQuiescence(pool);
      assertTrue(helperLines.getOutput().contains(thrown.getMessage()),
          "the pool never reported the exception a submitted task threw, so a "
              + "caller that does not read the future would learn nothing: "
              + helperLines.getOutput());
    } finally {
      helperLines.stopCapturing();
    }
  }

  // What is handed on untouched. Two things are deliberately never given an
  // identity to carry. A submission of nothing at all is passed straight on, so
  // that an executor refuses it where it always did, on the thread that made it,
  // rather than accepting it and failing later on a worker. And a task that
  // already carries one is not given a second, which is what keeps a single
  // reading enough to recover the task an executor was handed - the reading both
  // of the diagnostics above depend on.

  /**
   * A submission of nothing is refused by the executor that was handed it, on
   * the thread that made it.
   * <p>
   * The submission is made from inside an identity's scope, because that is the
   * case in which there is an identity to carry and therefore something for the
   * task to be wrapped in. Were nothing passed on as it is, an executor would be
   * handed a wrapper around nothing: it would accept the submission, and the
   * failure would surface later, on a worker thread, where no caller is waiting
   * for it.
   */
  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testASubmissionOfNothingIsRefusedWhereItAlwaysWas()
      throws Exception {
    Subject alice = newSubject(ALICE);
    HadoopThreadPoolExecutor pool = register(singleThreadPool("null-task"));
    SubjectPreservingExecutorService forwarding = register(forwardingService());
    SubjectPreservingScheduledExecutorService scheduled =
        register(forwardingScheduledService());
    HadoopScheduledThreadPoolExecutor scheduledPool =
        register(singleThreadScheduledPool());

    as(alice, new PrivilegedAction<Void>() {
      @Override
      public Void run() {
        assertThrows(NullPointerException.class, () -> pool.execute(null),
            "a Hadoop pool accepted a submission of nothing");
        assertThrows(NullPointerException.class,
            () -> pool.submit((Callable<Object>) null),
            "a Hadoop pool accepted a call of nothing");
        assertThrows(NullPointerException.class,
            () -> forwarding.execute(null),
            "a forwarding service accepted a submission of nothing");
        assertThrows(NullPointerException.class,
            () -> forwarding.submit((Callable<Object>) null),
            "a forwarding service accepted a call of nothing");
        assertThrows(NullPointerException.class,
            () -> scheduled.schedule((Runnable) null, 1, TimeUnit.MILLISECONDS),
            "a forwarding scheduled service accepted a schedule of nothing");
        assertThrows(NullPointerException.class,
            () -> scheduledPool.schedule((Callable<Object>) null, 1,
                TimeUnit.MILLISECONDS),
            "a Hadoop scheduled pool accepted a schedule of nothing");
        return null;
      }
    });

    // Nothing was queued by any of the refusals above, so the pool is still
    // able to run a task, which is what proves the refusals were clean.
    Observation observed = observeSubmission(alice, pool);
    assertObserved(alice, observed.subject(),
        "a pool that had refused a submission of nothing");
  }

  /**
   * A task that reaches a second Hadoop executor already carrying an identity is
   * handed on with the one it has, so a single reading still recovers it.
   * <p>
   * The composition is the one the single-thread factories can be given: a
   * forwarding service over a Hadoop pool, where the service reads the identity
   * and the pool is then handed the result. A second layer here would go
   * unnoticed by every assertion about identity, since both layers would
   * establish the same one; what it would break is the recovery of the submitted
   * task, which takes exactly one layer off. So the assertion is made on what an
   * operator reads: the debug line the pool writes must still name the task that
   * was submitted, which it can only do if one layer was added and not two.
   */
  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testATaskAlreadyCarryingAnIdentityIsNotGivenASecond()
      throws Exception {
    Subject alice = newSubject(ALICE);
    HadoopThreadPoolExecutor inner = register(singleThreadPool("twice-inner"));
    SubjectPreservingExecutorService outer =
        register(new SubjectPreservingExecutorService(inner));
    Logger poolLog = LoggerFactory.getLogger(HadoopThreadPoolExecutor.class);
    GenericTestUtils.setLogLevel(poolLog, Level.DEBUG);
    GenericTestUtils.LogCapturer poolLines =
        GenericTestUtils.LogCapturer.captureLogs(poolLog);
    try {
      Recorder recorder = new Recorder();
      as(alice, new PrivilegedAction<Void>() {
        @Override
        public Void run() {
          outer.execute(recorder);
          return null;
        }
      });
      Observation observed = recorder.awaitOne();
      assertObserved(alice, observed.subject(),
          "a forwarding service over a Hadoop pool");

      // The line is written before the task runs, so the observation above is
      // already proof it was written; the pool is let catch up all the same, so
      // that the reporting hook has returned before its output is read.
      awaitQuiescence(inner);

      assertTrue(poolLines.getOutput().contains(Recorder.class.getName()),
          "the before-execute line did not name the submitted task's class: "
              + poolLines.getOutput());
      assertFalse(poolLines.getOutput().contains("SubjectPreservingTasks"),
          "the before-execute line named a class used to carry an identity, so "
              + "the task was given a second one and a single reading no longer "
              + "recovers it: " + poolLines.getOutput());
    } finally {
      poolLines.stopCapturing();
      GenericTestUtils.setLogLevel(poolLog, Level.INFO);
    }
  }

  // The user the work is authorized and audited as. An observed subject is
  // necessary but not sufficient, because a task that lost its submitter's
  // identity outright still reports a perfectly valid user: the one the process
  // logged in as. Every reading below is therefore made through
  // UserGroupInformation as well, against a login user no task here is ever
  // submitted by, so the fall back shows up as a failure.

  private static UserGroupInformation newUser(String name) {
    return UserGroupInformation.createRemoteUser(name);
  }

  private Observation observeUserSubmission(UserGroupInformation user,
      ExecutorService pool) throws Exception {
    Future<Observation> submitted =
        user.doAs(new PrivilegedAction<Future<Observation>>() {
          @Override
          public Future<Observation> run() {
            return pool.submit(currentObservation());
          }
        });
    return submitted.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testSubmittedTaskIsAuthorizedAsItsSubmitter() throws Exception {
    UserGroupInformation alice = newUser("alice-submitter");
    ExecutorService pool = register(singleThreadPool("ugi-submit"));
    Observation observed = observeUserSubmission(alice, pool);
    assertNotNull(observed.subject(),
        "a task submitted by a logged in user observed no identity at all");
    assertObservedUser(alice, observed.user(),
        "HadoopThreadPoolExecutor.submit(Callable)");
  }

  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testExecutedTaskIsAuthorizedAsItsSubmitter() throws Exception {
    UserGroupInformation alice = newUser("alice-executor");
    ExecutorService pool = register(singleThreadPool("ugi-execute"));
    Recorder recorder = new Recorder();
    alice.doAs(new PrivilegedAction<Void>() {
      @Override
      public Void run() {
        pool.execute(recorder);
        return null;
      }
    });
    Observation observed = recorder.awaitOne();
    assertNotNull(observed.subject(),
        "a task executed for a logged in user observed no identity at all");
    assertObservedUser(alice, observed.user(),
        "HadoopThreadPoolExecutor.execute(Runnable)");
  }

  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testScheduledTaskIsAuthorizedAsItsScheduler() throws Exception {
    UserGroupInformation alice = newUser("alice-scheduler");
    ScheduledExecutorService pool = register(singleThreadScheduledPool());
    ScheduledFuture<Observation> scheduled =
        alice.doAs(new PrivilegedAction<ScheduledFuture<Observation>>() {
          @Override
          public ScheduledFuture<Observation> run() {
            return pool.schedule(currentObservation(), 0,
                TimeUnit.MILLISECONDS);
          }
        });
    Observation observed =
        scheduled.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    assertNotNull(observed.subject(),
        "a task scheduled by a logged in user observed no identity at all");
    assertObservedUser(alice, observed.user(),
        "HadoopScheduledThreadPoolExecutor.schedule(Callable, long, TimeUnit)");
  }

  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testForwardedTaskIsAuthorizedAsItsSubmitter() throws Exception {
    UserGroupInformation alice = newUser("alice-forwarded");
    ExecutorService pool = register(forwardingService());
    Observation observed = observeUserSubmission(alice, pool);
    assertNotNull(observed.subject(),
        "a task forwarded for a logged in user observed no identity at all");
    assertObservedUser(alice, observed.user(),
        "SubjectPreservingExecutorService.submit(Callable)");
  }

  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testBlockingPoolTaskIsAuthorizedAsItsSubmitter() throws Exception {
    UserGroupInformation alice = newUser("alice-blocking");
    ExecutorService pool =
        register(BlockingThreadPoolExecutorService.newInstance(1, 4, 60,
            TimeUnit.SECONDS, "ugi-blocking"));
    Observation observed = observeUserSubmission(alice, pool);
    assertNotNull(observed.subject(),
        "a task submitted to the blocking pool by a logged in user observed no "
            + "identity at all");
    assertObservedUser(alice, observed.user(),
        "BlockingThreadPoolExecutorService.submit(Callable)");
  }

  /**
   * A second user's task on a worker the first user's task already used is
   * authorized as whichever of them the runtime makes observable, and never as
   * the process login.
   * <p>
   * This is the reading that matters most, because a worker holding the wrong
   * user is worse than a worker holding none: work is then authorized and
   * recorded against somebody who did not ask for it.
   */
  @Test
  @Timeout(TIMEOUT_SECONDS)
  public void testReusedWorkerAuthorizesAsOneOfItsTwoSubmitters()
      throws Exception {
    UserGroupInformation alice = newUser("alice-reuse");
    UserGroupInformation bob = newUser("bob-reuse");
    ExecutorService pool = register(singleThreadPool("ugi-reuse"));

    Observation byAlice = observeUserSubmission(alice, pool);
    assertObservedUser(alice, byAlice.user(), "the first submission");

    Observation byBob = observeUserSubmission(bob, pool);
    assertSame(byAlice.worker(), byBob.worker(),
        "the two submissions did not share a worker, so this assertion no "
            + "longer covers a reused worker at all");
    assertNotEquals(LOGIN_USER, byBob.user().getUserName(),
        "a task on a reused worker fell back to the process login, so it lost "
            + "its submitter's identity without failing");
    if (SubjectUtil.THREAD_INHERITS_SUBJECT) {
      assertEquals(alice, byBob.user(),
          "on a runtime that hands a new thread its creator's identity, a "
              + "reused worker is expected to keep the user it was created for");
    } else {
      assertEquals(bob, byBob.user(),
          "a task run by a worker an earlier user had already used was not "
              + "authorized as its own submitter");
      assertNotEquals(alice, byBob.user(),
          "a worker kept across submissions ran one user's task under an "
              + "earlier user's identity");
    }
  }


}
