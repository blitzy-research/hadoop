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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;

import org.apache.hadoop.security.authentication.util.SubjectUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * Asserts the contract of {@link SubjectPreservingTasks} and the operational
 * output that depends on it.
 * <p>
 * Carrying a submitter's identity into a pool worker means handing the pool
 * something other than the task the submitter passed in, and the code that
 * looks at a task instead of running it has to go on seeing the task as it was
 * submitted. Three such places exist: the line each of Hadoop's two pools
 * writes before running a task, the line
 * {@link ExecutorHelper} writes after one, and the check by which that helper
 * finds the failure of a task the pool itself was told nothing about. The last
 * of those reports every exception thrown by a task submitted for its result,
 * so losing it would leave such failures unreported with nothing failing to
 * say so.
 * <p>
 * Four ways of handing a task on unchanged are asserted here as well, each for
 * its own reason. A task that is absent has to stay absent, so that the
 * executor it reaches rejects it exactly as it always did. A task submitted
 * with no identity to carry has to be handed on untouched, because
 * establishing an absent identity would hide an identity established around
 * it. A task already prepared once has to be handed on as it is, so that an
 * executor offering a task back after rejecting it ends up with one layer
 * rather than two, since only one is ever taken back off. And on a runtime
 * that hands a new thread the identity of whoever started it, nothing is
 * added at all, which is what makes this change cost such a runtime nothing.
 * <p>
 * The assertions are about outcomes rather than about mechanism, so they hold
 * on every runtime this project supports. Where a wrapper exists at all, one
 * assertion checks that it is the class these tests name when they insist a
 * log line does not mention it, so renaming it makes that guard fail loudly
 * rather than making the log assertions quietly vacuous. Each test uses
 * executors created for it, every executor is shut down afterwards, and every
 * logger this raises to debug is put back as it was.
 */
public class TestSubjectPreservingTasks {

  /** Bound on every wait, generous enough to survive a loaded build host. */
  private static final long TIMEOUT_SECONDS = 10;

  /** How often to look again for a line written by a pool worker. */
  private static final long POLL_MILLIS = 20;

  /** The text each pool writes before naming the type of a task. */
  private static final String RUNNABLE_TYPE = ", runnable type: ";

  /**
   * Simple name of the wrapper that carries an identity into a worker.
   * <p>
   * A log line naming this instead of the submitted task would be a change to
   * operational output. The name is asserted against the wrapper itself in
   * {@link #assertPreparedForAWorker(Runnable, Runnable)}, so it
   * cannot fall out of step with the class it names. Note that it is the whole
   * name rather than a prefix of it: this test class and its own tasks share
   * that prefix.
   */
  private static final String WRAPPER_NAME = "SubjectPreservingRunnable";

  /** Where {@link HadoopThreadPoolExecutor} writes its task lines. */
  private static final Logger POOL_LOG =
      LoggerFactory.getLogger(HadoopThreadPoolExecutor.class);

  /** Where {@link HadoopScheduledThreadPoolExecutor} writes its task lines. */
  private static final Logger SCHEDULED_LOG =
      LoggerFactory.getLogger(HadoopScheduledThreadPoolExecutor.class);

  /** Where {@link ExecutorHelper} reports a task that failed. */
  private static final Logger HELPER_LOG =
      LoggerFactory.getLogger(ExecutorHelper.class);

  /** Every executor created by a test, shut down when the test ends. */
  private final List<ExecutorService> pools = new ArrayList<>();

  /** Every capture a test started, stopped when the test ends. */
  private final List<LogCapturer> capturers = new ArrayList<>();

  /** Every logger a test raised to debug, put back when the test ends. */
  private final List<Logger> raised = new ArrayList<>();

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
   * Starts capturing what {@code logger} writes, at debug.
   * <p>
   * The level is raised because the lines that name a task are written only at
   * debug, and it is put back when the test ends so that a later test in the
   * same JVM sees the level it would have seen.
   *
   * @param logger the logger to capture
   * @return the capture, to be read once the lines have been written
   */
  private LogCapturer captureAtDebug(Logger logger) {
    GenericTestUtils.setLogLevel(logger, Level.DEBUG);
    raised.add(logger);
    LogCapturer capturer = LogCapturer.captureLogs(logger);
    capturers.add(capturer);
    return capturer;
  }

  /**
   * Shuts down every executor a test created and restores every logger it
   * raised.
   *
   * @throws InterruptedException if this thread is interrupted while waiting
   */
  @AfterEach
  public void releaseWhatTheTestTook() throws InterruptedException {
    for (LogCapturer capturer : capturers) {
      capturer.stopCapturing();
    }
    capturers.clear();
    for (Logger logger : raised) {
      GenericTestUtils.setLogLevel(logger, Level.INFO);
    }
    raised.clear();
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
   * Returns a task that reports the subject of the thread that calls it.
   *
   * @return a task yielding the calling thread's subject, which may be
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
   * Returns the type of task named by the last line that named one.
   *
   * @param output everything the captured logger has written
   * @return the reported class name
   */
  private static String loggedRunnableType(String output) {
    int marker = output.lastIndexOf(RUNNABLE_TYPE);
    assertTrue(marker >= 0,
        "no line naming the type of a task was written, only: " + output);
    int from = marker + RUNNABLE_TYPE.length();
    int end = output.indexOf('\n', from);
    String named =
        end < 0 ? output.substring(from) : output.substring(from, end);
    return named.trim();
  }

  /**
   * Asserts that a task submitted with an identity to carry was handed on in a
   * form that carries it.
   * <p>
   * The identity is read at every submission, on every runtime, because what a
   * worker was given when it was created is the identity of whoever first
   * caused it to exist and not of whoever submitted the task it is about to
   * run. Something other than the submitted task is therefore always handed
   * on, and its name is the name the log assertions in this class insist a line
   * does not mention.
   *
   * @param submitted the task as submitted
   * @param handedOn what the executor was given in its place
   */
  private static void assertPreparedForAWorker(Runnable submitted,
      Runnable handedOn) {
    assertNotSame(submitted, handedOn,
        "a task submitted with an identity to carry was handed on with "
            + "nothing to carry it");
    assertEquals(WRAPPER_NAME, handedOn.getClass().getSimpleName(),
        "what carries an identity into a worker is no longer named as the "
            + "log assertions in this class expect");
  }

  /**
   * A task that is not there is handed on as it is, and every executor that
   * could be given one still rejects it.
   * <p>
   * The check is made from inside a scope that has an identity to carry, so
   * that the absence of the task rather than the absence of an identity is what
   * the executor is left to deal with. Rejecting a missing task is the
   * behaviour of the executors these tests submit to before anything in this
   * project touched them, and a caller relying on it must go on being able to.
   *
   * @throws Exception if a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testAbsentTaskIsHandedOnUnchanged() throws Exception {
    Subject submitter = newSubject("absent-task@EXAMPLE.COM");
    final ExecutorService extendedPool =
        register(HadoopExecutors.newFixedThreadPool(1,
            new DaemonThreadFactory("absent-extended")));
    final ExecutorService forwardingPool =
        register(HadoopExecutors.newSingleThreadExecutor(
            new DaemonThreadFactory("absent-forwarding")));
    final ScheduledExecutorService forwardingScheduledPool =
        register(HadoopExecutors.newSingleThreadScheduledExecutor(
            new DaemonThreadFactory("absent-scheduled")));

    SubjectUtil.callAs(submitter, new Callable<Void>() {
      @Override
      public Void call() {
        assertNull(SubjectPreservingTasks.wrap((Runnable) null),
            "a runnable that was not there was replaced by one that was");
        assertNull(SubjectPreservingTasks.wrap((Callable<Subject>) null),
            "a callable that was not there was replaced by one that was");
        assertNull(SubjectPreservingTasks.unwrap(null),
            "unwrapping nothing yielded something");
        assertThrows(NullPointerException.class,
            () -> extendedPool.execute(null),
            "the pool Hadoop extends accepted no task at all");
        assertThrows(NullPointerException.class,
            () -> extendedPool.submit((Runnable) null),
            "the pool Hadoop extends accepted no task at all to submit");
        assertThrows(NullPointerException.class,
            () -> forwardingPool.execute(null),
            "the executor Hadoop forwards to accepted no task at all");
        assertThrows(NullPointerException.class,
            () -> forwardingPool.submit((Callable<Subject>) null),
            "the executor Hadoop forwards to accepted nothing to submit");
        assertThrows(NullPointerException.class,
            () -> forwardingScheduledPool.schedule((Runnable) null, 0,
                TimeUnit.MILLISECONDS),
            "the scheduled executor Hadoop forwards to accepted no task");
        return (Void) null;
      }
    });
  }

  /**
   * A task already prepared once is left with one layer when it is prepared
   * again.
   * <p>
   * An executor whose rejection policy offers a task back hands the very same
   * task in a second time, and only one layer is ever taken back off again. A
   * second layer would therefore leave the code that looks at a task seeing
   * what carries the identity rather than the task itself, which is why the
   * task has to come back out as submitted after a single unwrapping.
   *
   * @throws Exception if a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testTaskPreparedTwiceIsLeftWithOneLayer() throws Exception {
    Subject submitter = newSubject("prepared-twice@EXAMPLE.COM");
    final Runnable runnable = new RecordingTask();
    final Callable<Subject> callable = currentSubject();

    SubjectUtil.callAs(submitter, new Callable<Void>() {
      @Override
      public Void call() {
        Runnable once = SubjectPreservingTasks.wrap(runnable);
        Runnable twice = SubjectPreservingTasks.wrap(once);
        assertSame(once, twice,
            "a task already prepared once was prepared a second time");
        assertSame(runnable, SubjectPreservingTasks.unwrap(twice),
            "a task prepared twice did not come back out as it was submitted");
        Callable<Subject> calledOnce = SubjectPreservingTasks.wrap(callable);
        Callable<Subject> calledTwice = SubjectPreservingTasks.wrap(calledOnce);
        assertSame(calledOnce, calledTwice,
            "a callable already prepared once was prepared a second time");
        return (Void) null;
      }
    });
  }

  /**
   * Unwrapping yields the task as it was submitted, whatever it is handed.
   * <p>
   * A task that carries an identity comes back out as the task the submitter
   * passed in; a task that carries none is returned as it is, since it is
   * already that task; and nothing is returned for nothing. The first of those
   * is what lets a pool report on a task and hand a future back describing it,
   * and the second is what keeps a task that needed nothing done to it out of
   * a special case of its own.
   *
   * @throws Exception if a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testUnwrapYieldsTheTaskAsSubmitted() throws Exception {
    Subject submitter = newSubject("unwrap@EXAMPLE.COM");
    final Runnable submitted = new RecordingTask();
    final Runnable neverPrepared = new RecordingTask();

    SubjectUtil.callAs(submitter, new Callable<Void>() {
      @Override
      public Void call() {
        Runnable handedOn = SubjectPreservingTasks.wrap(submitted);
        assertPreparedForAWorker(submitted, handedOn);
        assertSame(submitted, SubjectPreservingTasks.unwrap(handedOn),
            "the task an executor was given did not come back out as the task "
                + "that was submitted");
        assertSame(neverPrepared,
            SubjectPreservingTasks.unwrap(neverPrepared),
            "a task carrying no identity was not returned as it is");
        assertNull(SubjectPreservingTasks.unwrap(null),
            "unwrapping nothing yielded something");
        return (Void) null;
      }
    });
  }

  /**
   * The pool Hadoop extends names the task as it was submitted in the line it
   * writes before running one.
   * <p>
   * That line is operational output that an operator reads to see what a pool
   * is running, so it has to go on naming the task the submitter passed in
   * rather than what carries the submitter's identity into the worker. The
   * assertion is made twice: once through the pool itself, where a submission
   * made under an identity really does reach the hook carrying it, and once by
   * handing the hook a prepared task directly, so that the same statement is
   * asserted on a runtime that prepares nothing.
   *
   * @throws Exception if a task fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testPoolNamesTheTaskAsSubmittedBeforeRunningIt()
      throws Exception {
    Subject submitter = newSubject("named-before@EXAMPLE.COM");
    LogCapturer captured = captureAtDebug(POOL_LOG);
    final RecordingTask task = new RecordingTask();
    final HadoopThreadPoolExecutor pool =
        register(new HadoopThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(),
            new DaemonThreadFactory("named-before")));

    SubjectUtil.callAs(submitter, new Callable<Void>() {
      @Override
      public Void call() {
        pool.execute(task);
        return (Void) null;
      }
    });
    assertTrue(task.awaitRun(), "the task never ran");

    String beforeItRan = captured.getOutput();
    assertEquals(RecordingTask.class.getName(),
        loggedRunnableType(beforeItRan),
        "the line written before a task ran did not name the task as it was "
            + "submitted");
    assertFalse(beforeItRan.contains(WRAPPER_NAME),
        "the line written before a task ran named what carries an identity "
            + "instead of the task as it was submitted: " + beforeItRan);
    assertSame(submitter, task.observed(),
        "the task did not run under the identity that submitted it");

    final AtomicReference<Runnable> prepared = new AtomicReference<>();
    SubjectUtil.callAs(submitter, new Callable<Void>() {
      @Override
      public Void call() {
        prepared.set(SubjectPreservingTasks.wrap(new RecordingTask()));
        return (Void) null;
      }
    });
    captured.clearOutput();
    pool.beforeExecute(Thread.currentThread(), prepared.get());
    assertEquals(RecordingTask.class.getName(),
        loggedRunnableType(captured.getOutput()),
        "handed a prepared task, the hook did not name the task as it was "
            + "submitted");
  }

  /**
   * The scheduled pool Hadoop extends names the same type of task before
   * running one whether or not there was an identity to carry.
   * <p>
   * The runtime puts a task of its own between a schedule and the worker that
   * runs it, so the line this pool writes names that task rather than either
   * the schedule or what carries an identity. Comparing the line written for a
   * schedule made with an identity against the line written for one made
   * without says that carrying an identity left the line alone, without this
   * test having to name a class of the runtime's own that it does not own. The
   * hook is then handed a prepared task directly, which asserts what it does
   * with one however a task reaches it.
   *
   * @throws Exception if a task fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testScheduledPoolNamesTheTaskAsSubmittedBeforeRunningIt()
      throws Exception {
    Subject scheduler = newSubject("named-before-scheduled@EXAMPLE.COM");
    LogCapturer captured = captureAtDebug(SCHEDULED_LOG);
    final HadoopScheduledThreadPoolExecutor withoutAnIdentity =
        register(new HadoopScheduledThreadPoolExecutor(1,
            new DaemonThreadFactory("scheduled-plain")));
    final HadoopScheduledThreadPoolExecutor withAnIdentity =
        register(new HadoopScheduledThreadPoolExecutor(1,
            new DaemonThreadFactory("scheduled-identified")));
    RecordingTask carryingNothing = new RecordingTask();
    final RecordingTask carryingOne = new RecordingTask();

    withoutAnIdentity.schedule(carryingNothing, 0, TimeUnit.MILLISECONDS);
    assertTrue(carryingNothing.awaitRun(),
        "the task scheduled with no identity to carry never ran");
    String namedWithoutOne = loggedRunnableType(captured.getOutput());
    captured.clearOutput();

    SubjectUtil.callAs(scheduler, new Callable<Void>() {
      @Override
      public Void call() {
        withAnIdentity.schedule(carryingOne, 0, TimeUnit.MILLISECONDS);
        return (Void) null;
      }
    });
    assertTrue(carryingOne.awaitRun(),
        "the task scheduled with an identity to carry never ran");
    String afterAnIdentity = captured.getOutput();

    assertEquals(namedWithoutOne, loggedRunnableType(afterAnIdentity),
        "carrying an identity changed the type of task this pool names before "
            + "running one");
    assertFalse(afterAnIdentity.contains(WRAPPER_NAME),
        "the line written before a scheduled task ran named what carries an "
            + "identity: " + afterAnIdentity);
    assertSame(scheduler, carryingOne.observed(),
        "the scheduled task did not run under the identity that scheduled it");
    assertNull(carryingNothing.observed(),
        "a task scheduled with no identity to carry observed one");

    final AtomicReference<Runnable> prepared = new AtomicReference<>();
    SubjectUtil.callAs(scheduler, new Callable<Void>() {
      @Override
      public Void call() {
        prepared.set(SubjectPreservingTasks.wrap(new RecordingTask()));
        return (Void) null;
      }
    });
    captured.clearOutput();
    withAnIdentity.beforeExecute(Thread.currentThread(), prepared.get());
    assertEquals(RecordingTask.class.getName(),
        loggedRunnableType(captured.getOutput()),
        "handed a prepared task, the hook did not name the task as it was "
            + "submitted");
  }

  /**
   * The failure of a task submitted for its result is still reported after the
   * task has run.
   * <p>
   * A pool is told nothing about such a failure, because the task the pool runs
   * keeps it for whoever asks for the result. Hadoop's pools therefore look at
   * the finished task themselves and report what it kept, which they can only do
   * while the task they are handed is still the one holding the result. Were the
   * identity-carrying layer left on, that check would find nothing to ask, every
   * such failure would go unreported, and nothing would fail to say so: the line
   * written after the task ran would also name the wrong type. Both are asserted
   * here, along with the failure reaching the caller unchanged.
   *
   * @throws Exception if a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testFailureOfATaskSubmittedForItsResultIsStillReported()
      throws Exception {
    Subject submitter = newSubject("reported-failure@EXAMPLE.COM");
    LogCapturer captured = captureAtDebug(HELPER_LOG);
    final IllegalStateException failure =
        new IllegalStateException("a task failure a pool has to report");
    final HadoopThreadPoolExecutor pool =
        register(new HadoopThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(),
            new DaemonThreadFactory("reported-failure")));
    final AtomicReference<Future<Subject>> submitted = new AtomicReference<>();

    SubjectUtil.callAs(submitter, new Callable<Void>() {
      @Override
      public Void call() {
        submitted.set(pool.submit(new Callable<Subject>() {
          @Override
          public Subject call() {
            throw failure;
          }
        }));
        return (Void) null;
      }
    });

    ExecutionException reachedTheCaller =
        assertThrows(ExecutionException.class,
            () -> submitted.get().get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
            "a task that threw did not fail its future");
    assertSame(failure, reachedTheCaller.getCause(),
        "the failure did not reach the caller as the exception the task threw");

    GenericTestUtils.waitFor(
        () -> captured.getOutput().contains(failure.getMessage()),
        POLL_MILLIS, TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS),
        "the failure of a task submitted for its result was never reported");

    String reported = captured.getOutput();
    assertTrue(reported.contains("Caught exception in thread"),
        "the failure was not reported as one caught in a pool thread: "
            + reported);
    assertEquals(FutureTask.class.getName(), loggedRunnableType(reported),
        "the line written after a task ran did not name the task the pool was "
            + "given");
  }

  /**
   * A task an executor offers itself again after rejecting it is prepared only
   * once.
   * <p>
   * A pool with room for one task at a time, and a policy of making room by
   * dropping the oldest waiting task, hands a rejected task straight back in
   * once it has done so. The task arriving the second time is the one already
   * prepared, and preparing it again would leave two layers where only one is
   * ever taken back off, so the line written before it ran would name what
   * carries the identity rather than the task. That the discarded task never
   * ran is asserted too, since it is what proves the rejection really happened
   * and the task really was offered again.
   *
   * @throws Exception if a task fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testTaskOfferedAgainAfterRejectionIsPreparedOnlyOnce()
      throws Exception {
    Subject submitter = newSubject("offered-again@EXAMPLE.COM");
    LogCapturer captured = captureAtDebug(POOL_LOG);
    final BlockingTask holdsTheWorker = new BlockingTask();
    final RecordingTask discarded = new RecordingTask();
    final RecordingTask offeredAgain = new RecordingTask();
    final HadoopThreadPoolExecutor pool =
        register(new HadoopThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<Runnable>(1),
            new DaemonThreadFactory("offered-again"),
            new ThreadPoolExecutor.DiscardOldestPolicy()));

    SubjectUtil.callAs(submitter, new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        pool.execute(holdsTheWorker);
        assertTrue(holdsTheWorker.awaitBusy(),
            "the only worker of the pool never took the task holding it");
        pool.execute(discarded);
        pool.execute(offeredAgain);
        return (Void) null;
      }
    });
    holdsTheWorker.release();

    assertTrue(offeredAgain.awaitRun(),
        "the task offered again after being rejected never ran");
    assertEquals(0, discarded.runs(),
        "the task the rejection policy discarded ran anyway, so nothing was "
            + "rejected and nothing was offered again");
    assertSame(submitter, offeredAgain.observed(),
        "the task offered again did not run under the identity that submitted "
            + "it");

    String output = captured.getOutput();
    assertFalse(output.contains(WRAPPER_NAME),
        "a task offered again was prepared a second time, so the line written "
            + "before it ran named what carries an identity: " + output);
    assertTrue(output.contains(RecordingTask.class.getName()),
        "no line named the task that was offered again: " + output);
  }

  /**
   * Records the identity in force where it runs, and how often it has run.
   */
  private static final class RecordingTask implements Runnable {

    /** The identity in force inside this task; {@code null} until it runs. */
    private final AtomicReference<Subject> seen = new AtomicReference<>();

    /** How often this task has run. */
    private final AtomicInteger completed = new AtomicInteger();

    /** Published to whoever is waiting for the first run. */
    private final CountDownLatch ran = new CountDownLatch(1);

    @Override
    public void run() {
      seen.set(SubjectUtil.current());
      completed.incrementAndGet();
      ran.countDown();
    }

    /**
     * Waits for this task to run once.
     *
     * @return whether it ran within the bound every wait here uses
     * @throws InterruptedException if this thread is interrupted while waiting
     */
    boolean awaitRun() throws InterruptedException {
      return ran.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * Returns the identity in force where this task ran.
     *
     * @return the observed subject, or {@code null} if there was none
     */
    Subject observed() {
      return seen.get();
    }

    /**
     * Returns how often this task has run.
     *
     * @return the number of completed runs
     */
    int runs() {
      return completed.get();
    }
  }

  /**
   * Occupies the only worker of a pool until it is let go.
   */
  private static final class BlockingTask implements Runnable {

    /** Published once a worker has taken this task. */
    private final CountDownLatch busy = new CountDownLatch(1);

    /** Awaited until the test lets the worker move on. */
    private final CountDownLatch released = new CountDownLatch(1);

    @Override
    public void run() {
      busy.countDown();
      try {
        released.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    /**
     * Waits until a worker has taken this task.
     *
     * @return whether a worker took it within the bound every wait here uses
     * @throws InterruptedException if this thread is interrupted while waiting
     */
    boolean awaitBusy() throws InterruptedException {
      return busy.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    /** Lets the worker running this task move on to the next one. */
    void release() {
      released.countDown();
    }
  }

  /**
   * Makes named daemon threads of the plainest kind available.
   * <p>
   * A thread of this kind is handed nothing when it is created on the runtimes
   * that carry nothing across a thread boundary, so a pool built on it can only
   * report an identity that arrived with the task itself.
   */
  private static final class DaemonThreadFactory implements ThreadFactory {

    private final String prefix;
    private final AtomicInteger created = new AtomicInteger(1);

    DaemonThreadFactory(String prefix) {
      this.prefix = prefix;
    }

    @Override
    public Thread newThread(Runnable r) {
      Thread thread = new Thread(r, prefix + "-" + created.getAndIncrement());
      thread.setDaemon(true);
      return thread;
    }
  }
}
