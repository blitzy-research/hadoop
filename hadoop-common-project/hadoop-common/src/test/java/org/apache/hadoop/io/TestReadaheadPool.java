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

package org.apache.hadoop.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;

import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.security.authentication.util.SubjectUtil;
import org.apache.hadoop.util.concurrent.HadoopThreadPoolExecutor;
import org.apache.hadoop.util.concurrent.SubjectPreservingTasks;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Asserts what {@link ReadaheadPool} does with a request its pool turns away,
 * and that its singleton can be reset.
 * <p>
 * A readahead request is submitted by whichever thread is reading a file, so
 * it runs under the identity of the reader that asked for it, as any task
 * handed to one of Hadoop's pools does. This pool is the one place where that
 * identity has a second boundary to cross. Rather than let a full queue throw,
 * it drops the
 * oldest queued request and offers the rejected one back to the pool, and it
 * offers back the request as the reader submitted it rather than the prepared
 * form it was handed. That is what keeps the pool reporting readahead requests
 * by their own type, and keeps a single layer of preparation around a request
 * however often it is rejected and retried, so that code which inspects a
 * queued task instead of running it still finds the request underneath.
 * <p>
 * The pool {@link ReadaheadPool} builds for itself has room for a thousand
 * queued requests and is handed out only where the native library is loaded, so
 * neither filling it nor reaching it is something a test can do. The handler is
 * therefore installed here in a pool with a single worker and room for a single
 * queued request, which is the smallest arrangement in which a rejection is
 * certain: the worker is held by a task that will not finish until the test
 * lets it, one request is queued, and the next is turned away. Nothing waits on
 * a timer, and nothing is asserted about which request is dropped beyond its
 * being the queued one, since that is the policy this handler extends rather
 * than anything it decides.
 * <p>
 * The retry is asserted twice over, because the two things worth asserting are
 * not both visible from the same place. Going through the pool shows what a
 * reader would see: the request is retried rather than lost, it runs, and it
 * runs as the reader. Handing the handler a prepared request directly, which is
 * what the pool does when it turns one away, shows what the handler itself
 * offers back, and is the only way to tell the request apart from the prepared
 * form of it. Both are done from inside the reader's scope, as the pool does:
 * a rejection is handled on the thread that submitted the request, which is why
 * preparing it a second time there carries the same identity as the first.
 * <p>
 * Every wait is bounded, every observation crosses a thread boundary through a
 * latch, and the pool a test creates is shut down and confirmed terminated
 * afterwards.
 */
public class TestReadaheadPool {

  /** Bound on every wait, generous enough to survive a loaded build host. */
  private static final long TIMEOUT_SECONDS = 10;

  /** The pool a test built to install the handler in, if it built one. */
  private HadoopThreadPoolExecutor pool;

  /**
   * Shuts down the pool a test built and forgets any readahead singleton.
   * <p>
   * The singleton is reset whether or not a test reached it, so that a pool
   * created by one test is never handed to the next, and no thread of one
   * outlives the test that caused it.
   *
   * @throws InterruptedException if this thread is interrupted while waiting
   */
  @AfterEach
  public void shutDownPoolAndResetSingleton() throws InterruptedException {
    HadoopThreadPoolExecutor created = pool;
    pool = null;
    try {
      if (created != null) {
        created.shutdownNow();
        assertTrue(created.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS),
            "the pool this test created did not terminate");
      }
    } finally {
      ReadaheadPool.resetInstance();
    }
  }

  /**
   * Returns a subject that no other subject can be mistaken for.
   * <p>
   * A {@link Subject} with nothing in it is equal to any other such subject, so
   * each identity is given a principal of its own to be told apart by. The
   * principal name is given in full so that building one resolves no default
   * realm and needs no Kerberos configuration.
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
   * Builds the pool these tests submit to, with the handler under test in it.
   * <p>
   * One worker and room for one queued request is what makes a rejection
   * certain. The handler is the one {@link ReadaheadPool} installs in the pool
   * it builds for itself -- {@link ThreadPoolExecutor.DiscardOldestPolicy},
   * which drops the request that has waited longest and offers the pool the
   * turned-away one in its place -- so what is asserted below is that handler's
   * own behaviour rather than a copy of it.
   *
   * @return the pool, also kept so that it is shut down when the test ends
   */
  private HadoopThreadPoolExecutor newPoolWithTheReadaheadHandler() {
    pool = new HadoopThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<Runnable>(1),
        new DaemonThreadFactory("readahead-rejection"));
    pool.setRejectedExecutionHandler(
        new ThreadPoolExecutor.DiscardOldestPolicy());
    return pool;
  }

  /**
   * Occupies the pool's only worker until the returned latch is counted down.
   * <p>
   * The task is confirmed to be running before this returns, so that everything
   * submitted afterwards is queued rather than run, which is what makes the
   * rejection that follows certain rather than likely.
   *
   * @param executor the pool whose worker to occupy
   * @return the latch that lets the worker go
   * @throws InterruptedException if this thread is interrupted while waiting
   */
  private CountDownLatch holdTheOnlyWorker(HadoopThreadPoolExecutor executor)
      throws InterruptedException {
    final CountDownLatch started = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    executor.execute(new Runnable() {
      @Override
      public void run() {
        started.countDown();
        try {
          assertTrue(release.await(TIMEOUT_SECONDS, TimeUnit.SECONDS),
              "the task holding the only worker was never let go");
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    });
    assertTrue(started.await(TIMEOUT_SECONDS, TimeUnit.SECONDS),
        "the pool never started the task that holds its only worker");
    return release;
  }

  /**
   * Returns the one task {@code executor} has queued.
   *
   * @param executor the pool to look at
   * @return the queued task, in the form the pool holds it
   */
  private static Runnable theOnlyQueuedTask(
      HadoopThreadPoolExecutor executor) {
    List<Runnable> queued = new ArrayList<>(executor.getQueue());
    assertEquals(1, queued.size(),
        "the pool has not queued exactly the one task it was left with");
    return queued.get(0);
  }

  /**
   * Shuts {@code executor} down and waits for the work it accepted to finish.
   *
   * @param executor the pool to shut down
   * @throws InterruptedException if this thread is interrupted while waiting
   */
  private static void awaitCompletion(HadoopThreadPoolExecutor executor)
      throws InterruptedException {
    executor.shutdown();
    assertTrue(executor.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS),
        "the pool did not finish the work it had accepted");
  }

  /**
   * A request the pool turns away is retried rather than lost, runs under the
   * identity of the reader that submitted it, and reaches the queue as the
   * request that was submitted.
   * <p>
   * This is the path a reader takes: a full pool, a request turned away, and a
   * retry made from inside the rejection handler on the reader's own thread.
   * The identity is asserted here rather than on a submission accepted first
   * time because the retry is a second preparation of the same request, at a
   * boundary that no other test crosses. The queued request is compared by
   * reference to
   * the one submitted, which is what shows a single layer of preparation: a
   * request prepared twice would yield the inner preparation when one layer was
   * taken off, not the request. And the request dropped to make room has to
   * stay dropped, since discarding it is what the pool does instead of
   * blocking, and a discarded request that ran anyway would be a readahead the
   * reader had been told was abandoned.
   *
   * @throws Exception if a task fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testRejectedRequestIsRetriedAsSubmittedUnderItsSubmitter()
      throws Exception {
    final Subject reader = newSubject("readahead-reader@EXAMPLE.COM");
    final HadoopThreadPoolExecutor executor = newPoolWithTheReadaheadHandler();
    CountDownLatch release = holdTheOnlyWorker(executor);

    RecordingRequest displaced = new RecordingRequest("displaced");
    final RecordingRequest rejected = new RecordingRequest("rejected");
    executor.execute(displaced);
    assertEquals(1, executor.getQueue().size(),
        "the request meant to be displaced was not queued");

    SubjectUtil.callAs(reader, new Callable<Void>() {
      public Void call() {
        executor.execute(rejected);
        return (Void) null;
      }
    });

    assertSame(rejected,
        SubjectPreservingTasks.unwrap(theOnlyQueuedTask(executor)),
        "the request queued after the rejection was not the request as it was "
            + "submitted");

    release.countDown();
    assertTrue(rejected.awaitRun(),
        "the request the pool turned away was never retried");
    assertSame(reader, rejected.observedSubject(),
        "the retried request did not run under the identity of the reader "
            + "that submitted it");

    awaitCompletion(executor);
    assertEquals(0, displaced.runs(),
        "a request dropped from the queue to make room ran anyway");
  }

  /**
   * A request the handler offers the pool a second time ends up queued with
   * exactly one layer of preparation, carrying the identity it was submitted
   * with.
   * <p>
   * The handler is given an already prepared request here, exactly as the pool
   * gives it one, because that is the case the retry actually presents and the
   * one that could go wrong unnoticed: offering the pool a prepared request
   * again reaches the same point that prepares every submission, so a second
   * layer would be added there and nothing would say so. Taking one layer off
   * would then yield the inner preparation rather than the request, which is
   * what the assertions below rule out. A second layer would also be a second
   * reading of the identity, taken on the thread that happened to make the
   * retry rather than on the thread that submitted the request, so the single
   * layer is what makes the retry run as the reader that asked for the
   * readahead.
   *
   * @throws Exception if a task fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testARetriedRequestKeepsExactlyOneLayerOfPreparation()
      throws Exception {
    final Subject reader = newSubject("readahead-retry@EXAMPLE.COM");
    final HadoopThreadPoolExecutor executor = newPoolWithTheReadaheadHandler();
    final ThreadPoolExecutor.DiscardOldestPolicy handler =
        new ThreadPoolExecutor.DiscardOldestPolicy();
    CountDownLatch release = holdTheOnlyWorker(executor);

    RecordingRequest displaced = new RecordingRequest("displaced");
    final RecordingRequest request = new RecordingRequest("retried");
    executor.execute(displaced);
    assertEquals(1, executor.getQueue().size(),
        "the request meant to be displaced was not queued");

    final AtomicReference<Runnable> prepared = new AtomicReference<>();
    SubjectUtil.callAs(reader, new Callable<Void>() {
      public Void call() {
        prepared.set(SubjectPreservingTasks.wrap(request));
        handler.rejectedExecution(prepared.get(), executor);
        return (Void) null;
      }
    });

    Runnable queued = theOnlyQueuedTask(executor);
    assertSame(prepared.get(), queued,
        "the request offered again was prepared a second time instead of "
            + "being queued as it already was");
    assertSame(request, SubjectPreservingTasks.unwrap(queued),
        "the request the handler offered back was not the request that was "
            + "submitted");

    release.countDown();
    assertTrue(request.awaitRun(),
        "the request the handler offered back never ran");
    assertSame(reader, request.observedSubject(),
        "the request the handler offered back did not run under the identity "
            + "it was submitted with");

    awaitCompletion(executor);
    assertEquals(0, displaced.runs(),
        "a request dropped from the queue to make room ran anyway");
  }

  /**
   * The readahead pool is a singleton that resetting forgets, and it exists
   * exactly when the native library it needs is loaded.
   * <p>
   * Both halves are asserted without asking which of the two situations the
   * build is in. Where the library is absent there is no pool to hand out, so
   * the singleton is nothing at all, twice over and after a reset as well;
   * where it is present the same pool is handed out each time. Resetting is
   * asserted
   * to bear repeating because it is what these tests, and everything else that
   * borrows the singleton, rely on to leave nothing behind.
   */
  @Test
  @Timeout(value = 30)
  public void testGetInstanceIsASingletonThatResetForgets() {
    ReadaheadPool.resetInstance();

    ReadaheadPool instance = ReadaheadPool.getInstance();
    assertEquals(NativeIO.isAvailable(), instance != null,
        "whether a readahead pool was handed out did not follow whether the "
            + "native library it needs is loaded");
    assertSame(instance, ReadaheadPool.getInstance(),
        "asking for the readahead pool twice produced two different pools");

    ReadaheadPool.resetInstance();
    ReadaheadPool.resetInstance();

    assertSame(ReadaheadPool.getInstance(), ReadaheadPool.getInstance(),
        "asking for the readahead pool twice after a reset produced two "
            + "different pools");
  }

  /**
   * A readahead request that reports the identity it ran under.
   * <p>
   * It stands in for a real readahead request, which cannot be built here: the
   * requests {@link ReadaheadPool} submits need a file descriptor and reach the
   * native library, and what is asserted above is what the pool does with a
   * request rather than what a request does.
   */
  private static final class RecordingRequest implements Runnable {

    private final String name;
    private final AtomicReference<Subject> observed = new AtomicReference<>();
    private final AtomicInteger runs = new AtomicInteger();
    private final CountDownLatch ran = new CountDownLatch(1);

    RecordingRequest(String requestName) {
      this.name = requestName;
    }

    @Override
    public void run() {
      observed.set(SubjectUtil.current());
      runs.incrementAndGet();
      ran.countDown();
    }

    /**
     * Waits for this request to run.
     *
     * @return whether it ran before the wait ran out
     * @throws InterruptedException if this thread is interrupted while waiting
     */
    boolean awaitRun() throws InterruptedException {
      return ran.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * Returns the identity in force while this request ran.
     *
     * @return the identity the run observed, which may be {@code null}
     */
    Subject observedSubject() {
      return observed.get();
    }

    /**
     * Returns how often this request has run.
     *
     * @return the number of runs that have completed
     */
    int runs() {
      return runs.get();
    }

    /**
     * Returns the name this request was given.
     *
     * @return the name, which is what a rejection would report
     */
    @Override
    public String toString() {
      return "RecordingRequest[" + name + "]";
    }
  }

  /**
   * Makes the named daemon threads this test's pool runs its work on.
   * <p>
   * The threads are of the plainest kind, so nothing but a request itself can
   * bring an identity to the worker, and they are daemons so that a worker left
   * behind by a failing test cannot hold the build open.
   */
  private static final class DaemonThreadFactory implements ThreadFactory {

    private final String prefix;
    private final AtomicInteger created = new AtomicInteger(1);

    DaemonThreadFactory(String threadNamePrefix) {
      this.prefix = threadNamePrefix;
    }

    @Override
    public Thread newThread(Runnable r) {
      Thread thread = new Thread(r, prefix + "-" + created.getAndIncrement());
      thread.setDaemon(true);
      return thread;
    }
  }
}
