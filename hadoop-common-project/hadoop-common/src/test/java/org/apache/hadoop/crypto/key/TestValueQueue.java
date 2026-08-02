/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.crypto.key;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hadoop.crypto.key.kms.ValueQueue;
import org.apache.hadoop.crypto.key.kms.ValueQueue.QueueRefiller;
import org.apache.hadoop.crypto.key.kms.ValueQueue.SyncGenerationPolicy;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.SubjectUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.thirdparty.com.google.common.cache.LoadingCache;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


public class TestValueQueue {
  Logger LOG = LoggerFactory.getLogger(TestValueQueue.class);

  /** Bound on the wait for an asynchronous refill, in seconds. */
  private static final long REFILL_WAIT_SECONDS = 10;

  private static class FillInfo {
    final int num;
    final String key;
    FillInfo(int num, String key) {
      this.num = num;
      this.key = key;
    }
  }

  private static class MockFiller implements QueueRefiller<String> {
    final LinkedBlockingQueue<FillInfo> fillCalls =
        new LinkedBlockingQueue<FillInfo>();
    @Override
    public void fillQueueForKey(String keyName, Queue<String> keyQueue,
        int numValues) throws IOException {
      fillCalls.add(new FillInfo(numValues, keyName));
      for(int i = 0; i < numValues; i++) {
        keyQueue.add("test");
      }
    }
    public FillInfo getTop() throws InterruptedException {
      return fillCalls.poll(500, TimeUnit.MILLISECONDS);
    }
  }

  /** Bound on waiting for a fill that is being held back to be let go. */
  private static final long GATE_SECONDS = 10;

  /** Keys chosen so that no two of them share a stripe of the queue's locks. */
  private static final String HELD_KEY = "held";
  private static final String DRAINED_KEY = "drained";
  private static final String KEPT_KEY = "kept";

  /**
   * A fill a refiller was asked for, and the identity it was made under.
   */
  private static final class Fill {
    private final String key;
    private final int num;
    private final Subject subject;
    private final Thread thread;

    Fill(String keyName, int numValues, Subject observed,
        Thread observedOn) {
      this.key = keyName;
      this.num = numValues;
      this.subject = observed;
      this.thread = observedOn;
    }
  }

  /**
   * What one asynchronous refill observed about itself.
   */
  private static class RefillInfo {
    private final String key;
    private final Subject subject;
    private final Thread thread;
    RefillInfo(String key, Subject subject, Thread thread) {
      this.key = key;
      this.subject = subject;
      this.thread = thread;
    }
  }

  /**
   * A refiller that records the identity in force while it fills a queue.
   * <p>
   * It fills exactly as {@link MockFiller} does, so the queues behave as they
   * do in the tests above, and it notes for each fill the key, how many values
   * were asked for, the JAAS subject in force, and the thread the fill ran on.
   * A synchronous fill runs on the thread that built the refiller, which is
   * what tells the two kinds of fill apart without a test having to be told
   * which is which.
   * <p>
   * Fills made on any other thread can be held back, which is how a test
   * arranges for refill tasks to sit in the filler pool's queue rather than
   * run. The identity is recorded before a fill is held back, so a held fill
   * still reports what it saw.
   */
  private static final class RecordingFiller
      implements QueueRefiller<String> {

    /** The thread that built this refiller, which makes synchronous fills. */
    private final Thread owner = Thread.currentThread();

    /** Every fill this refiller has been asked for, in order. */
    private final List<Fill> fills =
        Collections.synchronizedList(new ArrayList<Fill>());

    /** Fills held back wait on this; a latch already at zero holds nothing. */
    private volatile CountDownLatch gate = new CountDownLatch(0);

    @Override
    public void fillQueueForKey(String keyName, Queue<String> keyQueue,
        int numValues) throws IOException {
      fills.add(new Fill(keyName, numValues, SubjectUtil.current(),
          Thread.currentThread()));
      if (Thread.currentThread() != owner) {
        try {
          if (!gate.await(GATE_SECONDS, TimeUnit.SECONDS)) {
            throw new IOException(
                "a fill of " + keyName + " was never let go");
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
      }
      for (int i = 0; i < numValues; i++) {
        keyQueue.add("test");
      }
    }

    /**
     * Holds back every fill made on a thread other than the test's own.
     *
     * @return the latch that lets the held fills go
     */
    CountDownLatch holdBackFillsOnOtherThreads() {
      CountDownLatch held = new CountDownLatch(1);
      gate = held;
      return held;
    }

    /** Forgets the fills recorded so far. */
    void forgetFills() {
      fills.clear();
    }

    /**
     * Returns the fills of a key made on a thread other than the test's own,
     * which are the refills the filler pool ran.
     *
     * @param keyName the key whose fills to return
     * @return those fills, in the order they were made
     */
    List<Fill> asyncFills(String keyName) {
      return fillsOf(keyName, false);
    }

    /**
     * Returns the fills of a key made on the test's own thread, which are the
     * fills a caller made for itself.
     *
     * @param keyName the key whose fills to return
     * @return those fills, in the order they were made
     */
    List<Fill> syncFills(String keyName) {
      return fillsOf(keyName, true);
    }

    private List<Fill> fillsOf(String keyName, boolean onTheOwnerThread) {
      List<Fill> found = new ArrayList<>();
      synchronized (fills) {
        for (Fill fill : fills) {
          boolean isOwn = fill.thread == owner;
          if (isOwn == onTheOwnerThread && keyName.equals(fill.key)) {
            found.add(fill);
          }
        }
      }
      return found;
    }
  }

  /**
   * A refiller that records the subject of every asynchronous refill.
   * <p>
   * Only the refills that run on a filler thread of the value queue are
   * recorded. A queue also fills synchronously, on the requesting thread
   * itself, and such a fill observes the requester's subject whatever the
   * pooled refills do, so recording it would let an assertion pass on the
   * strength of the wrong fill.
   */
  private static class RefillRecordingFiller implements QueueRefiller<String> {

    private static final String REFILL_THREAD =
        ValueQueue.class.getName() + "_thread";

    private final LinkedBlockingQueue<RefillInfo> refills =
        new LinkedBlockingQueue<>();

    @Override
    public void fillQueueForKey(String keyName, Queue<String> keyQueue,
        int numValues) throws IOException {
      Thread current = Thread.currentThread();
      boolean onFillerThread = current.getName().startsWith(REFILL_THREAD);
      Subject subject = SubjectUtil.current();
      for (int i = 0; i < numValues; i++) {
        keyQueue.add("test");
      }
      if (onFillerThread) {
        refills.add(new RefillInfo(keyName, subject, current));
      }
    }

    /**
     * Waits for the next asynchronous refill and returns what it observed.
     *
     * @return that refill's key, subject and thread
     * @throws InterruptedException if this thread is interrupted while waiting
     */
    RefillInfo awaitRefill() throws InterruptedException {
      RefillInfo seen = refills.poll(REFILL_WAIT_SECONDS, TimeUnit.SECONDS);
      assertNotNull(seen, "no asynchronous refill happened");
      return seen;
    }
  }

  /**
   * Asks {@code valueQueue} for a value under {@code subject}'s identity.
   *
   * @param subject the identity to ask under, which may be {@code null}
   * @param valueQueue the queue to ask
   * @param keyName the key to ask for
   * @return the value the queue returned
   */
  private static String requestAs(Subject subject,
      final ValueQueue<String> valueQueue, final String keyName) {
    return SubjectUtil.callAs(subject, new Callable<String>() {
      @Override
      public String call() throws Exception {
        return valueQueue.getNext(keyName);
      }
    });
  }

  /**
   * An asynchronous refill runs under the identity of the request that caused
   * it, and a filler thread serving one request after another runs each refill
   * under its own requester.
   * <p>
   * A refill task is put straight into the backing queue of the executor, so
   * the submission entry point that gives a pooled task the identity of its
   * submitter is bypassed and the task has to carry that identity itself. What
   * a refill reaches is {@link QueueRefiller#fillQueueForKey}, which for the
   * KMS provider asks a remote server for key material as the current user, so
   * a refill running as nobody, or as the requester before it, asks on the
   * wrong account and is recorded against the wrong one.
   * <p>
   * One filler thread is used, and every refill is asserted to have run on the
   * same thread as the first, so a later request really is served by a thread
   * an earlier one has already used and an identity left behind on it would be
   * seen. The last request is made with no identity at all and has to be
   * served with none, rather than with what the request before it left behind.
   *
   * @throws Exception if a wait times out or a queue operation fails
   */
  @Test
  @Timeout(value = 30)
  public void testRefillRunsUnderTheSubjectThatAskedForTheValues()
      throws Exception {
    Subject alice = newSubject("alice@EXAMPLE.COM");
    Subject bob = newSubject("bob@EXAMPLE.COM");
    RefillRecordingFiller filler = new RefillRecordingFiller();
    final ValueQueue<String> vq = new ValueQueue<String>(100, 0.1f, 30000, 1,
        SyncGenerationPolicy.ALL, filler);
    try {
      assertEquals("test", requestAs(alice, vq, "k1"),
          "the queue did not return a value for the first request");
      RefillInfo first = filler.awaitRefill();
      assertEquals("k1", first.key,
          "the first refill was not for the key that was asked for");
      assertSame(alice, first.subject,
          "a refill did not run under the identity that asked for the values");

      assertEquals("test", requestAs(bob, vq, "k2"),
          "the queue did not return a value for the second request");
      RefillInfo second = filler.awaitRefill();
      assertEquals("k2", second.key,
          "the second refill was not for the key that was asked for");
      assertSame(first.thread, second.thread,
          "the two refills ran on different threads, so nothing was left "
              + "behind for the second to observe");
      assertSame(bob, second.subject,
          "a filler thread used for a second request ran that request's "
              + "refill under the identity of the first");

      assertEquals("test", vq.getNext("k3"),
          "the queue did not return a value for the third request");
      RefillInfo third = filler.awaitRefill();
      assertEquals("k3", third.key,
          "the third refill was not for the key that was asked for");
      assertSame(first.thread, third.thread,
          "the third refill ran on a different thread, so nothing was left "
              + "behind for it to observe");
      assertNull(third.subject,
          "a refill asked for with no identity at all ran under an identity "
              + "an earlier request had left behind");
    } finally {
      vq.shutdown();
    }
  }

  private void waitForRefill(ValueQueue<?> valueQueue, String queueName, int queueSize)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> {
      int size = valueQueue.getSize(queueName);
      if (size != queueSize) {
        LOG.info("Current ValueQueue size is " + size);
        return false;
      }
      return true;
    }, 100, 3000);
  }

  /**
   * Waits until the filler pool has begun a refill of the given key.
   *
   * @param filler the refiller recording the fills
   * @param queueName the key whose refill to wait for
   * @throws TimeoutException if the refill has not begun in time
   * @throws InterruptedException if this thread is interrupted while waiting
   */
  private void waitForRefillToBegin(RecordingFiller filler, String queueName)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> !filler.asyncFills(queueName).isEmpty(),
        50, 3000);
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
   * Returns the one fill in a list, failing if there is not exactly one.
   *
   * @param fills the fills to look at
   * @param what what to report if there is not exactly one
   * @return the only fill
   */
  private static Fill onlyFill(List<Fill> fills, String what) {
    assertEquals(1, fills.size(), what);
    return fills.get(0);
  }

  /**
   * Verifies that Queue is initially filled to "numInitValues"
   */
  @Test
  @Timeout(value = 30)
  public void testInitFill() throws Exception {
    MockFiller filler = new MockFiller();
    ValueQueue<String> vq =
        new ValueQueue<String>(10, 0.1f, 30000, 1,
            SyncGenerationPolicy.ALL, filler);
    assertEquals("test", vq.getNext("k1"));
    assertEquals(1, filler.getTop().num);
    vq.shutdown();
  }

  /**
   * Verifies that Queue is initialized (Warmed-up) for provided keys
   */
  @Test
  @Timeout(value = 30)
  public void testWarmUp() throws Exception {
    MockFiller filler = new MockFiller();
    ValueQueue<String> vq =
        new ValueQueue<String>(10, 0.5f, 30000, 1,
            SyncGenerationPolicy.ALL, filler);
    vq.initializeQueuesForKeys("k1", "k2", "k3");
    FillInfo[] fillInfos =
      {filler.getTop(), filler.getTop(), filler.getTop()};
    assertEquals(5, fillInfos[0].num);
    assertEquals(5, fillInfos[1].num);
    assertEquals(5, fillInfos[2].num);
    assertEquals(new HashSet<>(Arrays.asList("k1", "k2", "k3")),
        new HashSet<>(Arrays.asList(fillInfos[0].key,
            fillInfos[1].key,
            fillInfos[2].key)));
    vq.shutdown();
  }

  /**
   * Verifies that Queue is initialized (Warmed-up) for partial keys.
   */
  @Test
  @Timeout(value = 30)
  public void testPartialWarmUp() throws Exception {
    MockFiller filler = new MockFiller();
    ValueQueue<String> vq =
        new ValueQueue<>(10, 0.5f, 30000, 1,
            SyncGenerationPolicy.ALL, filler);

    @SuppressWarnings("unchecked")
    LoadingCache<String, LinkedBlockingQueue<KeyProviderCryptoExtension.EncryptedKeyVersion>> kq =
        (LoadingCache<String, LinkedBlockingQueue<KeyProviderCryptoExtension.EncryptedKeyVersion>>)
            FieldUtils.getField(ValueQueue.class, "keyQueues", true).get(vq);

    LoadingCache<String, LinkedBlockingQueue<KeyProviderCryptoExtension.EncryptedKeyVersion>>
        kqSpy = spy(kq);
    doThrow(new ExecutionException(new Exception())).when(kqSpy).get("k2");
    FieldUtils.writeField(vq, "keyQueues", kqSpy, true);

    assertThrows(IOException.class, () -> vq.initializeQueuesForKeys("k1", "k2", "k3"));
    verify(kqSpy, times(1)).get("k2");

    FillInfo[] fillInfos =
        {filler.getTop(), filler.getTop(), filler.getTop()};
    assertEquals(5, fillInfos[0].num);
    assertEquals(5, fillInfos[1].num);
    assertNull(fillInfos[2]);

    assertEquals(new HashSet<>(Arrays.asList("k1", "k3")),
        new HashSet<>(Arrays.asList(fillInfos[0].key,
            fillInfos[1].key)));
    vq.shutdown();
  }

  /**
   * Verifies that the refill task is executed after "checkInterval" if
   * num values below "lowWatermark"
   */
  @Test
  @Timeout(value = 30)
  public void testRefill() throws Exception {
    MockFiller filler = new MockFiller();
    ValueQueue<String> vq =
        new ValueQueue<String>(100, 0.1f, 30000, 1,
            SyncGenerationPolicy.ALL, filler);
    // Trigger a prefill (10) and an async refill (91)
    assertEquals("test", vq.getNext("k1"));
    assertEquals(10, filler.getTop().num);

    // Wait for the async task to finish
    waitForRefill(vq, "k1", 100);
    // Refill task should add 91 values to get to a full queue (10 produced by
    // the prefill to the low watermark, 1 consumed by getNext())
    assertEquals(91, filler.getTop().num);
    vq.shutdown();
  }

  /**
   * Verifies that the No refill Happens after "checkInterval" if
   * num values above "lowWatermark"
   */
  @Test
  @Timeout(value = 30)
  public void testNoRefill() throws Exception {
    MockFiller filler = new MockFiller();
    ValueQueue<String> vq =
        new ValueQueue<String>(10, 0.5f, 30000, 1,
            SyncGenerationPolicy.ALL, filler);
    // Trigger a prefill (5) and an async refill (6)
    assertEquals("test", vq.getNext("k1"));
    assertEquals(5, filler.getTop().num);

    // Wait for the async task to finish
    waitForRefill(vq, "k1", 10);
    // Refill task should add 6 values to get to a full queue (5 produced by
    // the prefill to the low watermark, 1 consumed by getNext())
    assertEquals(6, filler.getTop().num);

    // Take another value, queue is still above the watermark
    assertEquals("test", vq.getNext("k1"));

    // Wait a while to make sure that no async refills are triggered
    try {
      waitForRefill(vq, "k1", 10);
    } catch (TimeoutException ignored) {
      // This is the correct outcome - no refill is expected
    }
    assertEquals(null, filler.getTop());
    vq.shutdown();
  }

  /**
   * Verify getAtMost when SyncGeneration Policy = ALL
   */
  @Test
  @Timeout(value = 30)
  public void testGetAtMostPolicyALL() throws Exception {
    MockFiller filler = new MockFiller();
    final ValueQueue<String> vq =
        new ValueQueue<String>(10, 0.1f, 30000, 1,
            SyncGenerationPolicy.ALL, filler);
    // Trigger a prefill (1) and an async refill (10)
    assertEquals("test", vq.getNext("k1"));
    assertEquals(1, filler.getTop().num);

    // Wait for the async task to finish
    waitForRefill(vq, "k1", 10);
    // Refill task should add 10 values to get to a full queue (1 produced by
    // the prefill to the low watermark, 1 consumed by getNext())
    assertEquals(10, filler.getTop().num);

    // Drain completely, no further refills triggered
    vq.drain("k1");

    // Wait a while to make sure that no async refills are triggered
    try {
      waitForRefill(vq, "k1", 10);
    } catch (TimeoutException ignored) {
      // This is the correct outcome - no refill is expected
    }
    assertNull(filler.getTop());

    // Synchronous call:
    // 1. Synchronously fill returned list
    // 2. Start another async task to fill the queue in the cache
    assertEquals(10, vq.getAtMost("k1", 10).size(), "Failed in sync call.");
    assertEquals(10, filler.getTop().num, "Sync call filler got wrong number.");

    // Wait for the async task to finish
    waitForRefill(vq, "k1", 10);
    // Refill task should add 10 values to get to a full queue
    assertEquals(10, filler.getTop().num, "Failed in async call.");

    // Drain completely after filled by the async thread
    vq.drain("k1");
    assertEquals(0, vq.getSize("k1"), "Failed to drain completely after async.");

    // Synchronous call
    assertEquals(19, vq.getAtMost("k1", 19).size(), "Failed to get all 19.");
    assertEquals(19, filler.getTop().num, "Failed in sync call.");
    vq.shutdown();
  }

  /**
   * Verify getAtMost when SyncGeneration Policy = ALL
   */
  @Test
  @Timeout(value = 30)
  public void testgetAtMostPolicyATLEAST_ONE() throws Exception {
    MockFiller filler = new MockFiller();
    ValueQueue<String> vq =
        new ValueQueue<String>(10, 0.3f, 30000, 1,
            SyncGenerationPolicy.ATLEAST_ONE, filler);
    // Trigger a prefill (3) and an async refill (8)
    assertEquals("test", vq.getNext("k1"));
    assertEquals(3, filler.getTop().num);

    // Wait for the async task to finish
    waitForRefill(vq, "k1", 10);
    // Refill task should add 8 values to get to a full queue (3 produced by
    // the prefill to the low watermark, 1 consumed by getNext())
    assertEquals(8, filler.getTop().num, "Failed in async call.");

    // Drain completely, no further refills triggered
    vq.drain("k1");

    // Queue is empty, sync will return a single value and trigger a refill
    assertEquals(1, vq.getAtMost("k1", 10).size());
    assertEquals(1, filler.getTop().num);

    // Wait for the async task to finish
    waitForRefill(vq, "k1", 10);
    // Refill task should add 10 values to get to a full queue
    assertEquals(10, filler.getTop().num, "Failed in async call.");
    vq.shutdown();
  }

  /**
   * Verify getAtMost when SyncGeneration Policy = LOW_WATERMARK
   */
  @Test
  @Timeout(value = 30)
  public void testgetAtMostPolicyLOW_WATERMARK() throws Exception {
    MockFiller filler = new MockFiller();
    ValueQueue<String> vq =
        new ValueQueue<String>(10, 0.3f, 30000, 1,
            SyncGenerationPolicy.LOW_WATERMARK, filler);
    // Trigger a prefill (3) and an async refill (8)
    assertEquals("test", vq.getNext("k1"));
    assertEquals(3, filler.getTop().num);

    // Wait for the async task to finish
    waitForRefill(vq, "k1", 10);
    // Refill task should add 8 values to get to a full queue (3 produced by
    // the prefill to the low watermark, 1 consumed by getNext())
    assertEquals(8, filler.getTop().num, "Failed in async call.");

    // Drain completely, no further refills triggered
    vq.drain("k1");

    // Queue is empty, sync will return 3 values and trigger a refill
    assertEquals(3, vq.getAtMost("k1", 10).size());
    assertEquals(3, filler.getTop().num);

    // Wait for the async task to finish
    waitForRefill(vq, "k1", 10);
    // Refill task should add 10 values to get to a full queue
    assertEquals(10, filler.getTop().num, "Failed in async call.");
    vq.shutdown();
  }

  @Test
  @Timeout(value = 30)
  public void testDrain() throws Exception {
    MockFiller filler = new MockFiller();
    ValueQueue<String> vq =
        new ValueQueue<String>(10, 0.1f, 30000, 1,
            SyncGenerationPolicy.ALL, filler);
    // Trigger a prefill (1) and an async refill (10)
    assertEquals("test", vq.getNext("k1"));
    assertEquals(1, filler.getTop().num);

    // Wait for the async task to finish
    waitForRefill(vq, "k1", 10);
    // Refill task should add 10 values to get to a full queue (1 produced by
    // the prefill to the low watermark, 1 consumed by getNext())
    assertEquals(10, filler.getTop().num);

    // Drain completely, no further refills triggered
    vq.drain("k1");

    // Wait a while to make sure that no async refills are triggered
    try {
      waitForRefill(vq, "k1", 10);
    } catch (TimeoutException ignored) {
      // This is the correct outcome - no refill is expected
    }
    assertNull(filler.getTop());
    vq.shutdown();
  }

  /**
   * A refill runs as the caller whose request made it necessary.
   * <p>
   * A refill task is not submitted through the filler pool: it is put straight
   * into the queue that backs the pool, so the pool never gets the chance to
   * carry the queueing thread's identity over to the thread that runs it. The
   * refill still has to reach the key provider as the caller that emptied the
   * queue, because that is whose request the values are being fetched for and
   * whose access to the key the provider will check. This asserts that it does,
   * for the fill the caller makes for itself and for the refill the pool makes
   * afterwards, and it asserts the numbers of values both fills were asked for
   * so that carrying the identity is shown to leave the queue's own accounting
   * exactly as {@link #testRefill()} finds it.
   *
   * @throws Exception if a fill fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testAsyncRefillRunsUnderTheIdentityThatTriggeredIt()
      throws Exception {
    final Subject reader = newSubject("value-queue-reader@EXAMPLE.COM");
    RecordingFiller filler = new RecordingFiller();
    final ValueQueue<String> vq = new ValueQueue<String>(100, 0.1f, 30000, 1,
        SyncGenerationPolicy.ALL, filler);
    try {
      // Trigger a prefill (10) and an async refill (91), as the reader.
      SubjectUtil.callAs(reader, new Callable<Void>() {
        public Void call() throws Exception {
          assertEquals("test", vq.getNext("k1"));
          return (Void) null;
        }
      });

      waitForRefill(vq, "k1", 100);

      Fill sync = onlyFill(filler.syncFills("k1"),
          "the caller's own thread did not fill the queue exactly once");
      assertEquals(10, sync.num,
          "the prefill was asked for the wrong number of values");
      assertSame(reader, sync.subject,
          "the fill the caller made for itself did not run under the "
              + "caller's identity");

      Fill async = onlyFill(filler.asyncFills("k1"),
          "the filler pool did not refill the queue exactly once");
      assertEquals(91, async.num,
          "the refill was asked for the wrong number of values");
      assertSame(reader, async.subject,
          "the refill did not run under the identity of the caller that "
              + "triggered it");
    } finally {
      vq.shutdown();
    }
  }

  /**
   * A refill runs as the caller that triggered it, not as whoever happened to
   * cause a filler thread to be created.
   * <p>
   * The filler threads are started by the first request that needs a refill, so
   * on a runtime where a new thread takes on the identity of the thread that
   * created it, a filler thread carries that first caller's identity for the
   * rest of its life. Every later refill it runs would then be made as that
   * first caller rather than as the caller that emptied the queue, which is the
   * kind of confusion of one user's work with another's that the queue must not
   * introduce. Here the filler thread is started by a request made under no
   * identity at all, and the refill that follows is asserted to have been made
   * under none; the same thread is then made to run a refill for a caller with
   * an identity, and that refill is asserted to have been made under it. The
   * thread is compared by reference across the two, so the second assertion
   * cannot pass because a fresh thread was used.
   *
   * @throws Exception if a fill fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testAsyncRefillRunsAsTheTriggeringCallerNotTheThreadStarter()
      throws Exception {
    RecordingFiller filler = new RecordingFiller();
    final ValueQueue<String> vq = new ValueQueue<String>(10, 0.1f, 30000, 1,
        SyncGenerationPolicy.ALL, filler);
    try {
      // No identity: this request is what starts the filler thread.
      assertEquals("test", vq.getNext("k1"));
      waitForRefill(vq, "k1", 10);

      Fill started = onlyFill(filler.asyncFills("k1"),
          "the filler pool did not refill the queue exactly once");
      assertNull(started.subject,
          "a refill triggered by a caller with no identity was made under "
              + "one");
      Thread fillerThread = started.thread;

      // Empty the queue again so that the next request needs a refill.
      vq.drain("k1");
      filler.forgetFills();

      final Subject reader = newSubject("value-queue-late@EXAMPLE.COM");
      SubjectUtil.callAs(reader, new Callable<Void>() {
        public Void call() throws Exception {
          assertEquals("test", vq.getNext("k1"));
          return (Void) null;
        }
      });

      waitForRefill(vq, "k1", 10);

      Fill later = onlyFill(filler.asyncFills("k1"),
          "the filler pool did not refill the queue exactly once for the "
              + "later caller");
      assertSame(fillerThread, later.thread,
          "the filler pool used a second thread, so this asserts nothing "
              + "about where a reused thread takes its identity from");
      assertSame(reader, later.subject,
          "the refill was made as whoever started the filler thread instead "
              + "of as the caller that triggered it");
    } finally {
      vq.shutdown();
    }
  }

  /**
   * A refill triggered by a caller with no identity is made under none.
   * <p>
   * Carrying an identity to the filler thread must not turn into inventing one.
   * A filler thread may already hold an identity from an earlier refill, or
   * from having been created by a caller that had one, and a request made by
   * nobody must not be filled as that somebody: the key provider would then be
   * asked for values on behalf of a user who never asked for them. Both the
   * fill the caller makes for itself and the refill the pool makes afterwards
   * are asserted to have been made under no identity.
   *
   * @throws Exception if a fill fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testRefillWithoutAnIdentityRunsWithoutOne() throws Exception {
    RecordingFiller filler = new RecordingFiller();
    ValueQueue<String> vq = new ValueQueue<String>(10, 0.1f, 30000, 1,
        SyncGenerationPolicy.ALL, filler);
    try {
      // Trigger a prefill (1) and an async refill (10), as nobody.
      assertEquals("test", vq.getNext("k1"));
      waitForRefill(vq, "k1", 10);

      Fill sync = onlyFill(filler.syncFills("k1"),
          "the caller's own thread did not fill the queue exactly once");
      assertNull(sync.subject,
          "the fill a caller with no identity made for itself was made "
              + "under one");

      Fill async = onlyFill(filler.asyncFills("k1"),
          "the filler pool did not refill the queue exactly once");
      assertEquals(10, async.num,
          "the refill was asked for the wrong number of values");
      assertNull(async.subject,
          "a refill triggered by a caller with no identity took on the "
              + "identity of the filler thread that ran it");
    } finally {
      vq.shutdown();
    }
  }

  /**
   * Draining a key still cancels and removes a refill of it that is waiting to
   * run, now that a refill carries the identity of the caller that queued it.
   * <p>
   * The queue that backs the filler pool tracks the keys being refilled, and
   * draining a key finds that key's task, cancels it and removes it from both
   * the queue and the pool. All three go by the task itself, so a refill that
   * is prepared to run under an identity has to reach the queue as a task the
   * key can still be found by and the pool can still be given back. This holds
   * one filler thread inside a refill of one key, leaves refills of two more
   * keys waiting behind it, drains one of those two, and then lets the held
   * refill go: the drained key's refill must never run, while the other one
   * must, so that removing the right task is told apart from removing every
   * task. Both refills that do run are asserted to have been made under the
   * caller's identity, since a task the queue can still account for is of no
   * use if it has lost the identity it was queued with.
   *
   * @throws Exception if a fill fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testDrainCancelsAQueuedSubjectAwareRefill() throws Exception {
    final Subject reader = newSubject("value-queue-drainer@EXAMPLE.COM");
    RecordingFiller filler = new RecordingFiller();
    final ValueQueue<String> vq = new ValueQueue<String>(10, 0.1f, 30000, 1,
        SyncGenerationPolicy.ALL, filler);
    CountDownLatch release = filler.holdBackFillsOnOtherThreads();
    try {
      // Each request prefills (1), takes the value, and queues a refill (10).
      SubjectUtil.callAs(reader, new Callable<Void>() {
        public Void call() throws Exception {
          assertEquals("test", vq.getNext(HELD_KEY));
          waitForRefillToBegin(filler, HELD_KEY);
          assertEquals("test", vq.getNext(DRAINED_KEY));
          assertEquals("test", vq.getNext(KEPT_KEY));
          return (Void) null;
        }
      });

      // Every one of the three requests found an empty queue, filled it for
      // itself and so had a refill queued behind it. That is what makes the
      // key left alone below a control for the key that is drained, rather
      // than the two keys differing in how they were queued.
      for (String keyName : new String[] {HELD_KEY, DRAINED_KEY, KEPT_KEY}) {
        Fill own = onlyFill(filler.syncFills(keyName),
            "the caller did not fill " + keyName + " for itself once");
        assertSame(reader, own.subject,
            "the fill the caller made for itself under " + keyName
                + " did not run under the caller's identity");
      }

      // The only filler thread is held inside the refill of HELD_KEY, so the
      // refills of the other two keys are waiting in the pool's queue.
      vq.drain(DRAINED_KEY);
      release.countDown();

      waitForRefill(vq, KEPT_KEY, 10);
      waitForRefill(vq, HELD_KEY, 10);

      Fill held = onlyFill(filler.asyncFills(HELD_KEY),
          "the refill that was held back did not run exactly once");
      assertSame(reader, held.subject,
          "the refill that was held back lost the identity it was queued "
              + "with");

      Fill kept = onlyFill(filler.asyncFills(KEPT_KEY),
          "the refill that was left queued did not run exactly once");
      assertSame(reader, kept.subject,
          "the refill that waited in the queue lost the identity it was "
              + "queued with");

      assertTrue(filler.asyncFills(DRAINED_KEY).isEmpty(),
          "a refill that draining the key removed from the queue ran anyway");
      assertEquals(0, vq.getSize(DRAINED_KEY),
          "draining the key left values behind");
    } finally {
      release.countDown();
      vq.shutdown();
    }
  }

  /**
   * A refiller that records the user each refill served by a filler thread ran
   * as, read the way a real refiller reads it: {@code KMSClientProvider} calls
   * {@link UserGroupInformation#getCurrentUser()} to decide which user to
   * authenticate as, and whether the request is being made on behalf of a
   * proxied user.
   * <p>
   * A fill that happens on the thread that asked for a value is left out, so
   * that what is recorded is only what the filler pool ran.
   */
  private static final class RefillIdentityFiller
      implements QueueRefiller<String> {

    /** The thread this filler, and so every request in the test, is made on. */
    private final Thread requester = Thread.currentThread();
    private final LinkedBlockingQueue<String> asyncUsers =
        new LinkedBlockingQueue<>();

    @Override
    public void fillQueueForKey(String keyName, Queue<String> keyQueue,
        int numValues) throws IOException {
      if (Thread.currentThread() != requester) {
        asyncUsers.add(
            UserGroupInformation.getCurrentUser().getShortUserName());
      }
      for (int i = 0; i < numValues; i++) {
        keyQueue.add("test");
      }
    }

    /**
     * Returns the user the next refill served by a filler thread ran as, or
     * {@code null} if no such refill arrived.
     */
    String nextAsyncUser() throws InterruptedException {
      return asyncUsers.poll(30, TimeUnit.SECONDS);
    }
  }

  /**
   * Empties the queue for a key as {@code user}, far enough below the watermark
   * that a refill is queued for the filler pool to run.
   */
  private static void requestAs(final String user, final ValueQueue<String> vq,
      final String keyName) {
    UserGroupInformation.createRemoteUser(user)
        .doAs((PrivilegedAction<Void>) () -> {
          try {
            vq.getAtMost(keyName, 9);
            return null;
          } catch (IOException | ExecutionException e) {
            throw new RuntimeException(e);
          }
        });
  }

  /**
   * A refill runs as the user whose request made it necessary, and not as the
   * service the filler pool belongs to.
   * <p>
   * A refill task is put straight into the queue that backs the filler pool
   * rather than submitted through it, so nothing on that path carries the
   * requesting user over to the filler thread; the task has to read the identity
   * itself, as it is queued. Without that, the refill reaches the key provider
   * with no identity in force, and
   * {@link UserGroupInformation#getCurrentUser()} falls back to the logged-in
   * user -- so the keys the request asked for would be generated as the service,
   * and the audit record would name the service.
   */
  @Test
  @Timeout(value = 30)
  public void testRefillRunsAsTheRequestingUser() throws Exception {
    RefillIdentityFiller filler = new RefillIdentityFiller();
    ValueQueue<String> vq = new ValueQueue<>(10, 0.5f, 30000, 1,
        SyncGenerationPolicy.ATLEAST_ONE, filler);
    try {
      requestAs("alice", vq, "k1");
      assertEquals("alice", filler.nextAsyncUser(),
          "the refill alice's request queued did not run as alice");
    } finally {
      vq.shutdown();
    }
  }

  /**
   * A refill queued by a second user is not run as the user whose request
   * created the filler thread.
   * <p>
   * The pool is created with a single filler thread, and that one thread serves
   * every refill, so the thread the second user's refill runs on is the very
   * thread the first user's request brought into being. A filler thread that was
   * given the first user's identity when it was created keeps it for as long as
   * it lives, so a refill that took its identity from the thread rather than
   * from the request that queued it would generate the second user's keys as the
   * first user and name the first user in the audit record. The identity is read
   * when the refill is queued, on every runtime, so the second user's refill
   * runs as the second user.
   */
  @Test
  @Timeout(value = 60)
  public void testRefillForSecondUserOnTheSameFillerThread() throws Exception {
    RefillIdentityFiller filler = new RefillIdentityFiller();
    ValueQueue<String> vq = new ValueQueue<>(10, 0.5f, 30000, 1,
        SyncGenerationPolicy.ATLEAST_ONE, filler);
    try {
      // alice's request is what pre-starts the single filler thread.
      requestAs("alice", vq, "k1");
      assertEquals("alice", filler.nextAsyncUser(),
          "the refill alice's request queued did not run as alice");

      requestAs("bob", vq, "k2");
      assertEquals("bob", filler.nextAsyncUser(),
          "the refill bob's request queued ran as the user whose request had"
              + " created the filler thread rather than as bob");
    } finally {
      vq.shutdown();
    }
  }

}
