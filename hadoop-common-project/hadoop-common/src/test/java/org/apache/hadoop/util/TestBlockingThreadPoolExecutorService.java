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

package org.apache.hadoop.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Asserts which group of threads the threads of a blocking pool belong to.
 * <p>
 * That group used to be asked of the runtime's security manager, falling back to
 * the group of the thread building the factory whenever there was none. The
 * runtime no longer has one at all, so the question is gone and the group of the
 * thread building the factory is used outright. Nothing about that is visible
 * from a thread's name, or from whether it is a daemon, or from what identity it
 * runs under: a thread put in the wrong group still runs the same work in the
 * same way. What changes is where an operator looks for it and what an uncaught
 * failure in it is reported against, so the group is asserted here explicitly.
 * <p>
 * Asserting it needs the group to be something other than the group of the
 * thread that asks the factory for a thread, or the two would be
 * indistinguishable. Each of these tests therefore builds the factory on a
 * thread of a group made for the purpose and then asks for threads from the
 * thread running the test, so that the group being carried from the one to the
 * other is the only thing that can explain the outcome.
 */
public class TestBlockingThreadPoolExecutorService {

  /** Bound on every wait, generous enough to survive a loaded build host. */
  private static final long TIMEOUT_SECONDS = 10;

  /** Result a task returns to prove it ran to completion. */
  private static final String SENTINEL = "task-completed";

  /** Work for a thread that is never started. */
  private static final Runnable NOTHING = new Runnable() {
    @Override
    public void run() {
    }
  };

  /**
   * Builds a thread factory on a thread of a group of its own, and returns both.
   *
   * @param groupName what to call the group built for the purpose
   * @param daemonFactory whether to build the factory that makes daemon threads
   * @param prefix the name prefix to build the factory with
   * @return the group the factory was built in, and the factory itself
   * @throws InterruptedException if this thread is interrupted while waiting
   */
  private static FactoryInItsOwnGroup buildFactoryInItsOwnGroup(
      String groupName, final boolean daemonFactory, final String prefix)
      throws InterruptedException {
    final ThreadGroup group = new ThreadGroup(groupName);
    final AtomicReference<ThreadFactory> built = new AtomicReference<>();
    final AtomicReference<ThreadGroup> builtIn = new AtomicReference<>();
    Thread builder = new Thread(group, new Runnable() {
      @Override
      public void run() {
        builtIn.set(Thread.currentThread().getThreadGroup());
        built.set(daemonFactory
            ? BlockingThreadPoolExecutorService.newDaemonThreadFactory(prefix)
            : BlockingThreadPoolExecutorService.getNamedThreadFactory(prefix));
      }
    }, groupName + "-builder");
    builder.start();
    builder.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
    // Read from inside the builder: a thread that has finished reports no group.
    assertSame(group, builtIn.get(),
        "the thread that built the factory was not in the group built for it");
    assertTrue(built.get() != null, "the factory was never built");
    return new FactoryInItsOwnGroup(group, built.get());
  }

  /**
   * A group built for a test and the thread factory built inside it.
   */
  private static final class FactoryInItsOwnGroup {

    /** The group the factory was built in. */
    private final ThreadGroup group;

    /** The factory built in that group. */
    private final ThreadFactory factory;

    FactoryInItsOwnGroup(ThreadGroup group, ThreadFactory factory) {
      this.group = group;
      this.factory = factory;
    }
  }

  /**
   * Threads made by the named factory belong to the group of the thread that
   * built it.
   * <p>
   * They belong to it however far from that thread they are asked for, which is
   * what asserting from a thread of another group shows. Their names and the
   * kind of thread they are are asserted alongside, so that the group is pinned
   * down without loosening anything else about them.
   *
   * @throws InterruptedException if this thread is interrupted while waiting
   */
  @Test
  @Timeout(value = 30)
  public void testNamedFactoryUsesTheGroupItWasBuiltIn()
      throws InterruptedException {
    FactoryInItsOwnGroup built =
        buildFactoryInItsOwnGroup("named-factory-group", false, "named");

    Thread first = built.factory.newThread(NOTHING);
    Thread second = built.factory.newThread(NOTHING);

    assertSame(built.group, first.getThreadGroup(),
        "a thread the factory made is not in the group the factory was built "
            + "in");
    assertSame(built.group, second.getThreadGroup(),
        "a later thread the factory made is not in the group the factory was "
            + "built in");
    assertNotSame(Thread.currentThread().getThreadGroup(),
        first.getThreadGroup(),
        "a thread the factory made is in the group of the thread that asked "
            + "for it rather than the group the factory was built in");
    assertTrue(first instanceof SubjectInheritingThread,
        "a thread the factory made is not the kind of thread that carries an "
            + "identity");
    assertTrue(first.getName().startsWith("named-pool"),
        "a thread the factory made is not named after the factory: "
            + first.getName());
    assertTrue(first.getName().endsWith("-t1"),
        "the first thread the factory made is not numbered first: "
            + first.getName());
    assertTrue(second.getName().endsWith("-t2"),
        "the second thread the factory made is not numbered second: "
            + second.getName());
  }

  /**
   * Threads made by the daemon factory belong to the group of the thread that
   * built it, and are daemon threads of ordinary priority.
   * <p>
   * This factory makes its threads through the named one and then settles what
   * kind of thread they are, so the group has to survive that as well.
   *
   * @throws InterruptedException if this thread is interrupted while waiting
   */
  @Test
  @Timeout(value = 30)
  public void testDaemonFactoryUsesTheGroupItWasBuiltIn()
      throws InterruptedException {
    FactoryInItsOwnGroup built =
        buildFactoryInItsOwnGroup("daemon-factory-group", true, "daemon");

    Thread made = built.factory.newThread(NOTHING);

    assertSame(built.group, made.getThreadGroup(),
        "a thread the daemon factory made is not in the group the factory was "
            + "built in");
    assertNotSame(Thread.currentThread().getThreadGroup(),
        made.getThreadGroup(),
        "a thread the daemon factory made is in the group of the thread that "
            + "asked for it rather than the group the factory was built in");
    assertTrue(made.isDaemon(),
        "a thread the daemon factory made is not a daemon thread");
    assertEquals(Thread.NORM_PRIORITY, made.getPriority(),
        "a thread the daemon factory made is not of ordinary priority");
    assertTrue(made.getName().startsWith("daemon-pool"),
        "a thread the daemon factory made is not named after the factory: "
            + made.getName());
  }

  /**
   * Each factory numbers its pool differently from the last.
   * <p>
   * Two factories built one after another have to name their threads apart, or
   * two pools of the same prefix would be indistinguishable in a thread dump.
   *
   * @throws InterruptedException if this thread is interrupted while waiting
   */
  @Test
  @Timeout(value = 30)
  public void testEachFactoryNumbersItsPoolDifferently()
      throws InterruptedException {
    FactoryInItsOwnGroup first =
        buildFactoryInItsOwnGroup("first-pool-group", false, "counted");
    FactoryInItsOwnGroup second =
        buildFactoryInItsOwnGroup("second-pool-group", false, "counted");

    String fromFirst = first.factory.newThread(NOTHING).getName();
    String fromSecond = second.factory.newThread(NOTHING).getName();

    assertNotSame(fromFirst, fromSecond,
        "two factories named their threads with the same name");
    assertTrue(!fromFirst.equals(fromSecond),
        "two factories numbered their pools the same: " + fromFirst + " and "
            + fromSecond);
  }

  /**
   * A pool built by the blocking service runs its work on a thread of the group
   * the pool was built in.
   * <p>
   * This is the whole chain the group is carried along, from the thread that
   * builds the pool to the thread that runs the work, and it is what a caller
   * of this service actually gets.
   *
   * @throws Exception if the work fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testPoolRunsItsWorkInTheGroupItWasBuiltIn() throws Exception {
    final ThreadGroup group = new ThreadGroup("blocking-pool-group");
    final AtomicReference<BlockingThreadPoolExecutorService> pool =
        new AtomicReference<>();
    Thread builder = new Thread(group, new Runnable() {
      @Override
      public void run() {
        pool.set(BlockingThreadPoolExecutorService.newInstance(1, 1, 10,
            TimeUnit.MINUTES, "blocking-grouped"));
      }
    }, "blocking-pool-builder");
    builder.start();
    builder.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
    final AtomicReference<Thread> ranOn = new AtomicReference<>();

    try {
      String result = pool.get().submit(new Callable<String>() {
        @Override
        public String call() {
          ranOn.set(Thread.currentThread());
          return SENTINEL;
        }
      }).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

      assertEquals(SENTINEL, result, "the work did not run to completion");
      assertSame(group, ranOn.get().getThreadGroup(),
          "the work ran on a thread outside the group the pool was built in");
      assertTrue(ranOn.get().isDaemon(),
          "the work ran on a thread that is not a daemon thread");
      assertTrue(ranOn.get().getName().startsWith("blocking-grouped-pool"),
          "the work ran on a thread not named after the pool: "
              + ranOn.get().getName());
    } finally {
      pool.get().shutdownNow();
      assertTrue(
          pool.get().awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS),
          "the pool this test built did not terminate");
    }
  }
}
