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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Asserts that identities nest correctly when a scope that establishes one
 * hands work to an executor and the task then establishes another.
 * <p>
 * Running work as a user means establishing that user for as long as the work
 * lasts, and such a scope may be entered again from within itself, either on
 * the same thread or on a pool worker the outer scope handed the work to. Each
 * of these tests therefore reads the identity in force at every level of a
 * nested arrangement: on the thread that established the outer one, inside the
 * task the pool ran for it, inside the scope the task established for itself,
 * and again once that scope has been left. An identity that failed to reach the
 * task would leave the work running as whoever the process logged in as, and
 * one that outlived the scope that established it would leave a later caller
 * running as a user it never asked for; either decides authorization and is
 * what an audit record names.
 * <p>
 * Every assertion here is about an identity that was observed rather than about
 * how it was carried, so the same assertions hold on every runtime this project
 * supports: where the runtime hands a new thread the identity of its creator
 * and where it hands over nothing, a task observes the identity that submitted
 * it either way. Each pooled hop uses an executor created for it whose first
 * submission is made from inside the scope under test, so the worker that runs
 * the task belongs to that scope; identities are built with names of their own
 * so that a silent fall back to the process login can never be mistaken for
 * success; and every observation crosses a thread boundary through a future or
 * an atomic reference. Identities are compared as identities rather than by
 * name as well, because a user is told apart by the identity behind it.
 */
public class TestNestedSubjectPropagation {

  /** Identity the outermost scope of each test establishes. */
  private static final String OUTER_USER = "nested-outer-user";

  /** Identity the scope nested inside the outer one establishes. */
  private static final String INNER_USER = "nested-inner-user";

  /** Identity the innermost of three nested scopes establishes. */
  private static final String DEEPEST_USER = "nested-deepest-user";

  /** Bound on every wait, generous enough to survive a loaded build host. */
  private static final long TIMEOUT_SECONDS = 10;

  /** Every executor a test created, shut down when that test ends. */
  private final List<ExecutorService> pools = new ArrayList<>();

  /** The configuration the identity machinery is set up with. */
  private Configuration conf;

  /** Puts the identity machinery into a known state before each test. */
  @BeforeEach
  public void setupUgi() {
    conf = new Configuration();
    UserGroupInformation.reset();
    UserGroupInformation.setConfiguration(conf);
  }

  /** Leaves no logged in user behind for the next test to find. */
  @AfterEach
  public void resetUgi() {
    UserGroupInformation.setLoginUser(null);
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
   * Waits for a submitted task and yields what it returned.
   * <p>
   * A failure inside a pooled task arrives here wrapped. An assertion that
   * failed there has to reach the test runner as it was, so it is rethrown
   * unchanged; anything else is reported as the failure of a task rather than
   * of the wait, and a wait that runs out is reported as such.
   *
   * @param <T> the type the task yields
   * @param submitted the task to wait for
   * @return what the task returned
   */
  private static <T> T awaitQuietly(Future<T> submitted) {
    try {
      return submitted.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("interrupted while waiting for a task", e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof Error) {
        throw (Error) cause;
      }
      throw new RuntimeException("a task submitted by this test failed",
          cause == null ? e : cause);
    } catch (TimeoutException e) {
      throw new RuntimeException("a task did not finish within "
          + TIMEOUT_SECONDS + " seconds", e);
    }
  }

  /**
   * A pooled task runs under the identity that submitted it, a scope the task
   * establishes for itself wins for as long as it lasts, and the submitter's
   * identity is exactly restored once that scope has been left.
   * <p>
   * The four readings this takes are the whole of what nesting has to mean. The
   * first is taken on the thread that established the outer identity, so that a
   * failure there is told apart from one that only shows up across a thread
   * boundary. The second is taken inside the task the pool ran, which is where
   * an identity that never travelled would show up as the process login. The
   * third is taken inside the scope the task established for itself, and the
   * fourth after leaving it, which is where an identity established without
   * being taken back down again would show up. Each reading is asserted both as
   * an identity and by the name of its user, so that neither a second identity
   * carrying the same name nor the same identity renamed could satisfy it.
   *
   * @throws Exception if a task fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testNestedDoAsAcrossAPooledTask() throws Exception {
    final UserGroupInformation outer =
        UserGroupInformation.createRemoteUser(OUTER_USER);
    final UserGroupInformation inner =
        UserGroupInformation.createRemoteUser(INNER_USER);
    final AtomicReference<UserGroupInformation> inTaskBeforeInner =
        new AtomicReference<>();
    final AtomicReference<UserGroupInformation> inTaskInsideInner =
        new AtomicReference<>();
    final AtomicReference<UserGroupInformation> inTaskAfterInner =
        new AtomicReference<>();

    outer.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        assertEquals(outer, UserGroupInformation.getCurrentUser(),
            "level 1: the outer identity was not in force on the thread that "
                + "established it");
        ExecutorService pool = register(HadoopExecutors.newFixedThreadPool(1));
        awaitQuietly(pool.submit(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            inTaskBeforeInner.set(UserGroupInformation.getCurrentUser());
            inner.doAs(new PrivilegedExceptionAction<Void>() {
              @Override
              public Void run() throws Exception {
                inTaskInsideInner.set(UserGroupInformation.getCurrentUser());
                return null;
              }
            });
            inTaskAfterInner.set(UserGroupInformation.getCurrentUser());
            return null;
          }
        }));
        return null;
      }
    });

    assertEquals(outer, inTaskBeforeInner.get(),
        "level 2: the submitter's identity did not reach the pooled task");
    assertEquals(inner, inTaskInsideInner.get(),
        "level 3: the inner identity did not win inside the scope that "
            + "established it");
    assertEquals(outer, inTaskAfterInner.get(),
        "level 4: the outer identity was not exactly restored once the inner "
            + "scope had been left");
    assertEquals(OUTER_USER, inTaskBeforeInner.get().getUserName(),
        "level 2: the pooled task ran as a user of another name");
    assertEquals(INNER_USER, inTaskInsideInner.get().getUserName(),
        "level 3: the inner scope ran as a user of another name");
    assertEquals(OUTER_USER, inTaskAfterInner.get().getUserName(),
        "level 4: the identity that was restored was a user of another name");
    assertNotEquals(outer, UserGroupInformation.getCurrentUser(),
        "the outer identity outlived the scope that established it");
  }

  /**
   * A scope entered again from within itself gives the identity it establishes
   * priority for as long as it lasts and gives the one it interrupted back
   * afterwards, on a single thread.
   * <p>
   * This is the case with no executor in it at all, so a failure here says that
   * nesting itself is wrong rather than that an identity failed to cross a
   * thread boundary, which is what tells the two apart.
   *
   * @throws Exception if an identity cannot be read
   */
  @Test
  @Timeout(value = 30)
  public void testNestedDoAsOnOneThread() throws Exception {
    final UserGroupInformation outer =
        UserGroupInformation.createRemoteUser(OUTER_USER);
    final UserGroupInformation inner =
        UserGroupInformation.createRemoteUser(INNER_USER);

    outer.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        assertEquals(outer, UserGroupInformation.getCurrentUser(),
            "the outer identity was not in force inside its own scope");
        inner.doAs(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            assertEquals(inner, UserGroupInformation.getCurrentUser(),
                "the inner identity did not win inside its own scope");
            return null;
          }
        });
        assertEquals(outer, UserGroupInformation.getCurrentUser(),
            "the outer identity was not restored once the inner scope had "
                + "been left");
        return null;
      }
    });
  }

  /**
   * A task submitted from inside the inner scope runs under the inner identity,
   * not under the one that scope was entered from.
   * <p>
   * This is the nesting of the first case in the other order. The identity in
   * force at the moment of submission is the innermost one, so an
   * implementation that read an identity anywhere other than at the submission
   * would hand the task the outer one instead. The executor is created inside
   * the inner scope, so the worker that runs the task belongs to it.
   *
   * @throws Exception if a task fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testTaskSubmittedFromTheInnerScopeRunsUnderTheInnerIdentity()
      throws Exception {
    final UserGroupInformation outer =
        UserGroupInformation.createRemoteUser(OUTER_USER);
    final UserGroupInformation inner =
        UserGroupInformation.createRemoteUser(INNER_USER);
    final AtomicReference<UserGroupInformation> inTask =
        new AtomicReference<>();

    outer.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        return inner.doAs(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            ExecutorService pool =
                register(HadoopExecutors.newFixedThreadPool(1));
            awaitQuietly(pool.submit(new Callable<Void>() {
              @Override
              public Void call() throws Exception {
                inTask.set(UserGroupInformation.getCurrentUser());
                return null;
              }
            }));
            return null;
          }
        });
      }
    });

    assertEquals(inner, inTask.get(),
        "a task submitted from inside the inner scope did not observe the "
            + "inner identity");
    assertEquals(INNER_USER, inTask.get().getUserName(),
        "a task submitted from inside the inner scope ran as a user of "
            + "another name");
    assertNotEquals(outer, inTask.get(),
        "a task submitted from inside the inner scope observed the identity "
            + "that scope was entered from");
  }

  /**
   * Three scopes nested on a pool worker each win in turn and are each taken
   * back down in the order they were established.
   * <p>
   * Two levels show that an identity can be established over another; three
   * show that what is given back on the way out is the level immediately
   * outside rather than the outermost one, which two levels cannot tell apart.
   *
   * @throws Exception if a task fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testThreeNestedScopesOnAPoolWorkerUnwindInOrder()
      throws Exception {
    final UserGroupInformation outer =
        UserGroupInformation.createRemoteUser(OUTER_USER);
    final UserGroupInformation inner =
        UserGroupInformation.createRemoteUser(INNER_USER);
    final UserGroupInformation deepest =
        UserGroupInformation.createRemoteUser(DEEPEST_USER);
    final AtomicReference<UserGroupInformation> atOuter =
        new AtomicReference<>();
    final AtomicReference<UserGroupInformation> atInner =
        new AtomicReference<>();
    final AtomicReference<UserGroupInformation> atDeepest =
        new AtomicReference<>();
    final AtomicReference<UserGroupInformation> backAtInner =
        new AtomicReference<>();
    final AtomicReference<UserGroupInformation> backAtOuter =
        new AtomicReference<>();

    outer.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        ExecutorService pool = register(HadoopExecutors.newFixedThreadPool(1));
        awaitQuietly(pool.submit(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            atOuter.set(UserGroupInformation.getCurrentUser());
            inner.doAs(new PrivilegedExceptionAction<Void>() {
              @Override
              public Void run() throws Exception {
                atInner.set(UserGroupInformation.getCurrentUser());
                deepest.doAs(new PrivilegedExceptionAction<Void>() {
                  @Override
                  public Void run() throws Exception {
                    atDeepest.set(UserGroupInformation.getCurrentUser());
                    return null;
                  }
                });
                backAtInner.set(UserGroupInformation.getCurrentUser());
                return null;
              }
            });
            backAtOuter.set(UserGroupInformation.getCurrentUser());
            return null;
          }
        }));
        return null;
      }
    });

    assertEquals(outer, atOuter.get(),
        "the submitter's identity did not reach the pooled task");
    assertEquals(inner, atInner.get(),
        "the second identity did not win inside the scope that established "
            + "it");
    assertEquals(deepest, atDeepest.get(),
        "the third identity did not win inside the scope that established it");
    assertEquals(inner, backAtInner.get(),
        "leaving the third scope gave back an identity other than the one "
            + "immediately outside it");
    assertEquals(outer, backAtOuter.get(),
        "leaving the second scope did not give the submitter's identity back");
    assertEquals(DEEPEST_USER, atDeepest.get().getUserName(),
        "the innermost scope ran as a user of another name");
  }

  /**
   * A pooled task that establishes an identity of its own still reports the
   * submitter's once it is done, through the future the submission yielded.
   * <p>
   * The names travel back as the value of the task rather than through a field
   * this test set aside, so what a caller reading the result of the work sees is
   * what is asserted.
   *
   * @throws Exception if a task fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testFutureCarriesBackTheRestoredOuterUserName() throws Exception {
    final UserGroupInformation outer =
        UserGroupInformation.createRemoteUser(OUTER_USER);
    final UserGroupInformation inner =
        UserGroupInformation.createRemoteUser(INNER_USER);
    final AtomicReference<String> inNestedScope = new AtomicReference<>();
    final AtomicReference<String> afterNestedScope = new AtomicReference<>();

    outer.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        ExecutorService pool = register(HadoopExecutors.newFixedThreadPool(1));
        afterNestedScope.set(awaitQuietly(pool.submit(new Callable<String>() {
          @Override
          public String call() throws Exception {
            inNestedScope.set(
                inner.doAs(new PrivilegedExceptionAction<String>() {
                  @Override
                  public String run() throws Exception {
                    return UserGroupInformation.getCurrentUser().getUserName();
                  }
                }));
            return UserGroupInformation.getCurrentUser().getUserName();
          }
        })));
        return null;
      }
    });

    assertEquals(INNER_USER, inNestedScope.get(),
        "the name reported from inside the nested scope was not the inner "
            + "user's");
    assertEquals(OUTER_USER, afterNestedScope.get(),
        "the name a pooled task reported once it had left a nested scope was "
            + "not the submitter's");
  }
}
