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
 * Asserts that running as one user inside the scope of another behaves
 * correctly when a pooled task lies between the two.
 * <p>
 * {@link UserGroupInformation#doAs(PrivilegedExceptionAction)} establishes an
 * identity for the duration of one action, and code inside that action may
 * establish another for a shorter stretch still. Whichever scope is innermost
 * has to be the one that decides who the current user is, the scope around it
 * has to be exactly what is current again once the inner one ends, and neither
 * of those has to change merely because the work was handed to an executor on
 * the way. That crossing is where an identity is most easily lost: a pool
 * worker outlives the task it runs, so it belongs to no submitter in
 * particular, and work that reaches it without its submitter's identity runs
 * as whoever the worker was left holding, or as no one, without anything being
 * thrown or logged to say so.
 * <p>
 * These tests therefore watch the current user at every level of the nesting:
 * on the thread that establishes the outer identity, inside the pooled task,
 * inside the scope the task itself opens, and again after that scope has
 * ended. Each of those is asserted. The pool-free test does the same nesting
 * on a single thread, which tells a fault in the nesting itself apart from a
 * fault in the crossing.
 * <p>
 * Every assertion is about an observed identity and never about how that
 * identity was carried, so the same assertions hold on every runtime this
 * project supports: whether the runtime hands a new thread the identity of the
 * thread that created it or hands over nothing at all, work observes the user
 * it was submitted by. Each pooled hop uses an executor created for it whose
 * first submission is made from inside the scope being tested, so no worker of
 * it can predate that scope and no pool is shared between two identities. The
 * users are given names of their own that no account on the host can hold,
 * because a lost identity does not read as absent: the current user then falls
 * back to the user this process logged in as, and only a name that cannot be
 * that one tells the two apart. Every observation made on a worker reaches the
 * thread that asserts on it through a future or an atomic reference, and every
 * executor these tests create is shut down when the test ends.
 */
public class TestNestedSubjectPropagation {

  /** The user established by the outer scope of every nesting here. */
  private static final String OUTER_USER = "nested-outer-user";

  /** The user established by the scope nested within the outer one. */
  private static final String INNER_USER = "nested-inner-user";

  /** The user established by the innermost scope of the deepest nesting. */
  private static final String INNERMOST_USER = "nested-innermost-user";

  /** Bound on every wait, generous enough to survive a loaded build host. */
  private static final long TIMEOUT_SECONDS = 10;

  /** Every executor created by a test, shut down when the test ends. */
  private final List<ExecutorService> pools = new ArrayList<>();

  /**
   * Discards the user state an earlier test left behind and hands the user
   * machinery a configuration of its own.
   * <p>
   * Each test compares the user it establishes against the user this process
   * logged in as, so both are reset before every test rather than once for the
   * class: a process of its own is given to each test class, not to each test.
   */
  @BeforeEach
  public void setUpUgi() {
    Configuration conf = new Configuration();
    UserGroupInformation.reset();
    UserGroupInformation.setConfiguration(conf);
  }

  /**
   * Shuts down every executor a test created, then forgets the login user.
   * <p>
   * The executors go first, so that no task is still running by the time the
   * user state it reads is discarded, and the user is forgotten even if an
   * executor fails to stop, so that one such failure cannot leave the state
   * behind for the next test.
   *
   * @throws InterruptedException if this thread is interrupted while waiting
   */
  @AfterEach
  public void shutDownPoolsAndForgetLoginUser() throws InterruptedException {
    List<ExecutorService> registered = new ArrayList<>(pools);
    pools.clear();
    for (ExecutorService pool : registered) {
      pool.shutdownNow();
    }
    try {
      for (ExecutorService pool : registered) {
        assertTrue(pool.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS),
            "an executor created by this test did not terminate");
      }
    } finally {
      UserGroupInformation.setLoginUser(null);
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
   * Waits for a submitted task and returns what it produced.
   * <p>
   * Called for its effect alone, this waits for the task and lets any failure
   * of it reach the caller. A failure is reported as the task's own error or
   * unchecked exception wherever it is one, so that a failed assertion made on
   * a worker reads as that assertion and not as something wrapping it, and a
   * wait that does not finish is reported as itself rather than as a task
   * failure.
   * <p>
   * Nothing a caller of this method can be handed is a checked exception. That
   * matters because this is called from inside a scope established by
   * {@link UserGroupInformation#doAs(PrivilegedExceptionAction)}, which passes
   * an error or an unchecked exception straight out but describes anything else
   * in terms of a wrapper, and a genuine failure is far easier to read without
   * one.
   *
   * @param <T> the task's result type
   * @param task the submitted task to wait for
   * @return whatever the task produced
   */
  private static <T> T awaitResult(Future<T> task) {
    try {
      return task.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(
          "interrupted while waiting for a pooled task", e);
    } catch (TimeoutException e) {
      throw new RuntimeException("a pooled task did not finish within "
          + TIMEOUT_SECONDS + " seconds", e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof Error) {
        throw (Error) cause;
      }
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      throw new RuntimeException("a pooled task failed",
          cause != null ? cause : e);
    }
  }

  /**
   * A pooled task runs as the user that submitted it, a scope opened inside
   * that task runs as its own user, and the submitting user is current again
   * as soon as that scope ends.
   * <p>
   * This is the whole of the nesting in one test, watched at each of its four
   * levels: on the thread that establishes the outer user, inside the task
   * that thread submits, inside the scope the task opens for the inner user,
   * and inside the task again once that scope has ended. The pool is created
   * inside the outer scope and its first submission is made there, so no
   * worker of it exists before the scope it has to observe.
   * <p>
   * Each level is asserted twice: once against the user object, whose equality
   * holds only for the very identity that was established, and once against
   * the user's name, so that a level which lost the identity names the user
   * this process logged in as instead of failing on an object that is hard to
   * read.
   *
   * @throws Exception if a task fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testNestedDoAsAroundPooledTaskObservesEveryLevel()
      throws Exception {
    final UserGroupInformation outer =
        UserGroupInformation.createRemoteUser(OUTER_USER);
    final UserGroupInformation inner =
        UserGroupInformation.createRemoteUser(INNER_USER);
    final AtomicReference<UserGroupInformation> beforeInner =
        new AtomicReference<>();
    final AtomicReference<UserGroupInformation> insideInner =
        new AtomicReference<>();
    final AtomicReference<UserGroupInformation> afterInner =
        new AtomicReference<>();

    outer.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        assertEquals(outer, UserGroupInformation.getCurrentUser(),
            "level 1: the outer user must be current on the thread that "
                + "established it");
        ExecutorService pool =
            register(HadoopExecutors.newFixedThreadPool(1));
        Future<Void> submitted = pool.submit(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            beforeInner.set(UserGroupInformation.getCurrentUser());
            inner.doAs(new PrivilegedExceptionAction<Void>() {
              @Override
              public Void run() throws Exception {
                insideInner.set(UserGroupInformation.getCurrentUser());
                return null;
              }
            });
            afterInner.set(UserGroupInformation.getCurrentUser());
            return null;
          }
        });
        awaitResult(submitted);
        return null;
      }
    });

    assertEquals(outer, beforeInner.get(),
        "level 2: the submitting user must be current inside the pooled task");
    assertEquals(OUTER_USER, beforeInner.get().getUserName(),
        "level 2: the pooled task ran as the wrong user");
    assertEquals(inner, insideInner.get(),
        "level 3: the inner user must be current inside the inner scope");
    assertEquals(INNER_USER, insideInner.get().getUserName(),
        "level 3: the inner scope ran as the wrong user");
    assertEquals(outer, afterInner.get(),
        "level 4: the outer user must be current again once the inner scope "
            + "has ended");
    assertEquals(OUTER_USER, afterInner.get().getUserName(),
        "level 4: the wrong user was current once the inner scope had ended");
    assertNotEquals(outer, UserGroupInformation.getCurrentUser(),
        "the outer user must no longer be current once its own scope has "
            + "ended");
  }

  /**
   * One scope nested inside another on a single thread runs as the inner user
   * and leaves the outer user current again afterwards.
   * <p>
   * No executor takes part, so this is the nesting on its own. It is what
   * separates a fault in the nesting from a fault in the crossing to a worker:
   * should this fail as well, the nesting itself is at fault rather than
   * anything to do with a pool.
   * <p>
   * Each level is asserted where it holds, inside the scope that establishes
   * it, which a scope on this thread allows. A failed assertion there reaches
   * this method as itself, because an error raised by an action passes out of
   * the scope it was raised in unchanged.
   *
   * @throws Exception if establishing either user fails
   */
  @Test
  @Timeout(value = 30)
  public void testNestedDoAsOnOneThreadRestoresTheOuterUser()
      throws Exception {
    final UserGroupInformation outer =
        UserGroupInformation.createRemoteUser(OUTER_USER);
    final UserGroupInformation inner =
        UserGroupInformation.createRemoteUser(INNER_USER);

    outer.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        assertEquals(outer, UserGroupInformation.getCurrentUser(),
            "the outer user must be current inside the outer scope");
        assertEquals(OUTER_USER,
            UserGroupInformation.getCurrentUser().getUserName(),
            "the outer scope ran as the wrong user");
        inner.doAs(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            assertEquals(inner, UserGroupInformation.getCurrentUser(),
                "the inner user must be current inside the inner scope");
            assertEquals(INNER_USER,
                UserGroupInformation.getCurrentUser().getUserName(),
                "the inner scope ran as the wrong user");
            return null;
          }
        });
        assertEquals(outer, UserGroupInformation.getCurrentUser(),
            "the outer user must be current again once the inner scope has "
                + "ended");
        assertEquals(OUTER_USER,
            UserGroupInformation.getCurrentUser().getUserName(),
            "the wrong user was current once the inner scope had ended");
        return null;
      }
    });

    assertNotEquals(outer, UserGroupInformation.getCurrentUser(),
        "the outer user must no longer be current once its own scope has "
            + "ended");
  }

  /**
   * A task submitted from the innermost scope runs as that scope's user, not
   * as the user of the scope around it.
   * <p>
   * This is the nesting of the other test turned around: the inner scope is
   * opened before the submission rather than inside the task, so the identity
   * the task has to observe is the innermost one current when it was handed
   * over. Together the two show that what a task observes is the identity that
   * submitted it, whichever scope that submission was made from.
   * <p>
   * The pool is created inside the inner scope and its first submission is
   * made there, so it belongs to that scope alone and no worker of it can
   * carry the outer identity. That the outer user is current again after the
   * inner scope ends is asserted as well, so that opening a scope for a
   * submission is shown to leave nothing behind.
   *
   * @throws Exception if a task fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testInnermostScopeAtSubmissionReachesThePooledTask()
      throws Exception {
    final UserGroupInformation outer =
        UserGroupInformation.createRemoteUser(OUTER_USER);
    final UserGroupInformation inner =
        UserGroupInformation.createRemoteUser(INNER_USER);
    final AtomicReference<UserGroupInformation> inTask =
        new AtomicReference<>();
    final AtomicReference<UserGroupInformation> afterInner =
        new AtomicReference<>();

    outer.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        inner.doAs(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            ExecutorService pool =
                register(HadoopExecutors.newFixedThreadPool(1));
            Future<Void> submitted = pool.submit(new Callable<Void>() {
              @Override
              public Void call() throws Exception {
                inTask.set(UserGroupInformation.getCurrentUser());
                return null;
              }
            });
            awaitResult(submitted);
            return null;
          }
        });
        afterInner.set(UserGroupInformation.getCurrentUser());
        return null;
      }
    });

    assertEquals(inner, inTask.get(),
        "the user of the scope the task was submitted from must be current "
            + "inside it");
    assertEquals(INNER_USER, inTask.get().getUserName(),
        "the pooled task ran as the wrong user");
    assertEquals(outer, afterInner.get(),
        "the outer user must be current again once the scope the task was "
            + "submitted from has ended");
    assertEquals(OUTER_USER, afterInner.get().getUserName(),
        "the wrong user was current once the inner scope had ended");
  }

  /**
   * Scopes nested three deep inside a pooled task run as their own users and
   * unwind to exactly the user each was opened within.
   * <p>
   * Two scopes inside a task, one within the other, are what show that the
   * restoring of an identity is not merely a return to whatever the task
   * started with: the middle scope has to be current again when the innermost
   * one ends, and only then the submitting user when the middle one does. A
   * single scope inside a task cannot tell those two apart.
   * <p>
   * All five observations are recorded on the worker and asserted here, in the
   * order the task passed through them.
   *
   * @throws Exception if a task fails or a wait times out
   */
  @Test
  @Timeout(value = 30)
  public void testThreeLevelNestingInsidePooledTaskUnwindsExactly()
      throws Exception {
    final UserGroupInformation outer =
        UserGroupInformation.createRemoteUser(OUTER_USER);
    final UserGroupInformation inner =
        UserGroupInformation.createRemoteUser(INNER_USER);
    final UserGroupInformation innermost =
        UserGroupInformation.createRemoteUser(INNERMOST_USER);
    final AtomicReference<UserGroupInformation> atOuter =
        new AtomicReference<>();
    final AtomicReference<UserGroupInformation> atInner =
        new AtomicReference<>();
    final AtomicReference<UserGroupInformation> atInnermost =
        new AtomicReference<>();
    final AtomicReference<UserGroupInformation> backAtInner =
        new AtomicReference<>();
    final AtomicReference<UserGroupInformation> backAtOuter =
        new AtomicReference<>();

    outer.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        ExecutorService pool =
            register(HadoopExecutors.newFixedThreadPool(1));
        Future<Void> submitted = pool.submit(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            atOuter.set(UserGroupInformation.getCurrentUser());
            inner.doAs(new PrivilegedExceptionAction<Void>() {
              @Override
              public Void run() throws Exception {
                atInner.set(UserGroupInformation.getCurrentUser());
                innermost.doAs(new PrivilegedExceptionAction<Void>() {
                  @Override
                  public Void run() throws Exception {
                    atInnermost.set(UserGroupInformation.getCurrentUser());
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
        });
        awaitResult(submitted);
        return null;
      }
    });

    assertEquals(outer, atOuter.get(),
        "the submitting user must be current where the task begins");
    assertEquals(OUTER_USER, atOuter.get().getUserName(),
        "the pooled task began as the wrong user");
    assertEquals(inner, atInner.get(),
        "the inner user must be current inside the inner scope");
    assertEquals(INNER_USER, atInner.get().getUserName(),
        "the inner scope ran as the wrong user");
    assertEquals(innermost, atInnermost.get(),
        "the innermost user must be current inside the innermost scope");
    assertEquals(INNERMOST_USER, atInnermost.get().getUserName(),
        "the innermost scope ran as the wrong user");
    assertEquals(inner, backAtInner.get(),
        "the inner user must be current again once the innermost scope has "
            + "ended");
    assertEquals(INNER_USER, backAtInner.get().getUserName(),
        "the wrong user was current once the innermost scope had ended");
    assertEquals(outer, backAtOuter.get(),
        "the submitting user must be current again once the inner scope has "
            + "ended");
    assertEquals(OUTER_USER, backAtOuter.get().getUserName(),
        "the wrong user was current once the inner scope had ended");
  }

  /**
   * A pooled task reports the name of the user that submitted it through its
   * own future.
   * <p>
   * The submission is made inside the scope of that user and the result is
   * read after the scope has ended, so the identity is shown to have travelled
   * with the task rather than with the stack that submitted it. The result
   * crosses the thread boundary as the future's own value, which needs no
   * shared state to be published and which reports the user by name, the form
   * a lost identity is plainest in.
   *
   * @throws Exception if establishing the user fails
   */
  @Test
  @Timeout(value = 30)
  public void testPooledTaskReportsSubmittingUserThroughItsFuture()
      throws Exception {
    final UserGroupInformation outer =
        UserGroupInformation.createRemoteUser(OUTER_USER);
    final AtomicReference<Future<String>> submitted = new AtomicReference<>();

    outer.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() {
        ExecutorService pool =
            register(HadoopExecutors.newFixedThreadPool(1));
        submitted.set(pool.submit(new Callable<String>() {
          @Override
          public String call() throws Exception {
            return UserGroupInformation.getCurrentUser().getUserName();
          }
        }));
        return null;
      }
    });

    assertEquals(OUTER_USER, awaitResult(submitted.get()),
        "the pooled task must report the user that submitted it");
    assertNotEquals(outer, UserGroupInformation.getCurrentUser(),
        "the submitting user must no longer be current once its own scope "
            + "has ended");
  }
}
