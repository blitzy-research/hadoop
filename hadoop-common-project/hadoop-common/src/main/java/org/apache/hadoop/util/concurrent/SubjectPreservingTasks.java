/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.util.concurrent;

import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.Callable;
import javax.security.auth.Subject;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.security.authentication.util.SubjectUtil;

/**
 * Carries the JAAS subject of the thread that submits a task to the pooled
 * thread that runs it, on a runtime that no longer carries it across a thread
 * boundary itself.
 * <p>
 * Up to and including Java 21 a newly created thread was given the subject of
 * the thread that created it, and Hadoop relied on that. From Java 24 it is
 * given none, so work handed to a pool inside a {@code doAs} scope reaches its
 * worker with no identity at all -- and nothing reports it: such code compiles,
 * throws nothing and logs nothing, and the loss surfaces later as a denied
 * authorization, an audit record naming the wrong user, or a
 * {@link NullPointerException} far from its cause.
 * <p>
 * What a pooled task runs under therefore depends on the runtime, and is stated
 * here per runtime rather than in the abstract:
 * <ul>
 *   <li>where {@link SubjectUtil#THREAD_INHERITS_SUBJECT} is {@code false} --
 *   Java 24 and later, JDK 25 among them -- {@link #wrap(Runnable)} and
 *   {@link #wrap(Callable)} read the subject on the thread that calls them and
 *   return a task that re-establishes it around the original. They are called
 *   on the submitting thread, so the subject is read at submission rather than
 *   at worker creation, and a pool that reuses its threads runs each task under
 *   the identity of that task's own submitter;</li>
 *   <li>where it is {@code true} -- Java 21 and earlier, JDK 17 among them --
 *   the runtime propagates the identity itself, both methods return their
 *   argument, and a task observes whatever its worker holds: the identity in
 *   force when that worker was created, as a worker built by
 *   {@link SubjectInheritingThread} takes one, or none where the worker was
 *   given none. Behaviour and cost are left exactly as that runtime already had
 *   them.</li>
 * </ul>
 * A task is handed back unchanged for three further reasons: the submitting
 * thread carries no subject, the task is {@code null}, or the task was already
 * prepared here. The first is a matter of correctness rather than economy --
 * the replacement API binds a {@code null} subject, so re-establishing an
 * absent identity would hide an identity in force where the task runs instead
 * of leaving the task with none. The second leaves an executor's own rejection
 * of a {@code null} task where it was, and the third keeps a task an executor
 * is offered a second time carrying the identity of the submission that was
 * rejected, behind the single layer {@link #unwrap(Runnable)} takes off.
 * <p>
 * Where an identity is re-established, nothing else about the task changes. It
 * is applied with {@code SubjectUtil.doAs} rather than the replacement API
 * directly, because only {@code doAs} unwraps the
 * {@link java.util.concurrent.CompletionException} that API is specified to
 * throw and re-throws the original cause: an exception a task throws keeps its
 * own type, and stays the cause a {@link java.util.concurrent.Future} reports
 * on its {@link java.util.concurrent.ExecutionException}.
 * <p>
 * Wrapping belongs at one point per executor, so that no call site need know of
 * any of this, and code that describes a task instead of running it calls
 * {@link #unwrap(Runnable)} first. This class covers a task handed to a pool;
 * {@link SubjectInheritingThread} covers a thread created and started directly.
 */
@InterfaceAudience.Private
public final class SubjectPreservingTasks {

  private SubjectPreservingTasks() {
  }

  /**
   * Prepares {@code task} so that it runs under the subject current on this
   * thread, wherever this class wraps at all.
   * <p>
   * The subject is read here, on the calling thread, and applied on whichever
   * thread runs the task. The argument is returned unchanged in the four cases
   * the class contract lists -- a runtime that propagates the subject itself, a
   * calling thread carrying no subject, a {@code null} task, and a task already
   * prepared here -- and such a task observes whatever its worker holds.
   *
   * @param task the task to run under the current subject; may be {@code null}
   * @return a task that establishes the calling thread's subject, or
   *         {@code task} itself where there is nothing to establish
   */
  public static Runnable wrap(Runnable task) {
    if (task == null
        || task instanceof SubjectPreservingRunnable
        || SubjectUtil.THREAD_INHERITS_SUBJECT) {
      return task;
    }
    Subject captured = SubjectUtil.current();
    if (captured == null) {
      return task;
    }
    return new SubjectPreservingRunnable(task, captured);
  }

  /**
   * The {@link Callable} counterpart of {@link #wrap(Runnable)}, on exactly the
   * same terms.
   * <p>
   * An exception thrown by {@code task} keeps its own type, checked or
   * unchecked, so a caller reading the result through a future is handed an
   * {@link java.util.concurrent.ExecutionException} whose cause is the exception
   * the task itself threw.
   *
   * @param <T> the result type of the task
   * @param task the task to call under the current subject; may be {@code null}
   * @return a task that establishes the calling thread's subject, or
   *         {@code task} itself where there is nothing to establish
   */
  public static <T> Callable<T> wrap(Callable<T> task) {
    if (task == null
        || task instanceof SubjectPreservingCallable
        || SubjectUtil.THREAD_INHERITS_SUBJECT) {
      return task;
    }
    Subject captured = SubjectUtil.current();
    if (captured == null) {
      return task;
    }
    return new SubjectPreservingCallable<>(task, captured);
  }

  /**
   * Returns the task {@link #wrap(Runnable)} was given, when {@code task} is one
   * it returned; anything else, {@code null} included, is returned as it is.
   * <p>
   * This lets code that describes a task instead of running it see the task as
   * it was submitted, which matters because a prepared task is neither a
   * {@link java.util.concurrent.Future} nor of the class the submitter passed
   * in. Exactly one layer is removed: wrapping is applied at one point per
   * executor and an already-prepared task is left alone, so one layer is all
   * there is, and stopping after one leaves a mistaken second layer visible
   * instead of concealing it.
   *
   * @param task the task to unwrap; may be {@code null}
   * @return the task that was prepared, or {@code task} itself when it was not
   *         prepared here
   */
  public static Runnable unwrap(Runnable task) {
    if (task instanceof SubjectPreservingRunnable) {
      return ((SubjectPreservingRunnable) task).delegate;
    }
    return task;
  }

  /**
   * Runs its delegate under the subject read on the submitting thread.
   */
  private static final class SubjectPreservingRunnable implements Runnable {

    private final Runnable delegate;
    private final Subject subject;

    SubjectPreservingRunnable(Runnable delegate, Subject subject) {
      this.delegate = delegate;
      this.subject = subject;
    }

    @Override
    public void run() {
      // doAs rather than callAs, so that the delegate's own exception is what
      // propagates and so becomes the cause a future reports on its
      // ExecutionException, and what a pool's own reporting of an uncaught
      // failure observes. This overload declares no checked exception, so
      // nothing has to be caught.
      SubjectUtil.doAs(subject, (PrivilegedAction<Void>) () -> {
        delegate.run();
        return null;
      });
    }
  }

  /**
   * Calls its delegate under the subject read on the submitting thread.
   *
   * @param <T> the result type of the delegate
   */
  private static final class SubjectPreservingCallable<T>
      implements Callable<T> {

    private final Callable<T> delegate;
    private final Subject subject;

    SubjectPreservingCallable(Callable<T> delegate, Subject subject) {
      this.delegate = delegate;
      this.subject = subject;
    }

    @Override
    public T call() throws Exception {
      try {
        // doAs rather than callAs, for the reason given above. A runtime
        // exception never reaches the handler below, because this overload
        // re-throws one directly; only a checked exception arrives wrapped, and
        // its cause restores the type thrown.
        return SubjectUtil.doAs(subject,
            (PrivilegedExceptionAction<T>) delegate::call);
      } catch (PrivilegedActionException e) {
        Exception cause = e.getException();
        throw cause != null ? cause : e;
      }
    }
  }
}
