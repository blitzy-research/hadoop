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
 * Carries the current JAAS subject from the thread that submits a task to the
 * pooled thread that runs it.
 * <p>
 * Java copied the current subject into every thread it created up to and
 * including Java 21. Java 22 and 23 narrowed that to a special case, and from
 * Java 24 onwards the runtime never carries a subject across a thread boundary
 * on its own. A pool worker therefore observes no subject at all, even when the
 * thread that submitted the work to that pool was running as an authenticated
 * user. Nothing reports the loss, because such code compiles, throws nothing
 * and logs nothing; the missing identity only surfaces later, as a denied
 * authorization, an unauthenticated audit record, or a failure raised far from
 * its cause.
 * <p>
 * {@link #wrap(Runnable)} and {@link #wrap(Callable)} close that gap. Each one
 * reads the subject of the thread that calls it and returns a task that
 * re-establishes that subject inside the worker, for exactly the duration of
 * the original task. The caller of {@code wrap} is the thread submitting the
 * task, so the subject is captured at submission rather than when a worker
 * thread is created. That distinction is what makes the result correct for a
 * pool that reuses its threads: each task runs under the identity of its own
 * submitter, and never under the identity of whichever submitter happened to
 * cause the worker to be created.
 * <p>
 * Both overloads return the task they were given, unchanged, when the running
 * JVM still propagates the subject by itself, as reported by
 * {@link SubjectUtil#THREAD_INHERITS_SUBJECT}, and also when the calling thread
 * carries no subject to propagate. The second case is a matter of correctness
 * rather than of cost: re-establishing a subject that is absent would bind the
 * absence itself, hiding a subject that an enclosing scope had established.
 * <p>
 * Wrapping belongs at a single point per executor, so that no call site has to
 * know about any of this. Code that reports on a queued task, or otherwise
 * inspects one instead of running it, calls {@link #unwrap(Runnable)} first to
 * recover the task as it was submitted.
 * <p>
 * This class covers a task handed to a pool. {@link SubjectInheritingThread}
 * covers a thread that is created and started directly.
 */
@InterfaceAudience.Private
public final class SubjectPreservingTasks {

  /**
   * Not instantiable: this class only holds static methods.
   */
  private SubjectPreservingTasks() {
  }

  /**
   * Returns a task that runs {@code task} under the subject of the calling
   * thread.
   * <p>
   * The subject is read here, on the submitting thread, so the returned task
   * carries the identity of whoever submitted it rather than the identity that
   * happened to be current when the worker thread was created.
   * <p>
   * The argument is returned unchanged when the running JVM propagates the
   * subject by itself, when the calling thread carries no subject, and when
   * {@code task} is {@code null}. A {@code null} task is passed straight
   * through so that the executor it is handed to still rejects it exactly as it
   * would have.
   *
   * @param task the task to run under the current subject; may be {@code null}
   * @return a subject-preserving task, or {@code task} itself when there is no
   *         subject to carry
   */
  public static Runnable wrap(Runnable task) {
    if (SubjectUtil.THREAD_INHERITS_SUBJECT || task == null) {
      return task;
    }
    Subject subject = SubjectUtil.current();
    if (subject == null) {
      return task;
    }
    return new SubjectPreservingRunnable(task, subject);
  }

  /**
   * Returns a task that calls {@code task} under the subject of the calling
   * thread.
   * <p>
   * This is the {@link Callable} counterpart of {@link #wrap(Runnable)} and
   * decides whether to wrap on exactly the same terms, including returning the
   * argument unchanged when there is no subject to carry.
   * <p>
   * An exception thrown by {@code task} propagates with its own type, whether
   * it is checked or unchecked, so a caller that later reads the result through
   * a future observes the exception the task itself threw.
   *
   * @param <T> the result type of the task
   * @param task the task to call under the current subject; may be
   *             {@code null}
   * @return a subject-preserving task, or {@code task} itself when there is no
   *         subject to carry
   */
  public static <T> Callable<T> wrap(Callable<T> task) {
    if (SubjectUtil.THREAD_INHERITS_SUBJECT || task == null) {
      return task;
    }
    Subject subject = SubjectUtil.current();
    if (subject == null) {
      return task;
    }
    return new SubjectPreservingCallable<>(task, subject);
  }

  /**
   * Returns the task that {@link #wrap(Runnable)} was given, when {@code task}
   * is one of the tasks it returned.
   * <p>
   * Anything else, {@code null} included, is returned as it is. This lets code
   * that inspects a task instead of running it see the task as it was
   * submitted, which matters because a wrapper is neither a future nor of the
   * class the submitter passed in.
   * <p>
   * Exactly one layer is removed. Wrapping is applied at a single point per
   * executor, so one layer is all there is to remove, and stopping after one
   * leaves a mistaken second layer visible instead of concealing it.
   *
   * @param task the task to unwrap; may be {@code null}
   * @return the wrapped task, or {@code task} itself when it is not a wrapper
   */
  public static Runnable unwrap(Runnable task) {
    if (task instanceof SubjectPreservingRunnable) {
      return ((SubjectPreservingRunnable) task).delegate;
    }
    return task;
  }

  /**
   * Runs a delegate under the subject captured when this wrapper was built.
   */
  private static final class SubjectPreservingRunnable implements Runnable {

    /** The task as it was submitted. */
    private final Runnable delegate;

    /** The subject of the thread that submitted the task. */
    private final Subject subject;

    /**
     * Records the task and the subject to run it under.
     *
     * @param delegate the task as it was submitted
     * @param subject the subject read from the submitting thread
     */
    SubjectPreservingRunnable(Runnable delegate, Subject subject) {
      this.delegate = delegate;
      this.subject = subject;
    }

    /**
     * Runs the delegate under the captured subject.
     * <p>
     * The subject is applied with {@code doAs} rather than {@code callAs}
     * because only {@code doAs} re-throws the exception the delegate threw,
     * instead of one wrapping it. That is what a future, and a pool's own
     * reporting of an uncaught failure, go on to observe. The overload used
     * here declares no checked exception, so nothing has to be caught.
     */
    @Override
    public void run() {
      SubjectUtil.doAs(subject, (PrivilegedAction<Void>) () -> {
        delegate.run();
        return null;
      });
    }
  }

  /**
   * Calls a delegate under the subject captured when this wrapper was built.
   *
   * @param <T> the result type of the delegate
   */
  private static final class SubjectPreservingCallable<T> implements Callable<T> {

    /** The task as it was submitted. */
    private final Callable<T> delegate;

    /** The subject of the thread that submitted the task. */
    private final Subject subject;

    /**
     * Records the task and the subject to call it under.
     *
     * @param delegate the task as it was submitted
     * @param subject the subject read from the submitting thread
     */
    SubjectPreservingCallable(Callable<T> delegate, Subject subject) {
      this.delegate = delegate;
      this.subject = subject;
    }

    /**
     * Calls the delegate under the captured subject.
     * <p>
     * As in the {@link Runnable} wrapper, the subject is applied with
     * {@code doAs} so that the delegate's own exception is the one that
     * propagates. A runtime exception never reaches the handler below, because
     * that overload re-throws one directly; only a checked exception arrives
     * wrapped, and re-throwing its cause restores the type the delegate threw.
     *
     * @return the result of the delegate
     * @throws Exception the exception thrown by the delegate
     */
    @Override
    public T call() throws Exception {
      try {
        return SubjectUtil.doAs(subject, (PrivilegedExceptionAction<T>) delegate::call);
      } catch (PrivilegedActionException e) {
        Exception cause = e.getException();
        throw cause != null ? cause : e;
      }
    }
  }
}
