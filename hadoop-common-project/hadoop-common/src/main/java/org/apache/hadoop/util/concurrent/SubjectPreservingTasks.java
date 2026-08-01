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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import javax.security.auth.Subject;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.security.authentication.util.SubjectUtil;

/**
 * Carries the current JAAS subject from the thread that submits a task to the
 * pooled thread that runs it.
 * <p>
 * Up to and including Java 21 the runtime copied the current subject into every
 * thread it created, from Java 22 it does so only in a special case, and from
 * Java 24 never. A pool worker therefore observes no subject even when the
 * thread that submitted the work was running as an authenticated user, and
 * nothing reports the loss: such code compiles, throws nothing and logs
 * nothing, so the missing identity surfaces later as a denied authorization or
 * an unauthenticated audit record.
 * <p>
 * {@link #wrap(Runnable)} and {@link #wrap(Callable)} read the subject of the
 * thread that calls them and return a task that re-establishes it inside the
 * worker for the duration of that task. Because {@code wrap} runs on the
 * submitting thread, the subject is captured at submission rather than at
 * worker creation, so a pool that reuses its threads still runs every task
 * under the identity of its own submitter.
 * <p>
 * A worker is longer-lived than any one task it runs, and it can hold an
 * identity of its own: up to and including Java 21 a new thread starts out with
 * the identity of its creator, and on the runtimes that carry nothing across a
 * thread boundary {@link SubjectInheritingThread}, which several of Hadoop's
 * pools use for their workers, holds the subject of whoever created it for as
 * long as the worker lives. Left to that alone, every task after the first would
 * run as the first submitter. The identity to run under is therefore read at
 * every submission on every runtime, and an absent identity is re-established
 * just as a present one is, so that a task submitted with no subject runs with
 * none rather than under whatever its worker was left holding. Running one
 * submitter's work as another user is worse than running it as none: it decides
 * authorization and it is what an audit record names. Neither
 * {@link SubjectUtil#THREAD_INHERITS_SUBJECT} nor an absent subject is grounds
 * for leaving a pooled task alone; the one task handed straight back is
 * {@code null}, so that the executor it was destined for still rejects it
 * exactly as it would have.
 * <p>
 * A task that already carries an identity from one of these methods is handed
 * back as it is, so that preparing it happens once and once only. A rejected
 * task an executor is offered a second time, as
 * {@link java.util.concurrent.ThreadPoolExecutor.DiscardOldestPolicy} offers it,
 * therefore keeps the single layer that {@link #unwrap(Runnable)} takes back
 * off, and keeps the identity of the submission it belongs to.
 * <p>
 * Wrapping belongs at a single point per executor, so that no call site need
 * know of any of this, and code that inspects a task instead of running it calls
 * {@link #unwrap(Runnable)} first; a prepared task also reports the text of the
 * task it was given, so anything that names one without unwrapping it still
 * names the task as submitted. This class covers a task handed to a pool;
 * {@link SubjectInheritingThread} covers a thread created and started directly.
 */
@InterfaceAudience.Private
public final class SubjectPreservingTasks {

  private SubjectPreservingTasks() {
  }

  /**
   * Returns a task that runs {@code task} under the subject of the calling
   * thread, and under no subject at all when the calling thread carries none.
   * <p>
   * The subject is read here, on the submitting thread, so the returned task
   * carries the identity of whoever submitted it rather than the identity that
   * happened to be current when the worker thread was created. A worker that
   * has an identity of its own keeps it across every task it ever runs, so the
   * absence of a subject on the submitting thread is carried over just as
   * deliberately as a subject is: the returned task runs under no identity,
   * rather than under the identity of a worker it merely happens to land on.
   * <p>
   * The argument is returned unchanged in exactly two cases: when it is
   * {@code null}, so that the executor it is handed to still rejects it exactly
   * as it would have; and when it already came from this method, so that a task
   * an executor is offered again after being rejected ends up prepared once
   * rather than twice.
   *
   * @param task the task to run under the current subject; may be {@code null}
   * @return a task that establishes the calling thread's identity, or
   *         {@code task} itself when it is {@code null} or already establishes
   *         one
   */
  public static Runnable wrap(Runnable task) {
    if (task == null || task instanceof SubjectPreservingRunnable) {
      return task;
    }
    return new SubjectPreservingRunnable(task, SubjectUtil.current());
  }

  /**
   * Returns a task that calls {@code task} under the subject of the calling
   * thread, and under no subject at all when the calling thread carries none.
   * <p>
   * This is the {@link Callable} counterpart of {@link #wrap(Runnable)} and
   * reads the identity to run under on exactly the same terms, returning the
   * argument unchanged only when it is {@code null} or already came from this
   * method.
   * <p>
   * An exception thrown by {@code task} propagates with its own type, whether
   * it is checked or unchecked, so a caller that later reads the result through
   * a future observes the exception the task itself threw.
   *
   * @param <T> the result type of the task
   * @param task the task to call under the current subject; may be
   *             {@code null}
   * @return a task that establishes the calling thread's identity, or
   *         {@code task} itself when it is {@code null} or already establishes
   *         one
   */
  public static <T> Callable<T> wrap(Callable<T> task) {
    if (task == null || task instanceof SubjectPreservingCallable) {
      return task;
    }
    return new SubjectPreservingCallable<>(task, SubjectUtil.current());
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
   * executor and {@link #wrap(Runnable)} leaves an already-wrapped task alone,
   * so one layer is all there is to remove, and stopping after one leaves a
   * mistaken second layer visible instead of concealing it.
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
   * Returns the given tasks as they were submitted.
   * <p>
   * This is {@link #unwrap(Runnable)} over a list, for an executor that hands
   * its queued tasks back to a caller: the caller submitted those tasks and is
   * owed them, not the form the pool queued them in.
   *
   * @param tasks the tasks to unwrap, in the order they were given
   * @return a new list holding each task as it was submitted, in the same order
   */
  static List<Runnable> unwrapAll(List<Runnable> tasks) {
    List<Runnable> unwrapped = new ArrayList<>(tasks.size());
    for (Runnable task : tasks) {
      unwrapped.add(unwrap(task));
    }
    return unwrapped;
  }

  /**
   * Runs its delegate under the subject captured on the submitting thread.
   */
  private static final class SubjectPreservingRunnable implements Runnable {

    private final Runnable delegate;
    /** The submitting thread's subject; {@code null} is a state to establish. */
    private final Subject subject;

    SubjectPreservingRunnable(Runnable delegate, Subject subject) {
      this.delegate = delegate;
      this.subject = subject;
    }

    @Override
    public void run() {
      // doAs rather than callAs: only doAs re-throws the exception the delegate
      // threw instead of one wrapping it, and that is what a future and a
      // pool's own reporting of an uncaught failure go on to observe. This
      // overload declares no checked exception, so nothing has to be caught.
      SubjectUtil.doAs(subject, (PrivilegedAction<Void>) () -> {
        delegate.run();
        return null;
      });
    }

    /**
     * Returns the text of the task as it was submitted.
     * <p>
     * Some of what describes a queued task reaches it without the chance to
     * unwrap it first: a pool that turns a submission away names the task in
     * the exception it throws. Reporting the delegate's own text keeps such a
     * message as it was.
     *
     * @return the result of the submitted task's own {@code toString}
     */
    @Override
    public String toString() {
      return delegate.toString();
    }
  }

  /**
   * Calls its delegate under the subject captured on the submitting thread.
   *
   * @param <T> the result type of the delegate
   */
  private static final class SubjectPreservingCallable<T> implements Callable<T> {

    private final Callable<T> delegate;
    /** The submitting thread's subject; {@code null} is a state to establish. */
    private final Subject subject;

    SubjectPreservingCallable(Callable<T> delegate, Subject subject) {
      this.delegate = delegate;
      this.subject = subject;
    }

    @Override
    public T call() throws Exception {
      try {
        // doAs rather than callAs, so that the delegate's own exception is the
        // one that propagates. A runtime exception never reaches the handler
        // below, because this overload re-throws one directly; only a checked
        // exception arrives wrapped, and its cause restores the type thrown.
        return SubjectUtil.doAs(subject, (PrivilegedExceptionAction<T>) delegate::call);
      } catch (PrivilegedActionException e) {
        Exception cause = e.getException();
        throw cause != null ? cause : e;
      }
    }

    /**
     * Returns the text of the task as it was submitted, for the same reason the
     * {@link Runnable} wrapper does.
     *
     * @return the result of the submitted task's own {@code toString}
     */
    @Override
    public String toString() {
      return delegate.toString();
    }
  }
}
