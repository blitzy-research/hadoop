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
 * Up to and including Java 21 the runtime copied the current subject into every
 * thread it created, from Java 22 it does so only in a special case, and from
 * Java 24 never. A pooled task therefore stops seeing the identity of the
 * thread that submitted it: it sees no subject at all where its worker holds
 * none, and where the worker does hold one of its own it sees that one instead,
 * which belongs to some earlier point in the worker's life rather than to this
 * submission. Nothing reports either outcome, since such code compiles, throws
 * nothing and logs nothing, so the wrong identity surfaces later as a denied
 * authorization or as an audit record naming the wrong user.
 * <p>
 * {@link #wrap(Runnable)} and {@link #wrap(Callable)} read the subject of the
 * thread that calls them and return a task that re-establishes it inside the
 * worker for the duration of that task. Because {@code wrap} runs on the
 * submitting thread, the subject is captured at submission rather than at
 * worker creation, so a pool that reuses its threads still runs every task
 * under the identity of its own submitter. That is the point of reading it at
 * every submission: a worker outlives any one task it runs, so an identity read
 * once per worker would run every later submission as the first submitter, and
 * running one submitter's work as another user decides authorization and is
 * what an audit record names.
 * <p>
 * A worker is longer-lived than any one task it runs, so reading the identity
 * when a worker is created would bind whoever first caused that worker to
 * exist, and every later task would run as that submitter. That is worse than
 * running with no identity at all: it decides authorization and it is what an
 * audit record names. Reading the identity at each submission is what avoids
 * it.
 * <p>
 * A task is handed straight back, unchanged and with nothing allocated, in
 * four cases. When {@link SubjectUtil#THREAD_INHERITS_SUBJECT} is set the
 * runtime carries the subject across a thread boundary by itself, so there is
 * nothing to add and these methods are an identity function: on those runtimes
 * behaviour is exactly what it was. When the calling thread carries no subject,
 * establishing one that is absent would hide the subject an enclosing scope had
 * established, so a task submitted without an identity is left alone rather
 * than given an empty one. When the task itself is {@code null} it is
 * passed on, so that the executor it was destined for rejects it exactly as it
 * always has. And when the task already carries an identity from one of these
 * methods it is left as it is, so that a rejected task an executor is offered a
 * second time, as
 * {@link java.util.concurrent.ThreadPoolExecutor.DiscardOldestPolicy} offers
 * it, keeps the single layer that {@link #unwrap(Runnable)} takes back off.
 * <p>
 * Wrapping belongs at a single point per executor, so that no call site need
 * know of any of this, and code that inspects a task instead of running it
 * calls {@link #unwrap(Runnable)} first. This class covers a task handed to a
 * pool; {@link SubjectInheritingThread} covers a thread created and started
 * directly.
 */
@InterfaceAudience.Private
public final class SubjectPreservingTasks {

  private SubjectPreservingTasks() {
  }

  /**
   * Returns a task that runs {@code task} under the subject of the calling
   * thread.
   * <p>
   * The subject is read here, on the submitting thread, so the returned task
   * carries the identity of whoever submitted it rather than the identity that
   * happened to be current when the worker thread was created. A pool that
   * reuses its workers therefore runs every task under the identity of its own
   * submitter.
   * <p>
   * The argument is returned unchanged, with nothing allocated, when
   * {@link SubjectUtil#THREAD_INHERITS_SUBJECT} is set, because the runtime
   * carries the subject across a thread boundary by itself; when the calling
   * thread carries no subject, because establishing one that is absent would
   * hide the subject of an enclosing scope; when {@code task} is {@code null},
   * so that the executor it is handed to still rejects it exactly as it would
   * have; and when {@code task} already came from this method, so that a task
   * an executor is offered again after rejecting it ends up prepared once
   * rather than twice.
   *
   * @param task the task to run under the current subject; may be {@code null}
   * @return a task that establishes the calling thread's identity, or
   *         {@code task} itself when there is no identity to establish
   */
  public static Runnable wrap(Runnable task) {
    if (SubjectUtil.THREAD_INHERITS_SUBJECT || task == null
        || task instanceof SubjectPreservingRunnable) {
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
   * reads the identity to run under on exactly the same terms, returning the
   * argument unchanged in exactly the same four cases.
   * <p>
   * An exception thrown by {@code task} keeps its own type, whether it is
   * checked or unchecked, because establishing the identity contributes no
   * exception of its own. A caller that later reads the result through a future
   * is therefore handed an
   * {@link java.util.concurrent.ExecutionException} whose cause is the exception
   * the task itself threw.
   *
   * @param <T> the result type of the task
   * @param task the task to call under the current subject; may be
   *             {@code null}
   * @return a task that establishes the calling thread's identity, or
   *         {@code task} itself when there is no identity to establish
   */
  public static <T> Callable<T> wrap(Callable<T> task) {
    if (SubjectUtil.THREAD_INHERITS_SUBJECT || task == null
        || task instanceof SubjectPreservingCallable) {
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
   * executor and {@link #wrap(Runnable)} leaves an already-prepared task alone,
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
   * Runs its delegate under the subject captured on the submitting thread.
   */
  private static final class SubjectPreservingRunnable implements Runnable {

    private final Runnable delegate;
    /** The subject captured on the submitting thread; never {@code null}. */
    private final Subject subject;

    SubjectPreservingRunnable(Runnable delegate, Subject subject) {
      this.delegate = delegate;
      this.subject = subject;
    }

    @Override
    public void run() {
      // doAs rather than callAs: callAs reports a failure of the delegate as the
      // cause of an exception of its own, whereas doAs re-throws what the
      // delegate threw. That leaves the delegate's own exception as the cause a
      // future reports on its ExecutionException, and as what a pool's own
      // reporting of an uncaught failure observes. This overload declares no
      // checked exception, so nothing has to be caught.
      SubjectUtil.doAs(subject, (PrivilegedAction<Void>) () -> {
        delegate.run();
        return null;
      });
    }
  }

  /**
   * Calls its delegate under the subject captured on the submitting thread.
   *
   * @param <T> the result type of the delegate
   */
  private static final class SubjectPreservingCallable<T> implements Callable<T> {

    private final Callable<T> delegate;
    /** The subject captured on the submitting thread; never {@code null}. */
    private final Subject subject;

    SubjectPreservingCallable(Callable<T> delegate, Subject subject) {
      this.delegate = delegate;
      this.subject = subject;
    }

    @Override
    public T call() throws Exception {
      try {
        // doAs rather than callAs, so that the delegate's own exception is the
        // one that propagates and so becomes the cause a future reports on its
        // ExecutionException. A runtime exception never reaches the handler
        // below, because this overload re-throws one directly; only a checked
        // exception arrives wrapped, and its cause restores the type thrown.
        return SubjectUtil.doAs(subject, (PrivilegedExceptionAction<T>) delegate::call);
      } catch (PrivilegedActionException e) {
        Exception cause = e.getException();
        throw cause != null ? cause : e;
      }
    }
  }
}
