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
 * Up to and including Java 21 the runtime gave a newly created thread the
 * subject of the thread that created it, and Hadoop relied on that: work handed
 * to a thread pool created inside a {@code doAs} scope reached its worker with
 * the caller's identity in place. From Java 24 a new thread never inherits it,
 * so that work now reaches its worker with no identity at all. Nothing reports
 * this: such code compiles, throws nothing and logs nothing, and the loss
 * surfaces later as a denied authorization, as an audit record naming the wrong
 * user, or as a {@link NullPointerException} far from its cause.
 * <p>
 * The methods here close that gap at the point where a task is handed over.
 * {@link #wrap(Runnable)} and {@link #wrap(Callable)} read the subject of the
 * thread that calls them and return a task that re-establishes it, for the
 * duration of that task, on whichever thread eventually runs it. Because they
 * are called on the submitting thread, the subject is read at submission rather
 * than at worker creation, so a pool that reuses its threads runs each task
 * under the identity of that task's own submitter rather than under the identity
 * that happened to cause the worker to exist.
 * <p>
 * A task is handed straight back, with nothing allocated and nothing to
 * re-establish, in four cases:
 * <ul>
 *   <li>the running JVM still hands a newly created thread the subject of its
 *   creator, as {@link SubjectUtil#THREAD_INHERITS_SUBJECT} reports for Java 21
 *   and earlier. Such a runtime carries the identity across the thread boundary
 *   itself, so this class stays out of its way entirely and leaves its
 *   behaviour, and its cost, exactly as they were before this migration;</li>
 *   <li>the submitting thread carries no subject. Re-establishing an absent
 *   identity is not the same as leaving one out: the replacement API accepts a
 *   {@code null} subject and binds it, so passing one on would hide an
 *   identity in force where the task runs instead of leaving the task with
 *   none;</li>
 *   <li>the task is {@code null}, so that the executor it was destined for
 *   rejects it exactly as it always has, at the moment it is handed over;</li>
 *   <li>the task already carries an identity read by one of these methods, so
 *   that a rejected task an executor is offered a second time -- as
 *   {@link java.util.concurrent.ThreadPoolExecutor.DiscardOldestPolicy} offers
 *   it -- keeps the single layer that {@link #unwrap(Runnable)} takes back off,
 *   and keeps the identity of the submission that was rejected rather than
 *   picking up whichever identity happens to be current on the retry.</li>
 * </ul>
 * What a task handed back unchanged observes is therefore whatever its worker
 * holds: nothing at all on a worker that was given no identity of its own, and
 * the worker's own identity where it took one when it was created, as a worker
 * built by {@link SubjectInheritingThread} does. That is what such a task
 * observed on every runtime this project supported before this migration, and
 * it is recorded, with its consequences, under "Behavioural resolutions" in
 * {@code JDK25Migration.md}.
 * <p>
 * Where an identity is re-established, nothing else about the task changes. The
 * identity is applied through {@code SubjectUtil.doAs} rather than through the
 * replacement API directly, because only {@code doAs} unwraps the
 * {@link java.util.concurrent.CompletionException} that the replacement API is
 * specified to throw and re-throws the original cause: an exception a task
 * throws therefore keeps its own type, and stays the cause a
 * {@link java.util.concurrent.Future} reports on its
 * {@link java.util.concurrent.ExecutionException}. The future a submitter holds
 * is still the executor's own, because an executor builds that future before it
 * hands the work on, and every place that describes a queued task rather than
 * running it takes this layer back off with {@link #unwrap(Runnable)} first, so
 * what such a place reports is the task that was submitted.
 * <p>
 * Wrapping belongs at a single point per executor, so that no call site need
 * know of any of this, and code that inspects a task instead of running it calls
 * {@link #unwrap(Runnable)} first. This class covers a task handed to a pool;
 * {@link SubjectInheritingThread} covers a thread created and started directly.
 */
@InterfaceAudience.Private
public final class SubjectPreservingTasks {

  private SubjectPreservingTasks() {
  }

  /**
   * Returns a task that runs {@code task} under the subject current on this
   * thread.
   * <p>
   * The subject is read here, on the calling thread, and applied on whichever
   * thread runs the task, so a task submitted to a pool runs under the identity
   * of its own submitter. The argument is returned unchanged in the four cases
   * this class documents: a runtime that hands a new thread its creator's
   * subject, a calling thread carrying no subject, a {@code null} task, and a
   * task already prepared here.
   * <p>
   * An exception thrown by {@code task} propagates with its own type, because
   * establishing the identity contributes no exception of its own.
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
   * Returns a task that calls {@code task} under the subject current on this
   * thread.
   * <p>
   * This is the {@link Callable} counterpart of {@link #wrap(Runnable)}: it
   * reads the subject on exactly the same terms and returns the argument
   * unchanged in exactly the same four cases.
   * <p>
   * An exception thrown by {@code task} keeps its own type, whether it is
   * checked or unchecked, so a caller reading the result through a future is
   * handed an {@link java.util.concurrent.ExecutionException} whose cause is the
   * exception the task itself threw.
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
   * Returns the task that {@link #wrap(Runnable)} was given, when {@code task}
   * is one of the tasks it returned.
   * <p>
   * Anything else, {@code null} included, is returned as it is. This lets code
   * that inspects a task instead of running it see the task as it was
   * submitted, which matters because a prepared task is neither a
   * {@link java.util.concurrent.Future} nor of the class the submitter passed
   * in.
   * <p>
   * Exactly one layer is removed. Wrapping is applied at a single point per
   * executor and an already-prepared task is left alone, so one layer is all
   * there is to remove, and stopping after one leaves a mistaken second layer
   * visible instead of concealing it.
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

    /**
     * The subject read on the submitting thread, never {@code null}: a
     * submission carrying no subject is handed back unprepared, so that its
     * task is not run with an absent identity established over whatever is in
     * force where it runs.
     */
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
   * Calls its delegate under the subject read on the submitting thread.
   *
   * @param <T> the result type of the delegate
   */
  private static final class SubjectPreservingCallable<T>
      implements Callable<T> {

    private final Callable<T> delegate;

    /**
     * The subject read on the submitting thread, never {@code null}, for the
     * reason given in {@link SubjectPreservingRunnable}.
     */
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
        return SubjectUtil.doAs(subject,
            (PrivilegedExceptionAction<T>) delegate::call);
      } catch (PrivilegedActionException e) {
        Exception cause = e.getException();
        throw cause != null ? cause : e;
      }
    }
  }
}
