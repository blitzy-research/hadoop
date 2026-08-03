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
import java.util.Collection;
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
 * What is prepared is the <em>task</em> a submission asks to have run, never the
 * future an executor makes to run it with. That is what keeps the identity's
 * lifetime the task's own: a future releases the task it was given as soon as it
 * completes or is cancelled, so a caller that abandons its work leaves nothing
 * of its identity behind, and what an executor keeps on its queue stays the
 * object it would have queued with nothing prepared at all -- which is what
 * cancellation sweeps, shut-down lists and task removal are all expressed in
 * terms of. An executor whose own submission methods prepare on its behalf
 * therefore opens a prepared-submission scope for the thread making the call, so
 * that the future it then builds around the prepared task is queued as itself;
 * that scope is {@link #beginPreparedSubmission()} and it is thread-confined,
 * because every submission a JDK submission path makes on behalf of a call is
 * made on the thread that made the call.
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

  /**
   * Set for as long as a submission on this thread has task bodies prepared for
   * it that a JDK submission path is about to build futures around. It carries
   * no identity, is read only by {@link #submissionAlreadyPrepared()}, and is
   * removed again by {@link #endPreparedSubmission(boolean)}, so a pooled thread
   * is left holding nothing between submissions.
   */
  private static final ThreadLocal<Boolean> PREPARED_SUBMISSION =
      new ThreadLocal<>();

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
   * Prepares every task in {@code tasks}, on exactly the terms
   * {@link #wrap(Callable)} states, for an executor whose bulk submission
   * methods prepare on their own behalf.
   * <p>
   * The preparation happens here, on the thread that called the bulk submission
   * method, which is what makes the subject that thread's own. A {@code null}
   * collection, and a {@code null} element within one, are passed on as they
   * are, so that an executor's own refusal of either is left exactly where it
   * was.
   *
   * @param <T> the result type of the tasks
   * @param tasks the tasks to prepare; may be {@code null}
   * @return the prepared tasks, in the order {@code tasks} yields them, or
   *         {@code null} when {@code tasks} is {@code null}
   */
  static <T> Collection<Callable<T>> wrapEach(
      Collection<? extends Callable<T>> tasks) {
    if (tasks == null) {
      return null;
    }
    Collection<Callable<T>> prepared = new ArrayList<>(tasks.size());
    for (Callable<T> task : tasks) {
      prepared.add(wrap(task));
    }
    return prepared;
  }

  /**
   * Whether the submission being made on this thread has already had the task
   * it asks to have run prepared here.
   * <p>
   * An executor asks this of a task that arrives already inside a future,
   * because such a task is one a JDK submission path built for a body this class
   * has just prepared: preparing that future in turn would put the identity
   * outside the object the future releases when it completes or is cancelled,
   * and would leave a second layer behind the single one
   * {@link #unwrap(Runnable)} takes off.
   *
   * @return {@code true} where a prepared-submission scope is open on this
   *         thread, and always {@code false} on a runtime where nothing is
   *         prepared at all
   */
  static boolean submissionAlreadyPrepared() {
    return !SubjectUtil.THREAD_INHERITS_SUBJECT
        && PREPARED_SUBMISSION.get() != null;
  }

  /**
   * Opens a prepared-submission scope on this thread, for an executor that has
   * just prepared the task it is about to hand to a JDK submission path.
   * <p>
   * The value returned says whether such a scope was already open, and must be
   * given back to {@link #endPreparedSubmission(boolean)} in a {@code finally}
   * block, so that the thread is left in exactly the state it was found in
   * however the submission ends. Nothing is recorded on a runtime where nothing
   * is prepared.
   *
   * @return whether a prepared-submission scope was already open on this thread
   */
  static boolean beginPreparedSubmission() {
    if (SubjectUtil.THREAD_INHERITS_SUBJECT) {
      return false;
    }
    boolean enclosing = PREPARED_SUBMISSION.get() != null;
    PREPARED_SUBMISSION.set(Boolean.TRUE);
    return enclosing;
  }

  /**
   * Closes the prepared-submission scope {@link #beginPreparedSubmission()}
   * opened, leaving an enclosing scope open where there was one.
   *
   * @param enclosing the value {@link #beginPreparedSubmission()} returned
   */
  static void endPreparedSubmission(boolean enclosing) {
    if (SubjectUtil.THREAD_INHERITS_SUBJECT || enclosing) {
      return;
    }
    PREPARED_SUBMISSION.remove();
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
