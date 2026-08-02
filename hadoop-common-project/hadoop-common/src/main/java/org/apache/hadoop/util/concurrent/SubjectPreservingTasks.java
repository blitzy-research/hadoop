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
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import javax.security.auth.Subject;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.security.authentication.util.SubjectUtil;

/**
 * Carries the current JAAS subject from the thread that submits a task to the
 * pooled thread that runs it.
 * <p>
 * No runtime hands a task the identity of whoever submitted it. What a worker
 * holds is an identity of its own, fixed when it was created: up to and
 * including Java 21 the runtime gave a new thread the identity of the thread
 * that created it, from Java 22 it does so only in a special case, and from
 * Java 24 never. Since a worker outlives any one task it runs and serves one
 * submitter after another, a task therefore observes either no identity at
 * all, where its worker was given none, or the identity of whoever first
 * caused that worker to exist. The second outcome is the worse of the two: one
 * submitter's work then decides authorization as some other user, and that
 * other user is what an audit record names. Neither outcome reports itself,
 * since such code compiles, throws nothing and logs nothing, so the wrong
 * identity surfaces later as a denied authorization or as an audit record
 * naming the wrong user.
 * <p>
 * The methods here read the subject of the thread that calls them and return a
 * task that re-establishes it inside the worker for the duration of that task.
 * Because they run on the submitting thread, the subject is captured at
 * submission rather than at worker creation, so a pool that reuses its threads
 * still runs every task under the identity of its own submitter. That is the
 * point of reading it at every submission: a worker outlives any one task it
 * runs, so an identity read once per worker would run every later submission as
 * the first submitter, and running one submitter's work as another user decides
 * authorization and is what an audit record names.
 * <p>
 * Every submission is treated this way, on every runtime and whether or not
 * the submitting thread carries an identity.
 * {@link SubjectUtil#THREAD_INHERITS_SUBJECT} reports whether the runtime
 * gives a <em>newly created</em> thread the identity of its creator, which is
 * a different boundary from a task reaching a worker that already exists: on a
 * runtime where it is set, the identity a worker was created with is precisely
 * the stale one a later submitter's task would otherwise observe, so that flag
 * cannot stand in for reading the subject here and is deliberately not
 * consulted. A submission that carries no identity is prepared in the same
 * way, and for the same reason: running such a task with none is what its
 * submitter would have seen, whereas leaving it alone would let it run as
 * whichever user its worker was left holding.
 * <p>
 * An absent identity is carried across as deliberately as a present one: a
 * worker that took an identity of its own when it was created, as
 * {@link SubjectInheritingThread} does and as the runtimes that copy an identity
 * into a new thread do, keeps it for as long as it lives and would otherwise
 * lend it to every later task it happens to run. A task submitted by a thread
 * holding nothing therefore runs holding nothing, which is what its submitter
 * would have observed.
 * <p>
 * A task is handed straight back, unchanged and with nothing allocated, in two
 * cases. When the task itself is {@code null} it is passed on, so that the
 * executor it was destined for rejects it exactly as it always has. And when the
 * task already carries an identity from one of these methods it is left as it
 * is, so that a rejected task an executor is offered a second time, as
 * {@link java.util.concurrent.ThreadPoolExecutor.DiscardOldestPolicy} offers
 * it, keeps the single layer that {@link #unwrap(Runnable)} takes back off —
 * and keeps the identity of the submission that was rejected rather than
 * picking up whichever identity happens to be current on the retry.
 * <p>
 * The cost is one small object per submission and the establishing of an
 * identity per execution, which is the price of running each task as its own
 * submitter and is bounded by the number of tasks rather than by the work they
 * do. Nothing else about a task changes: an exception it throws keeps its own
 * type, the future its submitter holds is the executor's own, and the task
 * names itself as it always did, so that a rejection message and a log line
 * describe the submitted task rather than the machinery around it.
 * <p>
 * Wrapping belongs at a single point per executor, so that no call site need
 * know of any of this, and code that inspects a task instead of running it
 * calls {@link #unwrap(Runnable)} first. A method that is handed a whole
 * collection of tasks at once passes it through {@link #wrapEach(Collection)},
 * which prepares each task as that method reaches it rather than beforehand, so
 * that the method's own timing and its own order of handing tasks over are left
 * to it. This class covers a task handed to a pool;
 * {@link SubjectInheritingThread} covers a thread created and started directly.
 */
@InterfaceAudience.Private
public final class SubjectPreservingTasks {

  private SubjectPreservingTasks() {
  }

  /**
   * Returns a task that runs {@code task} under the subject of the calling
   * thread, or under no subject at all when the calling thread holds none.
   * <p>
   * The subject is read here, on the submitting thread, so the returned task
   * carries the identity of whoever submitted it rather than the identity that
   * happened to be current when the worker thread was created, and it is
   * established for the duration of that task alone, so a worker that runs it
   * carries none of it into the task after it. A pool that reuses its workers
   * therefore runs every task under the identity of its own submitter, and a
   * task submitted with no identity runs with none rather than under whatever
   * its worker was left holding.
   * <p>
   * The argument is returned unchanged, with nothing allocated, when
   * {@code task} is {@code null}, so that the executor it is handed to still
   * rejects it exactly as it would have; and when {@code task} already came from
   * one of these methods, so that a task an executor is offered again after
   * rejecting it ends up prepared once rather than twice, under the identity
   * that submitted it rather than under whatever is current on the retry.
   *
   * @param task the task to run under the current subject, or under none;
   *             may be {@code null}
   * @return a task that establishes the calling thread's identity or its
   *         absence, or {@code task} itself when it is {@code null} or already
   *         prepared
   */
  public static Runnable wrap(Runnable task) {
    if (task == null || task instanceof SubjectPreservingRunnable) {
      return task;
    }
    return new SubjectPreservingRunnable(task, SubjectUtil.current());
  }

  /**
   * Returns a task that calls {@code task} under the subject of the calling
   * thread, or under no subject at all when the calling thread holds none.
   * <p>
   * This is the {@link Callable} counterpart of {@link #wrap(Runnable)} and
   * reads the identity to run under on exactly the same terms, returning the
   * argument unchanged in exactly the same two cases.
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
   *         {@code task} itself when it is {@code null} or already prepared
   */
  public static <T> Callable<T> wrap(Callable<T> task) {
    if (task == null || task instanceof SubjectPreservingCallable) {
      return task;
    }
    return new SubjectPreservingCallable<>(task, SubjectUtil.current());
  }

  /**
   * Returns a view of {@code tasks} in which each task is prepared, by
   * {@link #wrap(Callable)}, as it is taken out.
   * <p>
   * This exists for the methods that hand a whole collection of tasks over at
   * once. Such a method sets its own deadline and decides for itself when to
   * hand over the next task, both from the collection it is given, so preparing
   * the tasks by reading that collection through beforehand would move work
   * before the deadline is set and would hand every task over at once. A view
   * leaves the reading to the method that was given it, so a deadline still
   * covers the reading of the collection and a task is still handed over only
   * when it is wanted.
   * <p>
   * The view holds no tasks of its own: it reports the size of the collection
   * behind it and yields that collection's tasks, in its order, each prepared
   * as it is reached. Reading it twice therefore prepares each task twice, so
   * it is meant to be read once, as the methods it is written for do. It is
   * read-only, the collection behind it is left as it is, and {@code null} is
   * returned unchanged so that a method given {@code null} still rejects it
   * exactly as it would have.
   * <p>
   * Because the reading happens on the thread that called such a method, and
   * that method does not return until its tasks have finished or its deadline
   * has passed, every task is still prepared on the submitting thread and under
   * the identity that submitted it.
   *
   * @param <T> the result type of the tasks
   * @param tasks the tasks to prepare as they are taken out; may be
   *              {@code null}
   * @return a view of {@code tasks} preparing each task as it is reached, or
   *         {@code null} when {@code tasks} is {@code null}
   */
  public static <T> Collection<Callable<T>> wrapEach(
      Collection<? extends Callable<T>> tasks) {
    if (tasks == null) {
      return null;
    }
    return new PreparingCollection<>(tasks);
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
   * executor and an already-prepared task is left alone, so one layer is all
   * there is to remove, and stopping after one leaves a mistaken second layer
   * visible instead of concealing it.
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
   * Returns the given tasks as they were submitted, in the order they arrived.
   * <p>
   * A pool stopped at once owes its caller the tasks it was handed and never
   * started, so that they can be run elsewhere or reported on, and not the form
   * the pool queued them in. The list handed back by the pool is left alone and
   * a new one is returned, since the caller of a shutdown is entitled to a list
   * of its own.
   * <p>
   * Each task is unwrapped one level, exactly as {@link #unwrap(Runnable)} does
   * and for the same reason, so a task that was never wrapped is returned as it
   * is.
   *
   * @param tasks the tasks a pool gave back, as it queued them
   * @return those tasks as they were submitted
   */
  static List<Runnable> unwrapAll(List<Runnable> tasks) {
    List<Runnable> unwrapped = new ArrayList<>(tasks.size());
    for (Runnable task : tasks) {
      unwrapped.add(unwrap(task));
    }
    return unwrapped;
  }

  /**
   * Reports whether {@code captured} is already the identity in force here, so
   * that establishing it again would leave everything exactly as it is.
   * <p>
   * A task can reach a worker having been prepared more than once. One executor
   * forwarding to another prepares it at each boundary it crosses, and the
   * decoration each of them adds on its own account -- a permit to release, a
   * listenable future to complete -- sits between the two, so neither can tell by
   * looking that the other has already been there. Every one of those
   * preparations captured the same identity from the same submitting thread, so
   * all but the outermost re-establish an identity that is already established.
   * <p>
   * Establishing it again is harmless and is not what this avoids: an identity
   * established over itself leaves the same one in force, so what a task
   * observes never depends on how many times it was prepared. What it costs is
   * real, though. Each one is a scoped binding held for the whole of the task,
   * and the innermost task pays for all of them on every run. Recognising the
   * case here, where the identity in force can simply be read, catches it however
   * the layers were nested and whatever sits between them, which is what
   * comparing task types cannot do.
   * <p>
   * The comparison is by reference on purpose. Two subjects holding the same
   * principals are equal to one another without being the same object, and code
   * downstream may reasonably distinguish them; skipping on equality alone would
   * leave a different instance in force from the one that was captured. Being
   * the same object is the only case in which nothing whatsoever changes, and it
   * is the case that arises here, because every layer captured the identity from
   * one thread at one moment. Two absences are likewise the same absence, which
   * covers a task prepared for a submission that carried no identity.
   *
   * @param captured the identity captured when the task was prepared, which may
   *        be {@code null}
   * @return whether establishing {@code captured} would change nothing
   */
  private static boolean isAlreadyCurrent(Subject captured) {
    return captured == SubjectUtil.current();
  }

  /**
   * A read-only view of a collection of tasks that prepares each task as it is
   * reached.
   *
   * @param <T> the result type of the tasks
   */
  private static final class PreparingCollection<T>
      extends AbstractCollection<Callable<T>> {

    private final Collection<? extends Callable<T>> tasks;

    PreparingCollection(Collection<? extends Callable<T>> tasks) {
      this.tasks = tasks;
    }

    @Override
    public Iterator<Callable<T>> iterator() {
      final Iterator<? extends Callable<T>> source = tasks.iterator();
      return new Iterator<Callable<T>>() {
        @Override
        public boolean hasNext() {
          return source.hasNext();
        }

        @Override
        public Callable<T> next() {
          return wrap(source.next());
        }
      };
    }

    @Override
    public int size() {
      return tasks.size();
    }
  }

  /**
   * Runs its delegate under the subject captured on the submitting thread.
   */
  private static final class SubjectPreservingRunnable implements Runnable {

    private final Runnable delegate;
    /**
     * The subject captured on the submitting thread, which is {@code null} when
     * that thread held none and the task is bound for a worker that may hold one
     * of its own. Establishing a {@code null} subject leaves the delegate with no
     * identity, which is what such a task is owed.
     */
    private final Subject subject;

    SubjectPreservingRunnable(Runnable delegate, Subject subject) {
      this.delegate = delegate;
      this.subject = subject;
    }

    @Override
    public void run() {
      if (isAlreadyCurrent(subject)) {
        delegate.run();
        return;
      }
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

    /**
     * Returns the text of the task as it was submitted.
     * <p>
     * Not everything that describes a queued task gets the chance to unwrap it
     * first. A pool that turns a submission away names the task in the exception
     * it throws, and that message reaches an operator. Reporting the submitted
     * task's own text leaves such a message exactly as it was.
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
    /**
     * The subject captured on the submitting thread, {@code null} when that
     * thread held none and the task is bound for a worker that may hold one of
     * its own, exactly as in {@link SubjectPreservingRunnable}.
     */
    private final Subject subject;

    SubjectPreservingCallable(Callable<T> delegate, Subject subject) {
      this.delegate = delegate;
      this.subject = subject;
    }

    @Override
    public T call() throws Exception {
      if (isAlreadyCurrent(subject)) {
        return delegate.call();
      }
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
