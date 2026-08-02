/*
 * *
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
 * /
 */

package org.apache.hadoop.util.concurrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


/**
 * An extension of ThreadPoolExecutor that provides additional functionality.
 * <p>
 * A task is prepared to run under the identity of the thread that submitted it
 * at exactly one point in this class, {@link #execute(Runnable)}, which is where
 * every one of the JDK's ways of submitting work arrives. No other method here
 * prepares a task, so a submitted task is behind exactly one such layer and
 * {@link SubjectPreservingTasks#unwrap(Runnable)} takes all of it back off.
 * <p>
 * The other methods overridden here neither prepare a task nor change how one is
 * submitted. They are what keeps that single decision from being visible
 * anywhere else: this pool goes on describing a task as the task its caller
 * submitted rather than as the form it queued -- in the class name written
 * before a task runs, in the tasks a stopped pool hands back, in a named task
 * being taken off the queue, and in a swept queue -- and it lets go of an
 * identity the queue would otherwise keep reachable after the work that identity
 * belonged to has been given up on. Each method below says which of the two it
 * is there for.
 */
public final class HadoopThreadPoolExecutor extends ThreadPoolExecutor {

  private static final Logger LOG = LoggerFactory
      .getLogger(HadoopThreadPoolExecutor.class);

  /**
   * The prepared queue entries that a submission for one result, in progress on
   * this thread, will owe a reclamation once it ends, or {@code null} when no
   * such submission is in progress here.
   * <p>
   * This holds no identity and propagates nothing: it is a note the pool keeps
   * to itself, for the extent of one call on one thread, of what that call put
   * on the queue on its behalf, and it is put back exactly as it was found when
   * the call ends. {@link #invokeAny(Collection)} explains what the note is for
   * and why nothing else can serve in its place.
   */
  private static final ThreadLocal<List<Runnable>> ENTRIES_TO_RECLAIM =
      new ThreadLocal<>();

  public HadoopThreadPoolExecutor(int corePoolSize,
      int maximumPoolSize,
      long keepAliveTime,
      TimeUnit unit,
      BlockingQueue<Runnable> workQueue) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
  }

  public HadoopThreadPoolExecutor(int corePoolSize,
      int maximumPoolSize,
      long keepAliveTime,
      TimeUnit unit,
      BlockingQueue<Runnable> workQueue,
      ThreadFactory threadFactory) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue,
        threadFactory);
  }

  public HadoopThreadPoolExecutor(int corePoolSize,
      int maximumPoolSize,
      long keepAliveTime,
      TimeUnit unit,
      BlockingQueue<Runnable> workQueue,
      RejectedExecutionHandler handler) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue,
        handler);
  }

  public HadoopThreadPoolExecutor(int corePoolSize,
      int maximumPoolSize,
      long keepAliveTime,
      TimeUnit unit,
      BlockingQueue<Runnable> workQueue,
      ThreadFactory threadFactory,
      RejectedExecutionHandler handler) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue,
        threadFactory, handler);
  }

  /**
   * Hands {@code command} to the pool so that it runs under the JAAS subject of
   * the thread that submitted it.
   * <p>
   * Every way of giving work to this executor arrives here: the JDK routes each
   * {@code submit}, {@code invokeAll} and {@code invokeAny} overload through
   * this method. Preparing the task once, at this single point, therefore
   * reaches every submission and leaves none of them out, and no other method
   * of this class prepares one, so the submitted task stays behind the single
   * layer that {@link SubjectPreservingTasks} takes back off.
   * <p>
   * A worker of this pool outlives the task it runs and serves one submitter
   * after another, so an identity to run under has to be read from the
   * submitting thread as the task is handed over, and not once, when the worker
   * is created. Reading it here is what gives a task the identity of its own
   * submitter rather than that of whoever first caused a worker to exist, and
   * establishing it for the task's own extent is what keeps the worker from
   * carrying it into the task after it. Every submission is prepared this way,
   * including one made by a thread that holds no identity: such a task runs
   * with none, which is what its submitter would have observed, rather than
   * under whichever user its worker was left holding.
   * <p>
   * This method can also be reached a second time with the very same task: a
   * rejection policy that offers a task back, as
   * {@link ThreadPoolExecutor.DiscardOldestPolicy} does, calls into it again
   * once the pool has made room. An already prepared task is handed on as it is,
   * so a second offer neither adds a layer nor replaces the identity read when
   * the task was first submitted. The task this pool reports on, and the future
   * returned to whoever submitted it, both still describe the task as
   * submitted.
   * <p>
   * When the task handed over is a future this pool did not make, and a
   * submission for one result is in progress on this thread, the prepared entry
   * is noted down so that {@link #invokeAny(Collection)} can reclaim it if that
   * submission abandons it. The note is set aside for the duration of the
   * hand-over itself, because a rejection policy may run the task here and from
   * there hand over work of its own, which belongs to whoever submitted it and
   * is not the enclosing submission's to reclaim.
   *
   * @param command the task to run
   */
  @Override
  public void execute(Runnable command) {
    Runnable prepared = SubjectPreservingTasks.wrap(command);
    List<Runnable> reclaimable = reclamationNotesFor(command, prepared);
    if (reclaimable == null) {
      super.execute(prepared);
      return;
    }
    reclaimable.add(prepared);
    ENTRIES_TO_RECLAIM.remove();
    try {
      super.execute(prepared);
    } finally {
      ENTRIES_TO_RECLAIM.set(reclaimable);
    }
  }

  /**
   * Answers which submission for one result, if any, is owed a reclamation of
   * {@code prepared} should it be abandoned on the queue.
   * <p>
   * Only a prepared entry standing for a future of someone else's making can be
   * owed one. A task that was handed over as it is carries no identity to
   * release; a future this pool made reclaims its own place when it is
   * cancelled, through {@link #newTaskFor(Callable)}; and a task handed over
   * while no such submission is in progress on this thread belongs to no
   * submission that could abandon it.
   *
   * @param command the task as it was handed over
   * @param prepared the form of it that reaches the queue
   * @return the notes of the submission that queued it, or {@code null} if none
   */
  private List<Runnable> reclamationNotesFor(Runnable command,
      Runnable prepared) {
    if (prepared == command || !(command instanceof RunnableFuture<?>)
        || command instanceof QueueReclaimingFutureTask<?>) {
      return null;
    }
    return ENTRIES_TO_RECLAIM.get();
  }

  /**
   * Builds the future that a task submitted for its result is reported through,
   * as one that gives up its place on the queue as soon as it is cancelled.
   * <p>
   * A task submitted for a result reaches the queue prepared with the identity
   * of its submitter, and that identity, together with the credentials in it,
   * stays reachable for as long as the queue holds the prepared task. Cancelling
   * the future releases what the future itself was holding but says nothing to
   * the queue, so a cancelled task keeps its submitter's credentials alive until
   * something else displaces it -- which, for a pool whose workers are all
   * occupied or whose queue has stopped draining, may be never. Reclaiming the
   * place at the moment of cancellation is what bounds that, and doing it here
   * is what makes it happen on its own rather than only when a caller thinks to
   * ask for it. {@link #purge()} remains for the tasks this cannot reach.
   * <p>
   * Nothing is prepared here. Preparing a task remains the business of
   * {@link #execute(Runnable)} alone, so a submission still passes exactly one
   * such point and the prepared task still has exactly one layer for
   * {@link SubjectPreservingTasks#unwrap(Runnable)} to take back off. The future
   * this returns is a {@link java.util.concurrent.FutureTask} like the one it
   * replaces and behaves as that one does in every other respect: run in the
   * same way, cancelled in the same way, and reporting the same result and the
   * same failure to whoever holds it. Its own type is therefore what the line
   * written before such a task runs names, that line naming the task this pool
   * was handed as it always has.
   *
   * @param <T> the result type of the task
   * @param callable the task to be called for its result
   * @return a future that reclaims its place on the queue when cancelled
   */
  @Override
  protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
    return new QueueReclaimingFutureTask<>(callable);
  }

  /**
   * Builds the future that a task submitted without a result of its own is
   * reported through, on the same terms as
   * {@link #newTaskFor(java.util.concurrent.Callable)}.
   *
   * @param <T> the type of the given result
   * @param runnable the task to be run
   * @param value the result to report once the task has run
   * @return a future that reclaims its place on the queue when cancelled
   */
  @Override
  protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
    return new QueueReclaimingFutureTask<>(runnable, value);
  }

  /**
   * Runs the given tasks until one of them completes, returning its result and
   * letting go of the identities the abandoned ones were prepared with.
   * <p>
   * The JDK runs a submission for one result through a completion service, which
   * hands this pool a future of its own making to run -- one that carries the
   * future this pool made, so that the first task to finish can be recognised.
   * It is that carrier which reaches the queue prepared with the submitter's
   * identity, and when the submission ends only the futures this pool made are
   * cancelled. A cancelled one of those reclaims its own place through
   * {@link #newTaskFor(Callable)}, and {@link #purge()} recognises an entry
   * standing for a cancelled future, but neither reaches the carrier: it is a
   * different object, and nothing ever cancels it. On a pool whose workers are
   * all occupied, or whose queue has stopped draining, every abandoned carrier
   * would therefore keep its submitter's subject, and the credentials in it,
   * reachable for as long as it stayed there.
   * <p>
   * This is closed by reclaiming, once the submission has ended, the entries it
   * queued that are still waiting. That happens after the JDK has cancelled the
   * futures it is abandoning, so an entry still waiting then stands for work
   * that will not be run and that no caller is left holding: the task that won
   * has already run, its result or its failure has already been returned to
   * whoever asked for it, and the queue entries of a submission that has ended
   * are visible to nobody else. Which task wins, how long the pool waits, and
   * what is thrown are all the JDK's own and are unchanged.
   *
   * @param <T> the result type of the tasks
   * @param tasks the tasks to run
   * @return the result of a task that completed
   * @throws InterruptedException if this thread was interrupted while waiting
   * @throws ExecutionException if no task completed without throwing
   */
  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
      throws InterruptedException, ExecutionException {
    List<Runnable> reclaimable = new ArrayList<>();
    List<Runnable> enclosing = ENTRIES_TO_RECLAIM.get();
    ENTRIES_TO_RECLAIM.set(reclaimable);
    try {
      return super.invokeAny(tasks);
    } finally {
      endReclamation(enclosing, reclaimable);
    }
  }

  /**
   * Runs the given tasks until one of them completes or the time runs out, on
   * the same terms as {@link #invokeAny(Collection)}.
   * <p>
   * Running out of time abandons every task, so this is the case in which the
   * most identities are left on the queue, and reclaiming them happens here for
   * the same reason and in the same place: after the JDK has cancelled what it
   * is abandoning, and before the timeout is reported to the caller.
   *
   * @param <T> the result type of the tasks
   * @param tasks the tasks to run
   * @param timeout how long to wait for a task to complete
   * @param unit the unit of {@code timeout}
   * @return the result of a task that completed
   * @throws InterruptedException if this thread was interrupted while waiting
   * @throws ExecutionException if no task completed without throwing
   * @throws TimeoutException if no task completed before the time ran out
   */
  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout,
      TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    List<Runnable> reclaimable = new ArrayList<>();
    List<Runnable> enclosing = ENTRIES_TO_RECLAIM.get();
    ENTRIES_TO_RECLAIM.set(reclaimable);
    try {
      return super.invokeAny(tasks, timeout, unit);
    } finally {
      endReclamation(enclosing, reclaimable);
    }
  }

  /**
   * Puts back the notes of the enclosing submission, if there was one, and takes
   * off the queue whatever the submission that has just ended left waiting
   * there.
   * <p>
   * An entry that a worker has already taken, or that the pool no longer holds,
   * is simply not found, and nothing happens for it. Removal goes through the
   * pool's own so that the entry is matched as the object it is, without
   * searching the queue for the task it stands for.
   *
   * @param enclosing the notes to restore, or {@code null} if there were none
   * @param queued the entries the ended submission put on the queue
   */
  private void endReclamation(List<Runnable> enclosing, List<Runnable> queued) {
    if (enclosing == null) {
      ENTRIES_TO_RECLAIM.remove();
    } else {
      ENTRIES_TO_RECLAIM.set(enclosing);
    }
    for (Runnable entry : queued) {
      super.remove(entry);
    }
  }

  /**
   * Stops this pool at once and returns the tasks that never started, as they
   * were submitted.
   * <p>
   * A caller shutting a pool down is owed the tasks it handed over, so that it
   * can run them somewhere else or report on them, and not the form this pool
   * queued them in. Handing back the prepared form would make every caller
   * responsible for recognising and undoing a detail of this class, and a caller
   * that did not know to would be left holding tasks it could not identify.
   *
   * @return the tasks that never started, each as it was submitted
   */
  @Override
  public List<Runnable> shutdownNow() {
    return SubjectPreservingTasks.unwrapAll(super.shutdownNow());
  }

  /**
   * Takes a task off the queue if it is still waiting there.
   * <p>
   * The caller names the task it submitted, while the queue may be holding the
   * prepared form of it, so the task standing for the one named is removed as
   * well. The queue is only searched when the task is not found as it is, which
   * is what happens for a task that was never prepared, so the common case still
   * costs one queue removal and nothing more.
   * <p>
   * Nothing else about removal changes: it still reports whether the task was
   * waiting, and a pool left empty by it still terminates, because the removal
   * itself is still the pool's own.
   * <p>
   * This is not a hypothetical caller. Within this project,
   * {@code ValueQueue.drain(String)} takes a refill task off this pool by naming
   * the task it queued, and any caller of the inherited method is entitled to the
   * same, since naming a task and having it removed is what the method has always
   * promised.
   *
   * @param task the task to remove, as it was submitted
   * @return whether the task was waiting on the queue and was removed
   */
  @Override
  public boolean remove(Runnable task) {
    if (super.remove(task)) {
      return true;
    }
    if (task == null) {
      return false;
    }
    for (Runnable queued : getQueue().toArray(new Runnable[0])) {
      if (task.equals(SubjectPreservingTasks.unwrap(queued))) {
        return super.remove(queued);
      }
    }
    return false;
  }

  /**
   * Drops the cancelled tasks that are still taking up room on the queue.
   * <p>
   * A cancelled task says so through the future it was submitted as, and the
   * queue may be holding the prepared form of that future rather than the future
   * itself, which the pool's own sweep does not recognise. Each queued task is
   * therefore asked about the task it stands for first, and only entries that
   * are prepared are examined here, because the pool's own sweep afterwards
   * already handles the rest.
   * <p>
   * Reclaiming these entries is what releases the identity each one captured.
   * A cancelled task left on the queue keeps its submitter's subject, and the
   * credentials in it, reachable until something else displaces it -- which for
   * a queue that has stopped draining may be never.
   */
  @Override
  public void purge() {
    for (Runnable queued : getQueue().toArray(new Runnable[0])) {
      Runnable task = SubjectPreservingTasks.unwrap(queued);
      if (task != queued && task instanceof Future<?>
          && ((Future<?>) task).isCancelled()) {
        super.remove(queued);
      }
    }
    super.purge();
  }

  @Override
  protected void beforeExecute(Thread t, Runnable r) {
    r = SubjectPreservingTasks.unwrap(r);
    if (LOG.isDebugEnabled()) {
      LOG.debug("beforeExecute in thread: " + Thread.currentThread()
          .getName() + ", runnable type: " + r.getClass().getName());
    }
  }

  @Override
  protected void afterExecute(Runnable r, Throwable t) {
    super.afterExecute(r, t);
    ExecutorHelper.logThrowableFromAfterExecute(r, t);
  }

  /**
   * The future a task submitted for its result is reported through, which gives
   * up the task's place on the queue as soon as the task is cancelled.
   * <p>
   * A task completes once, and being cancelled is one of the ways it completes,
   * so the reclaiming below happens at most once for a task and only ever for a
   * task that will not be run. It goes through {@link #remove(Runnable)}, which
   * recognises the prepared form the queue is holding, so the entry that carries
   * the submitter's identity is the entry that is reclaimed and the identity
   * stops being reachable through this pool. A task that is no longer on the
   * queue -- because a worker took it, or because the pool was stopped -- is
   * simply not found there, and nothing happens.
   * <p>
   * No identity is captured or prepared here, and no task is decorated. This
   * exists only to notice cancellation, so a submission still passes exactly one
   * preparing point, in {@link #execute(Runnable)}, and the prepared task still
   * has exactly one layer to be taken back off.
   *
   * @param <T> the result type of the task
   */
  private final class QueueReclaimingFutureTask<T> extends FutureTask<T> {

    QueueReclaimingFutureTask(Callable<T> callable) {
      super(callable);
    }

    QueueReclaimingFutureTask(Runnable runnable, T value) {
      super(runnable, value);
    }

    /**
     * Reclaims this task's place on the queue, once it is known that the task
     * has been cancelled and so will never be run.
     */
    @Override
    protected void done() {
      if (isCancelled()) {
        remove(this);
      }
    }
  }
}
