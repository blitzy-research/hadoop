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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** An extension of ScheduledThreadPoolExecutor that provides additional
 * functionality. */
public class HadoopScheduledThreadPoolExecutor extends
    ScheduledThreadPoolExecutor {

  private static final Logger LOG = LoggerFactory
      .getLogger(HadoopScheduledThreadPoolExecutor.class);

  /**
   * The queue entries that a submission for one result, in progress on this
   * thread, will owe a reclamation once it ends, or {@code null} when no such
   * submission is in progress here.
   * <p>
   * This holds no identity and propagates nothing: it is a note the pool keeps
   * to itself, for the extent of one call on one thread, of what that call put
   * on the queue on its behalf, and it is put back exactly as it was found when
   * the call ends. {@link #invokeAny(Collection)} explains what the note is for.
   */
  private static final ThreadLocal<List<Runnable>> ENTRIES_TO_RECLAIM =
      new ThreadLocal<>();

  public HadoopScheduledThreadPoolExecutor(int corePoolSize) {
    super(corePoolSize);
  }

  public HadoopScheduledThreadPoolExecutor(int corePoolSize,
      ThreadFactory threadFactory) {
    super(corePoolSize, threadFactory);
  }

  public HadoopScheduledThreadPoolExecutor(int corePoolSize,
      RejectedExecutionHandler handler) {
    super(corePoolSize, handler);
  }

  public HadoopScheduledThreadPoolExecutor(int corePoolSize,
      ThreadFactory threadFactory,
      RejectedExecutionHandler handler) {
    super(corePoolSize, threadFactory, handler);
  }

  /**
   * Schedules a task to run once, under the subject of the calling thread.
   * <p>
   * The subject is read here, on the scheduling thread, rather than when a
   * worker thread is created, so the task runs as whoever scheduled it even on a
   * pool that reuses its threads, and it is established for the task's own
   * extent, so the worker carries none of it into the task after it. Scheduling
   * done by a thread that holds no identity is prepared in the same way, and the
   * task then runs with none, which is what its scheduler would have observed.
   * {@link SubjectPreservingTasks} describes this, and why an identity has to be
   * carried across a thread boundary at all.
   * <p>
   * Every way of giving work to this pool arrives here or at one of the other
   * scheduling methods, because the JDK routes each {@code execute} and
   * {@code submit} overload through them. When the work handed over is a future
   * this pool did not make, and a submission for one result is in progress on
   * this thread, the queue entry that carries it is noted down so that
   * {@link #invokeAny(Collection)} can reclaim it if that submission abandons
   * it. The note is set aside for the scheduling itself, because a rejection
   * policy may run the task here and from there hand over work of its own, which
   * belongs to whoever submitted it and is not the enclosing submission's to
   * reclaim.
   *
   * @param command the task to run
   * @param delay how long to wait before the task runs
   * @param unit the unit of {@code delay}
   * @return a future that reports the task's completion and yields
   *         {@code null}
   */
  @Override
  public ScheduledFuture<?> schedule(Runnable command, long delay,
      TimeUnit unit) {
    Runnable prepared = SubjectPreservingTasks.wrap(command);
    List<Runnable> reclaimable = reclamationNotesFor(command, prepared);
    if (reclaimable == null) {
      return super.schedule(prepared, delay, unit);
    }
    ENTRIES_TO_RECLAIM.remove();
    ScheduledFuture<?> scheduled;
    try {
      scheduled = super.schedule(prepared, delay, unit);
    } finally {
      ENTRIES_TO_RECLAIM.set(reclaimable);
    }
    if (scheduled instanceof Runnable) {
      reclaimable.add((Runnable) scheduled);
    }
    return scheduled;
  }

  /**
   * Answers which submission for one result, if any, is owed a reclamation of
   * the entry {@code prepared} will be queued as, should it be abandoned there.
   * <p>
   * Only a prepared entry standing for a future of someone else's making can be
   * owed one. Work handed over as it is carries no identity to release, and work
   * handed over while no such submission is in progress on this thread belongs
   * to no submission that could abandon it.
   *
   * @param command the task as it was handed over
   * @param prepared the form of it that is scheduled
   * @return the notes of the submission that queued it, or {@code null} if none
   */
  private List<Runnable> reclamationNotesFor(Runnable command,
      Runnable prepared) {
    if (prepared == command || !(command instanceof RunnableFuture<?>)) {
      return null;
    }
    return ENTRIES_TO_RECLAIM.get();
  }

  /**
   * Schedules a task to be called once, under the subject of the calling
   * thread.
   * <p>
   * This is the {@link Callable} counterpart of
   * {@link #schedule(Runnable, long, TimeUnit)} and reads the subject on the same
   * terms. A future reports a failed task through an
   * {@link java.util.concurrent.ExecutionException}, and the exception the task
   * threw becomes the direct cause of that, with no further exception wrapping it
   * for a caller to unwrap.
   *
   * @param <V> the result type of the task
   * @param callable the task to call
   * @param delay how long to wait before the task is called
   * @param unit the unit of {@code delay}
   * @return a future that yields the task's result
   */
  @Override
  public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay,
      TimeUnit unit) {
    return super.schedule(SubjectPreservingTasks.wrap(callable), delay, unit);
  }

  /**
   * Schedules a task to run repeatedly at a fixed rate, under the subject of
   * the calling thread.
   * <p>
   * The subject is read once, when the task is scheduled, and re-established
   * for every execution: the schedule was created under that identity, so
   * every run of it belongs to that identity too.
   *
   * @param command the task to run
   * @param initialDelay how long to wait before the first run
   * @param period how long from the start of one run to the start of the next
   * @param unit the unit of {@code initialDelay} and {@code period}
   * @return a future that completes only when the task is cancelled or fails
   */
  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(Runnable command,
      long initialDelay, long period, TimeUnit unit) {
    return super.scheduleAtFixedRate(SubjectPreservingTasks.wrap(command),
        initialDelay, period, unit);
  }

  /**
   * Schedules a task to run repeatedly with a fixed delay between runs, under
   * the subject of the calling thread.
   * <p>
   * As with {@link #scheduleAtFixedRate(Runnable, long, long, TimeUnit)}, the
   * subject is read once, when the task is scheduled, and re-established for
   * every execution.
   *
   * @param command the task to run
   * @param initialDelay how long to wait before the first run
   * @param delay how long from the end of one run to the start of the next
   * @param unit the unit of {@code initialDelay} and {@code delay}
   * @return a future that completes only when the task is cancelled or fails
   */
  @Override
  public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command,
      long initialDelay, long delay, TimeUnit unit) {
    return super.scheduleWithFixedDelay(SubjectPreservingTasks.wrap(command),
        initialDelay, delay, unit);
  }

  /**
   * Runs the given tasks until one of them completes, returning its result and
   * letting go of the identities the abandoned ones were prepared with.
   * <p>
   * The JDK runs a submission for one result through a completion service, which
   * hands this pool a future of its own making to run, so that the first task to
   * finish can be recognised. Scheduling that future puts an entry on the queue
   * which holds it, prepared with the submitter's identity, and when the
   * submission ends it cancels only the futures it was given back -- never the
   * entries this pool queued to hold them, which nothing cancels and which
   * {@link #purge()} therefore does not recognise either. On a pool whose
   * workers are all occupied, or whose queue has stopped draining, every
   * abandoned entry would keep its submitter's subject, and the credentials in
   * it, reachable for as long as it stayed there.
   * <p>
   * This is closed by reclaiming, once the submission has ended, the entries it
   * queued that are still waiting. That happens after the JDK has cancelled the
   * futures it is abandoning, so an entry still waiting then holds work that
   * will not be run and that no caller is left holding. Which task wins, how
   * long the pool waits, and what is thrown are all the JDK's own and are
   * unchanged.
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
   * pool's own so that the entry is matched as the object it is.
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
}
