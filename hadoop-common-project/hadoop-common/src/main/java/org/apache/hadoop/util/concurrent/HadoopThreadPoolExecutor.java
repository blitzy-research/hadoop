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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


/** An extension of ThreadPoolExecutor that provides additional functionality.
 * <p>
 * A task handed to this pool is prepared by {@link SubjectPreservingTasks} on the
 * thread that hands it over, so that it carries that thread's JAAS subject. Every
 * way into the pool prepares: {@link #execute(Runnable)}, all three
 * {@code submit} overloads and both overloads each of {@code invokeAll} and
 * {@code invokeAny}. Where that utility prepares -- wherever
 * {@code SubjectUtil.THREAD_INHERITS_SUBJECT} is {@code false}, JDK 25 among
 * those runtimes -- the task runs under the subject current on the thread that
 * handed it over, even on a worker another submitter brought into existence.
 * Where the runtime propagates the subject itself, or a submission carries none
 * at all, the task is passed on unchanged and observes whatever its worker
 * holds: the identity in force when that worker was created, or none where the
 * worker was given none.
 * <p>
 * What is prepared is the task a submission asks to have run, and never the
 * future the pool makes to run it with. The submission methods prepare the body
 * they are given and open a prepared-submission scope, so that
 * {@link #execute(Runnable)} queues that future as itself, and the object on the
 * queue is then the one the pool would have queued with nothing prepared at all.
 * Two things follow, and both are why preparation is placed there. A captured
 * identity lives no longer than the task it was captured for, because a future
 * releases the task it was given as soon as it completes or is cancelled: a
 * caller that cancels its work and walks away leaves no credentials reachable
 * from the queue. And every operation named in terms of the queued object goes on
 * behaving as it did: {@link #purge()} recognises a cancelled future,
 * {@link #shutdownNow()} hands back what was submitted, {@link #remove(Runnable)}
 * finds it, and the debug line below names the class it always named.
 * <p>
 * A task handed straight to {@link #execute(Runnable)} has no future of its own,
 * so there the prepared task is what reaches the queue. The code that describes a
 * queued task rather than running it therefore takes that single layer back off
 * first -- {@link #beforeExecute(Thread, Runnable)} here, {@code ExecutorHelper}
 * from {@link #afterExecute(Runnable, Throwable)}, and
 * {@link #shutdownNow()}/{@link #remove(Runnable)} on the caller's behalf.
 *  */
public final class HadoopThreadPoolExecutor extends ThreadPoolExecutor {

  private static final Logger LOG = LoggerFactory
      .getLogger(HadoopThreadPoolExecutor.class);

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
   * Hands a task to the pool, prepared on the terms the class contract states.
   * <p>
   * A task that arrives already inside a future while a prepared-submission scope
   * is open on this thread is one of the submission methods below handing on a
   * body it has just prepared, and is queued as it is. Everything else is
   * prepared here, a task a caller hands straight to this method included, so
   * that a submission passes exactly one preparing point and a prepared task has
   * exactly one layer for {@link SubjectPreservingTasks#unwrap(Runnable)} to take
   * back off.
   *
   * @param command the task to run
   */
  @Override
  public void execute(Runnable command) {
    if (command instanceof RunnableFuture<?>
        && SubjectPreservingTasks.submissionAlreadyPrepared()) {
      super.execute(command);
      return;
    }
    super.execute(SubjectPreservingTasks.wrap(command));
  }

  /**
   * Submits a task for its result, prepared on the terms the class contract
   * states.
   *
   * @param <T> the result type of the task
   * @param task the task to call
   * @return a future representing the pending result of the task
   */
  @Override
  public <T> Future<T> submit(Callable<T> task) {
    Callable<T> prepared = SubjectPreservingTasks.wrap(task);
    boolean enclosing = SubjectPreservingTasks.beginPreparedSubmission();
    try {
      return super.submit(prepared);
    } finally {
      SubjectPreservingTasks.endPreparedSubmission(enclosing);
    }
  }

  /**
   * Submits a task for a result given in advance, prepared on the terms the class
   * contract states.
   *
   * @param <T> the type of the given result
   * @param task the task to run
   * @param result the result to return once the task has run
   * @return a future representing the pending completion of the task
   */
  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    Runnable prepared = SubjectPreservingTasks.wrap(task);
    boolean enclosing = SubjectPreservingTasks.beginPreparedSubmission();
    try {
      return super.submit(prepared, result);
    } finally {
      SubjectPreservingTasks.endPreparedSubmission(enclosing);
    }
  }

  /**
   * Submits a task for its completion, prepared on the terms the class contract
   * states.
   *
   * @param task the task to run
   * @return a future representing the pending completion of the task
   */
  @Override
  public Future<?> submit(Runnable task) {
    Runnable prepared = SubjectPreservingTasks.wrap(task);
    boolean enclosing = SubjectPreservingTasks.beginPreparedSubmission();
    try {
      return super.submit(prepared);
    } finally {
      SubjectPreservingTasks.endPreparedSubmission(enclosing);
    }
  }

  /**
   * Submits every task and waits for all of them, each prepared on the terms the
   * class contract states.
   *
   * @param <T> the result type of the tasks
   * @param tasks the tasks to call
   * @return the futures of the tasks, in the order the collection yielded them
   * @throws InterruptedException if the wait is interrupted
   */
  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
      throws InterruptedException {
    Collection<Callable<T>> prepared = SubjectPreservingTasks.wrapEach(tasks);
    boolean enclosing = SubjectPreservingTasks.beginPreparedSubmission();
    try {
      return super.invokeAll(prepared);
    } finally {
      SubjectPreservingTasks.endPreparedSubmission(enclosing);
    }
  }

  /**
   * Submits every task and waits for all of them or for the given timeout,
   * whichever comes first, each prepared on the terms the class contract states.
   * <p>
   * The wait is the pool's own, measured from the moment it is given the prepared
   * tasks.
   *
   * @param <T> the result type of the tasks
   * @param tasks the tasks to call
   * @param timeout how long to wait
   * @param unit the unit {@code timeout} is given in
   * @return the futures of the tasks, in the order the collection yielded them
   * @throws InterruptedException if the wait is interrupted
   */
  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks,
      long timeout, TimeUnit unit) throws InterruptedException {
    Collection<Callable<T>> prepared = SubjectPreservingTasks.wrapEach(tasks);
    boolean enclosing = SubjectPreservingTasks.beginPreparedSubmission();
    try {
      return super.invokeAll(prepared, timeout, unit);
    } finally {
      SubjectPreservingTasks.endPreparedSubmission(enclosing);
    }
  }

  /**
   * Submits every task and returns the result of whichever finishes first, each
   * prepared on the terms the class contract states.
   * <p>
   * The tasks this abandons once one has succeeded are cancelled by the pool as
   * it returns, and a cancelled task releases what was prepared for it, so no
   * identity outlives the call on the queue.
   *
   * @param <T> the result type of the tasks
   * @param tasks the tasks to call
   * @return the result of a task that completed without throwing
   * @throws InterruptedException if the wait is interrupted
   * @throws ExecutionException if no task completed without throwing
   */
  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
      throws InterruptedException, ExecutionException {
    Collection<Callable<T>> prepared = SubjectPreservingTasks.wrapEach(tasks);
    boolean enclosing = SubjectPreservingTasks.beginPreparedSubmission();
    try {
      return super.invokeAny(prepared);
    } finally {
      SubjectPreservingTasks.endPreparedSubmission(enclosing);
    }
  }

  /**
   * Submits every task and returns the result of whichever finishes first within
   * the given timeout, each prepared on the terms the class contract states.
   * <p>
   * The wait is the pool's own, measured from the moment it is given the prepared
   * tasks, and the tasks a timeout abandons release what was prepared for them.
   *
   * @param <T> the result type of the tasks
   * @param tasks the tasks to call
   * @param timeout how long to wait
   * @param unit the unit {@code timeout} is given in
   * @return the result of a task that completed without throwing
   * @throws InterruptedException if the wait is interrupted
   * @throws ExecutionException if no task completed without throwing
   * @throws TimeoutException if none had completed when the timeout elapsed
   */
  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout,
      TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    Collection<Callable<T>> prepared = SubjectPreservingTasks.wrapEach(tasks);
    boolean enclosing = SubjectPreservingTasks.beginPreparedSubmission();
    try {
      return super.invokeAny(prepared, timeout, unit);
    } finally {
      SubjectPreservingTasks.endPreparedSubmission(enclosing);
    }
  }

  /**
   * Stops the pool at once and hands back the tasks that never began, as they
   * were submitted.
   * <p>
   * A task handed straight to {@link #execute(Runnable)} is queued prepared, so
   * without this the list would hold the object that carries the identity rather
   * than the object the caller passed in. Anything else on the queue is returned
   * exactly as the pool held it.
   *
   * @return the tasks that were waiting to run, as they were submitted
   */
  @Override
  public List<Runnable> shutdownNow() {
    List<Runnable> pending = super.shutdownNow();
    pending.replaceAll(SubjectPreservingTasks::unwrap);
    return pending;
  }

  /**
   * Takes a task off the queue, whether or not it was prepared on its way in.
   * <p>
   * The queue is searched for the task itself first, which is what a task that
   * reached the pool inside a future -- or one put on the queue directly, as
   * {@code ValueQueue.submitRefillTask} does -- is held as. A task handed to
   * {@link #execute(Runnable)} under an identity is held prepared instead, so it
   * is looked for behind that single layer, and the caller's own reference goes on
   * being enough to withdraw it.
   *
   * @param task the task to withdraw; may be {@code null}
   * @return whether the task was on the queue and has been taken off it
   */
  @Override
  public boolean remove(Runnable task) {
    if (super.remove(task)) {
      return true;
    }
    if (task == null) {
      return false;
    }
    for (Runnable queued : getQueue()) {
      if (SubjectPreservingTasks.unwrap(queued) == task) {
        return super.remove(queued);
      }
    }
    return false;
  }

  @Override
  protected void beforeExecute(Thread t, Runnable r) {
    if (LOG.isDebugEnabled()) {
      // Report the task as it was submitted: a prepared task is not of the class
      // the submitter passed in, and this line is read by operators.
      LOG.debug("beforeExecute in thread: " + Thread.currentThread()
          .getName() + ", runnable type: "
          + SubjectPreservingTasks.unwrap(r).getClass().getName());
    }
  }

  @Override
  protected void afterExecute(Runnable r, Throwable t) {
    super.afterExecute(r, t);
    ExecutorHelper.logThrowableFromAfterExecute(r, t);
  }
}
