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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** An extension of ScheduledThreadPoolExecutor that provides additional
 * functionality.
 * <p>
 * A task handed to this pool passes through
 * {@link SubjectPreservingTasks#wrap(Runnable)} in the four scheduling methods
 * below, which are the whole of the pool's ordinary intake:
 * {@code ScheduledThreadPoolExecutor} routes {@code execute} and all three
 * {@code submit} overloads through {@code schedule}, so overriding them as well
 * would prepare a task twice over and leave a second layer behind the one
 * {@link SubjectPreservingTasks#unwrap(Runnable)} takes off. The two bulk
 * submission methods do not reach {@code schedule} with the task a caller
 * submitted -- they reach it with a future of the JDK's own making, wrapped around
 * that task -- so each of them prepares the tasks it is given and opens a
 * prepared-submission scope, and {@code schedule} then passes that future on as
 * it is. Preparing the future instead would put a captured identity outside the
 * object the future releases when it completes or is cancelled, and the tasks a
 * bulk submission abandons would keep their submitter's credentials reachable
 * from the queue for as long as the entry lived.
 * <p>
 * Where that utility wraps -- wherever
 * {@code SubjectUtil.THREAD_INHERITS_SUBJECT} is {@code false}, JDK 25 among
 * those runtimes -- a task runs under the subject current on the thread that
 * scheduled it, and a repeating task under that same subject on every one of its
 * executions. Where the runtime propagates the subject itself, or scheduling
 * carries none at all, the task is passed on unchanged and observes whatever its
 * worker holds: the identity in force when that worker was created, or none
 * where the worker was given none. */
public class HadoopScheduledThreadPoolExecutor extends
    ScheduledThreadPoolExecutor {

  private static final Logger LOG = LoggerFactory
      .getLogger(HadoopScheduledThreadPoolExecutor.class);

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
   * Schedules a task to be run once, after the given delay, prepared on the
   * terms the class contract states.
   * <p>
   * A task that arrives already inside a future while a prepared-submission scope
   * is open on this thread is one of the bulk submission methods below handing on
   * a task it has just prepared -- {@code execute} and the {@code submit}
   * overloads of {@code ScheduledThreadPoolExecutor} both arrive here -- and is
   * scheduled as it is.
   *
   * @param command the task to run
   * @param delay how long to wait before running the task
   * @param unit the unit {@code delay} is given in
   * @return a future representing the pending completion of the task
   */
  @Override
  public ScheduledFuture<?> schedule(Runnable command, long delay,
      TimeUnit unit) {
    if (command instanceof RunnableFuture<?>
        && SubjectPreservingTasks.submissionAlreadyPrepared()) {
      return super.schedule(command, delay, unit);
    }
    return super.schedule(SubjectPreservingTasks.wrap(command), delay, unit);
  }

  /**
   * Schedules a task to be called once, after the given delay, prepared on the
   * terms the class contract states.
   *
   * @param <V> the result type of the task
   * @param callable the task to call
   * @param delay how long to wait before calling the task
   * @param unit the unit {@code delay} is given in
   * @return a future representing the pending result of the task
   */
  @Override
  public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay,
      TimeUnit unit) {
    return super.schedule(SubjectPreservingTasks.wrap(callable), delay, unit);
  }

  /**
   * Schedules a task to be run over and over at the given rate, beginning after
   * the given initial delay, prepared on the terms the class contract states.
   * <p>
   * Where a subject is established at all it is established on every execution,
   * and not the first alone, because it is read once when the schedule is
   * created and re-established on each run.
   *
   * @param command the task to run
   * @param initialDelay how long to wait before the first execution
   * @param period how long to leave between the starts of two executions
   * @param unit the unit {@code initialDelay} and {@code period} are given in
   * @return a future representing the series of executions still to come
   */
  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(Runnable command,
      long initialDelay, long period, TimeUnit unit) {
    return super.scheduleAtFixedRate(SubjectPreservingTasks.wrap(command),
        initialDelay, period, unit);
  }

  /**
   * Schedules a task to be run over and over with the given delay between one
   * execution and the next, beginning after the given initial delay, prepared on
   * the terms the class contract states.
   * <p>
   * Where a subject is established at all it is established on every execution,
   * and not the first alone.
   *
   * @param command the task to run
   * @param initialDelay how long to wait before the first execution
   * @param delay how long to leave between the end of one execution and the
   *        start of the next
   * @param unit the unit {@code initialDelay} and {@code delay} are given in
   * @return a future representing the series of executions still to come
   */
  @Override
  public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command,
      long initialDelay, long delay, TimeUnit unit) {
    return super.scheduleWithFixedDelay(SubjectPreservingTasks.wrap(command),
        initialDelay, delay, unit);
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
   * The tasks this abandons once one has succeeded are cancelled by the pool as it
   * returns, and a cancelled task releases what was prepared for it, so no
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
