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

import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/** An extension of ScheduledThreadPoolExecutor that provides additional
 * functionality. */
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
    return super.schedule(SubjectPreservingTasks.wrap(command), delay, unit);
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
