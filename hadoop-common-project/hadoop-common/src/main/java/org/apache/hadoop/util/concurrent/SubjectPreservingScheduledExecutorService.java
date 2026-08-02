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

import org.apache.hadoop.classification.InterfaceAudience;

import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * A scheduled executor service that prepares the tasks it forwards so that they
 * carry the scheduling thread's JAAS subject.
 * <p>
 * The four scheduling methods send their task through
 * {@link SubjectPreservingTasks} on its way in, always on the thread doing the
 * scheduling, and the inherited submission methods cover every other way a task
 * can reach the service this class wraps. Where that utility wraps -- wherever
 * {@code SubjectUtil.THREAD_INHERITS_SUBJECT} is {@code false}, JDK 25 among
 * those runtimes -- a task runs under the identity of whoever scheduled it
 * rather than the identity current when a worker was created, and a repeating
 * task under that same identity on every one of its executions rather than on
 * the first alone, because the subject is read once when the schedule is created
 * and re-established on each run. Long-lived work whose purpose is to maintain a
 * caller's credentials depends on that. Where the runtime propagates the subject
 * itself, or scheduling carries none at all, the task is forwarded as it arrived
 * and observes whatever its worker holds.
 * <p>
 * Nothing else is altered. A delay, a period and a unit are passed on as given,
 * so the wrapped service keeps sole charge of its timing, and the
 * {@link ScheduledFuture} handed back is the one it returned rather than a
 * stand-in, so cancelling a task, waiting on its result and reading its
 * remaining delay all reach that service directly.
 * <p>
 * Why it forwards rather than extends, and why it belongs in front of a
 * scheduled service {@link HadoopExecutors} obtains from
 * {@link java.util.concurrent.Executors} and not in front of a pool Hadoop owns
 * such as {@link HadoopScheduledThreadPoolExecutor}, is set out on
 * {@link SubjectPreservingExecutorService}.
 */
@InterfaceAudience.Private
public class SubjectPreservingScheduledExecutorService
    extends SubjectPreservingExecutorService implements ScheduledExecutorService {

  private final ScheduledExecutorService scheduledDelegate;

  /**
   * Wraps a scheduled executor service so that the tasks scheduled on it carry
   * the subject of the thread that schedules them.
   *
   * @param delegate the scheduled executor service to forward every call to
   * @throws NullPointerException if {@code delegate} is {@code null}
   */
  public SubjectPreservingScheduledExecutorService(
      ScheduledExecutorService delegate) {
    super(delegate);
    this.scheduledDelegate = delegate;
  }

  @Override
  public ScheduledFuture<?> schedule(Runnable command, long delay,
      TimeUnit unit) {
    return scheduledDelegate.schedule(SubjectPreservingTasks.wrap(command),
        delay, unit);
  }

  @Override
  public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay,
      TimeUnit unit) {
    return scheduledDelegate.schedule(SubjectPreservingTasks.wrap(callable),
        delay, unit);
  }

  /**
   * Schedules a task to be run over and over at the given rate, beginning after
   * the given initial delay, prepared on the terms the class contract states.
   *
   * @param command the task to run
   * @param initialDelay how long to wait before the first execution
   * @param period how long to leave between the starts of two executions
   * @param unit the unit {@code initialDelay} and {@code period} are given in
   * @return a future representing the series of executions still to come, which
   *         ends only when it is cancelled, when the wrapped service terminates
   *         and cancels it in turn, or when an execution throws
   */
  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(Runnable command,
      long initialDelay, long period, TimeUnit unit) {
    return scheduledDelegate.scheduleAtFixedRate(
        SubjectPreservingTasks.wrap(command), initialDelay, period, unit);
  }

  /**
   * Schedules a task to be run over and over with the given delay between one
   * execution and the next, beginning after the given initial delay, prepared on
   * the terms the class contract states.
   *
   * @param command the task to run
   * @param initialDelay how long to wait before the first execution
   * @param delay how long to leave between the end of one execution and the
   *        start of the next
   * @param unit the unit {@code initialDelay} and {@code delay} are given in
   * @return a future representing the series of executions still to come, which
   *         ends only when it is cancelled, when the wrapped service terminates
   *         and cancels it in turn, or when an execution throws
   */
  @Override
  public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command,
      long initialDelay, long delay, TimeUnit unit) {
    return scheduledDelegate.scheduleWithFixedDelay(
        SubjectPreservingTasks.wrap(command), initialDelay, delay, unit);
  }
}
