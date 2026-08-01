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
 * A scheduled executor service that carries the scheduling thread's JAAS
 * subject into every task it passes to the scheduled executor service it
 * wraps.
 * <p>
 * The four scheduling methods send their task through
 * {@link SubjectPreservingTasks} on its way in, always on the thread doing the
 * scheduling, so a task runs under the identity of whoever scheduled it rather
 * than the identity current when a worker was created. The inherited submission
 * methods cover every other way a task can be handed over, and because this
 * class forwards to a separate service rather than routing back through itself,
 * every task is wrapped exactly once whichever way it arrives.
 * <p>
 * A repeating task re-establishes the subject of the thread that scheduled it on
 * every one of its executions, for as long as it goes on repeating: the subject
 * is read once, when the schedule is created, so however much later a repetition
 * falls it runs under the identity the schedule was created under. Long-lived
 * work whose purpose is to maintain a caller's credentials depends on that.
 * <p>
 * Nothing else is altered. A delay, a period and a unit are passed on as given,
 * so the wrapped service keeps sole charge of its timing and still refuses an
 * argument it would have refused before, and the {@link ScheduledFuture} handed
 * back is the one it returned rather than a stand-in, so cancelling a task,
 * waiting on its result and reading its remaining delay all behave as they did.
 * <p>
 * It forwards rather than extends because {@link HadoopThreadPoolExecutor} is
 * final and the single-thread scheduled services {@link HadoopExecutors} obtains
 * from {@link java.util.concurrent.Executors} are implementations Hadoop cannot
 * see into, with particular semantics it has deliberately not reproduced.
 * <p>
 * Only such a service needs to be wrapped. A pool Hadoop owns, such as
 * {@link HadoopScheduledThreadPoolExecutor}, already carries the identity into
 * the tasks handed to it, so placing this in front of one would ask for the
 * same thing twice: {@link SubjectPreservingTasks#wrap(Runnable)} hands back a task it
 * has already prepared, so nothing is layered twice and nothing that reports on
 * a task is misled, but the second request buys nothing either.
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

  /**
   * Schedules a task to be run once, after the given delay, under the subject
   * current on this thread.
   *
   * @param command the task to run
   * @param delay how long to wait before running the task
   * @param unit the unit {@code delay} is given in
   * @return a future representing the pending completion of the task, and from
   *         which the delay still to run can be read
   */
  @Override
  public ScheduledFuture<?> schedule(Runnable command, long delay,
      TimeUnit unit) {
    return scheduledDelegate.schedule(SubjectPreservingTasks.wrap(command),
        delay, unit);
  }

  /**
   * Schedules a task to be called once, after the given delay, under the
   * subject current on this thread.
   *
   * @param <V> the result type of the task
   * @param callable the task to call
   * @param delay how long to wait before calling the task
   * @param unit the unit {@code delay} is given in
   * @return a future representing the pending result of the task, and from
   *         which the delay still to run can be read
   */
  @Override
  public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay,
      TimeUnit unit) {
    return scheduledDelegate.schedule(SubjectPreservingTasks.wrap(callable),
        delay, unit);
  }

  /**
   * Schedules a task to be run over and over at the given rate, under the
   * subject current on this thread, beginning after the given initial delay.
   * <p>
   * Every execution runs under that subject, and not the first alone.
   *
   * @param command the task to run
   * @param initialDelay how long to wait before the first execution
   * @param period how long to leave between the starts of two executions
   * @param unit the unit {@code initialDelay} and {@code period} are given in
   * @return a future representing the series of executions still to come, which
   *         ends only once it is cancelled or an execution fails
   */
  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(Runnable command,
      long initialDelay, long period, TimeUnit unit) {
    return scheduledDelegate.scheduleAtFixedRate(
        SubjectPreservingTasks.wrap(command), initialDelay, period, unit);
  }

  /**
   * Schedules a task to be run over and over with the given delay between one
   * execution and the next, under the subject current on this thread, beginning
   * after the given initial delay.
   * <p>
   * Every execution runs under that subject, and not the first alone.
   *
   * @param command the task to run
   * @param initialDelay how long to wait before the first execution
   * @param delay how long to leave between the end of one execution and the
   *        start of the next
   * @param unit the unit {@code initialDelay} and {@code delay} are given in
   * @return a future representing the series of executions still to come, which
   *         ends only once it is cancelled or an execution fails
   */
  @Override
  public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command,
      long initialDelay, long delay, TimeUnit unit) {
    return scheduledDelegate.scheduleWithFixedDelay(
        SubjectPreservingTasks.wrap(command), initialDelay, delay, unit);
  }
}
