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
 * Scheduling a task is the one thing {@link ScheduledExecutorService} adds to
 * an ordinary executor service, and it adds it in exactly four methods. Each of
 * those four sends its task through the wrapping methods of
 * {@link SubjectPreservingTasks} on its way in. Those methods read the subject
 * of the thread that calls them, and each is called here from the thread doing
 * the scheduling, so a task runs under the identity of whoever scheduled it
 * rather than under the identity that happened to be current when a worker
 * thread was created. That distinction is what keeps the result correct for a
 * service that schedules for several callers on one reused worker.
 * <p>
 * The submission methods of {@link SubjectPreservingExecutorService} are
 * inherited exactly as they are, and they cover every other way a task can be
 * handed over. They remain correct here because this class forwards to a
 * separate service: an inherited method wraps a task once and then passes it to
 * that service, whose own routing of the task stays inside itself and never
 * comes back through the four methods below. Every task is therefore wrapped
 * once, whichever way it arrives.
 * <p>
 * A repeating task re-establishes the subject of the thread that scheduled it
 * on every one of its executions, for as long as it goes on repeating. The
 * subject is read once, when the schedule is created, so the identity a
 * repetition runs under is the identity the schedule was created under, however
 * much later that repetition falls. Long-lived work whose whole purpose is to
 * maintain a caller's credentials depends on precisely that.
 * <p>
 * Nothing else is altered. A delay, a period and a unit are passed on just as
 * they were given, so the wrapped service keeps sole charge of its timing and
 * still refuses an argument it would have refused before. The future handed
 * back is the one the wrapped service returned, not a stand-in for it, so
 * cancelling a task, waiting on its result and reading its remaining delay all
 * behave as they did.
 * <p>
 * It forwards rather than extends, for two reasons.
 * {@link HadoopThreadPoolExecutor} is final and so cannot be extended at all.
 * And the single-thread scheduled services {@link HadoopExecutors} obtains from
 * {@link java.util.concurrent.Executors} are implementations Hadoop cannot see
 * into, with particular semantics it has deliberately not reproduced; putting a
 * forwarding service in front of one adds the propagation while leaving those
 * semantics untouched.
 * <p>
 * Only such a service should be wrapped. A pool Hadoop owns, such as
 * {@link HadoopScheduledThreadPoolExecutor}, already wraps the tasks handed to
 * it, so placing this in front of one would wrap them a second time. Because
 * {@link SubjectPreservingTasks#unwrap(Runnable)} removes a single layer by
 * design, code that reports on a task instead of running it would then be left
 * looking at a wrapper rather than at the task submitted.
 */
@InterfaceAudience.Private
public class SubjectPreservingScheduledExecutorService
    extends SubjectPreservingExecutorService implements ScheduledExecutorService {

  /** The scheduled executor service every scheduling call is forwarded to. */
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
