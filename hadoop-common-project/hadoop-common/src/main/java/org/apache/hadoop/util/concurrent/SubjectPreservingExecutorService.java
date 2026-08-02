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

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ForwardingExecutorService;

import org.apache.hadoop.classification.InterfaceAudience;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.Objects.requireNonNull;

/**
 * An executor service that carries the submitting thread's JAAS subject into
 * every task it passes to the executor service it wraps.
 * <p>
 * Each task goes through
 * {@link SubjectPreservingTasks#wrap(Runnable)} or
 * {@link SubjectPreservingTasks#wrap(Callable)} on its way in, and
 * always on the submitting thread, so a task runs under the identity of its own
 * submitter rather than the identity current when a worker was created. That is
 * what keeps a pool correct when one reused worker serves several submitters.
 * Where that utility has nothing to establish -- on a runtime that hands a new
 * thread its creator's subject, or for a submission carrying no subject at all
 * -- the task is passed on exactly as it arrived.
 * <p>
 * Nothing else is altered. Every other call, the whole of the service's
 * lifecycle included, goes straight to the wrapped service, which keeps its own
 * behaviour in full.
 * <p>
 * The methods that take a whole collection of tasks prepare each task in it and
 * hand the wrapped service the prepared tasks, in the order the collection
 * yields them. The collection passed in is left as it was, the futures returned
 * line up with it one for one, in its order, and a collection the wrapped
 * service would have refused is still refused by it.
 * <p>
 * It forwards rather than extends because {@link HadoopThreadPoolExecutor} is
 * final, and because the single-thread services {@link HadoopExecutors} obtains
 * from {@link java.util.concurrent.Executors} are implementations Hadoop cannot
 * see into, with particular semantics it has deliberately not reproduced;
 * forwarding adds the propagation and leaves those semantics untouched.
 * <p>
 * Such a service is what this class is for, and the only thing it is for. It is
 * not a general way to add propagation in front of an arbitrary executor: a pool
 * Hadoop owns already prepares the tasks handed to it, and putting this in front
 * of one would not merely be redundant. A task prepared here and then submitted
 * with {@link ExecutorService#submit(Runnable)} reaches that pool inside a new
 * future the pool builds around it, which the pool prepares in turn, so the task
 * would end up behind two layers where
 * {@link SubjectPreservingTasks#unwrap(Runnable)} takes one back off.
 */
@InterfaceAudience.Private
public class SubjectPreservingExecutorService extends ForwardingExecutorService {

  private final ExecutorService delegate;

  /**
   * Wraps an executor service so that the tasks submitted to it carry the
   * subject of the thread that submits them.
   *
   * @param delegate the executor service to forward every call to
   * @throws NullPointerException if {@code delegate} is {@code null}
   */
  public SubjectPreservingExecutorService(ExecutorService delegate) {
    this.delegate = requireNonNull(delegate);
  }

  @Override
  protected ExecutorService delegate() {
    return delegate;
  }

  /**
   * Submits a task to be called under the subject current on this thread.
   *
   * @param <T> the result type of the task
   * @param task the task to submit
   * @return a future representing the pending result of the task
   */
  @Override
  public <T> Future<T> submit(Callable<T> task) {
    return super.submit(SubjectPreservingTasks.wrap(task));
  }

  /**
   * Submits a task to be run under the subject current on this thread.
   *
   * @param <T> the type of the given result
   * @param task the task to submit
   * @param result the result to return once the task has run
   * @return a future that yields {@code result} once the task has run
   */
  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    return super.submit(SubjectPreservingTasks.wrap(task), result);
  }

  /**
   * Submits a task to be run under the subject current on this thread.
   *
   * @param task the task to submit
   * @return a future representing the pending completion of the task
   */
  @Override
  public Future<?> submit(Runnable task) {
    return super.submit(SubjectPreservingTasks.wrap(task));
  }

  /**
   * Hands a task over to be run under the subject current on this thread.
   *
   * @param command the task to run
   */
  @Override
  public void execute(Runnable command) {
    super.execute(SubjectPreservingTasks.wrap(command));
  }

  /**
   * Prepares every task in {@code tasks} so that each runs under the subject
   * current on this thread.
   * <p>
   * The preparation happens here, on the thread calling one of the bulk
   * submission methods, which is what makes the subject read the submitting
   * thread's own. {@code null} is handed on as it is, so that a wrapped service
   * given {@code null} still refuses it exactly as it would have.
   *
   * @param <T> the result type of the tasks
   * @param tasks the tasks to prepare; may be {@code null}
   * @return the prepared tasks, in the order {@code tasks} yields them, or
   *         {@code null} when {@code tasks} is {@code null}
   */
  private static <T> Collection<Callable<T>> prepare(
      Collection<? extends Callable<T>> tasks) {
    if (tasks == null) {
      return null;
    }
    Collection<Callable<T>> prepared = new ArrayList<>(tasks.size());
    for (Callable<T> task : tasks) {
      prepared.add(SubjectPreservingTasks.wrap(task));
    }
    return prepared;
  }

  /**
   * Runs every one of the given tasks under the subject current on this
   * thread, and returns once all of them have finished.
   *
   * @param <T> the result type of the tasks
   * @param tasks the tasks to run
   * @return a list of futures, in the order the tasks were given, each of them
   *         completed
   * @throws InterruptedException if the wait is interrupted, in which case the
   *         unfinished tasks are cancelled
   */
  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
      throws InterruptedException {
    return super.invokeAll(prepare(tasks));
  }

  /**
   * Runs every one of the given tasks under the subject current on this
   * thread, and returns once all of them have finished or the wait has run
   * out.
   * <p>
   * The wait is the wrapped service's own, measured from the moment it is
   * called with the prepared tasks, and it governs the running of them exactly
   * as it does without this class in the way.
   *
   * @param <T> the result type of the tasks
   * @param tasks the tasks to run
   * @param timeout how long to wait
   * @param unit the unit {@code timeout} is given in
   * @return a list of futures, in the order the tasks were given, each of them
   *         either completed or cancelled
   * @throws InterruptedException if the wait is interrupted, in which case the
   *         unfinished tasks are cancelled
   */
  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks,
      long timeout, TimeUnit unit) throws InterruptedException {
    return super.invokeAll(prepare(tasks), timeout, unit);
  }

  /**
   * Runs the given tasks under the subject current on this thread and returns
   * the result of one that finished without failing.
   * <p>
   * The wrapped service decides for itself how many of the prepared tasks it
   * needs to run, exactly as it does without this class in the way.
   *
   * @param <T> the result type of the tasks
   * @param tasks the tasks to run
   * @return the result of one task that finished without failing
   * @throws InterruptedException if the wait is interrupted
   * @throws ExecutionException if no task finished without failing
   */
  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
      throws InterruptedException, ExecutionException {
    return super.invokeAny(prepare(tasks));
  }

  /**
   * Runs the given tasks under the subject current on this thread and returns
   * the result of one that finished without failing before the wait ran out.
   * <p>
   * As with the untimed form, the wrapped service decides how many of the
   * prepared tasks it needs to run; and as with the other timed form, the wait
   * is the wrapped service's own.
   *
   * @param <T> the result type of the tasks
   * @param tasks the tasks to run
   * @param timeout how long to wait
   * @param unit the unit {@code timeout} is given in
   * @return the result of one task that finished without failing
   * @throws InterruptedException if the wait is interrupted
   * @throws ExecutionException if no task finished without failing
   * @throws TimeoutException if the wait ran out before any task finished
   */
  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout,
      TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return super.invokeAny(prepare(tasks), timeout, unit);
  }
}
