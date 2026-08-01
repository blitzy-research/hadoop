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
 * Each task goes through {@link SubjectPreservingTasks#wrap(Runnable)} or
 * {@link SubjectPreservingTasks#wrap(Callable)} on its way in. Those methods
 * read the subject on the thread that calls them, and every one of them is
 * called here from the thread that is submitting the task. The identity a task
 * runs under is therefore the identity of its own submitter, and not the
 * identity that happened to be current when a worker thread was created. That
 * distinction is what keeps the result correct for a pool that serves several
 * submitters from one reused worker.
 * <p>
 * Nothing else is altered. Every other call, the whole of the shutdown
 * lifecycle included, goes straight to the wrapped service, which keeps its own
 * behaviour in full. This class adds the propagation of an identity and changes
 * nothing besides.
 * <p>
 * It forwards rather than extends, for two reasons.
 * {@link HadoopThreadPoolExecutor} is final and so cannot be extended at all.
 * And the single-thread services {@link HadoopExecutors} obtains from
 * {@link java.util.concurrent.Executors} are implementations Hadoop cannot see
 * into, with particular semantics it has deliberately not reproduced; putting a
 * forwarding service in front of one adds the propagation while leaving those
 * semantics untouched.
 * <p>
 * Only such a service should be wrapped. A pool Hadoop owns already wraps the
 * tasks handed to it, so placing this in front of one would wrap them a second
 * time. Because {@link SubjectPreservingTasks#unwrap(Runnable)} removes a
 * single layer by design, code that reports on a task instead of running it
 * would then be left looking at a wrapper rather than at the task submitted.
 */
@InterfaceAudience.Private
public class SubjectPreservingExecutorService extends ForwardingExecutorService {

  /** The executor service every call is forwarded to. */
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

  /**
   * Returns the executor service every call is forwarded to.
   *
   * @return the wrapped executor service, never {@code null}
   */
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
   * Runs every one of the given tasks under the subject current on this
   * thread, and returns once all of them have finished.
   * <p>
   * The tasks are copied into a new list in the order the given collection
   * iterates, so the collection passed in is left as it was and the futures
   * returned line up with it one for one.
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
    List<Callable<T>> wrapped = new ArrayList<>(tasks.size());
    for (Callable<T> task : tasks) {
      wrapped.add(SubjectPreservingTasks.wrap(task));
    }
    return super.invokeAll(wrapped);
  }

  /**
   * Runs every one of the given tasks under the subject current on this
   * thread, and returns once all of them have finished or the wait has run
   * out.
   * <p>
   * The tasks are copied into a new list in the order the given collection
   * iterates, so the collection passed in is left as it was and the futures
   * returned line up with it one for one.
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
    List<Callable<T>> wrapped = new ArrayList<>(tasks.size());
    for (Callable<T> task : tasks) {
      wrapped.add(SubjectPreservingTasks.wrap(task));
    }
    return super.invokeAll(wrapped, timeout, unit);
  }

  /**
   * Runs the given tasks under the subject current on this thread and returns
   * the result of one that finished without failing.
   * <p>
   * The tasks are copied into a new list in the order the given collection
   * iterates, so the collection passed in is left as it was.
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
    List<Callable<T>> wrapped = new ArrayList<>(tasks.size());
    for (Callable<T> task : tasks) {
      wrapped.add(SubjectPreservingTasks.wrap(task));
    }
    return super.invokeAny(wrapped);
  }

  /**
   * Runs the given tasks under the subject current on this thread and returns
   * the result of one that finished without failing before the wait ran out.
   * <p>
   * The tasks are copied into a new list in the order the given collection
   * iterates, so the collection passed in is left as it was.
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
    List<Callable<T>> wrapped = new ArrayList<>(tasks.size());
    for (Callable<T> task : tasks) {
      wrapped.add(SubjectPreservingTasks.wrap(task));
    }
    return super.invokeAny(wrapped, timeout, unit);
  }
}
