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
 * {@link SubjectPreservingTasks#wrap(Callable)} on its way in, and always on
 * the submitting thread, so a task runs under the identity of its own submitter
 * rather than the identity current when a worker was created. That is what
 * keeps a pool correct when one reused worker serves several submitters.
 * <p>
 * Nothing else is altered. Every other call goes straight to the wrapped
 * service, which keeps its own behaviour in full. The one place where
 * forwarding alone would not be faithful is
 * {@link #shutdownNow()}: the wrapped service holds the tasks this class
 * prepared for it and hands those back, so they are returned in the form they
 * were submitted in, which is what a caller asking for its unstarted work is
 * owed. This class adds the propagation of an identity and changes nothing
 * besides.
 * The bulk methods copy the tasks into a new list in iteration order, so the
 * collection passed in is left as it was and the futures returned line up with
 * it one for one.
 * <p>
 * It forwards rather than extends because {@link HadoopThreadPoolExecutor} is
 * final, and because the single-thread services {@link HadoopExecutors} obtains
 * from {@link java.util.concurrent.Executors} are implementations Hadoop cannot
 * see into, with particular semantics it has deliberately not reproduced;
 * forwarding adds the propagation and leaves those semantics untouched.
 * <p>
 * Such a service is what this is for. A pool Hadoop owns already prepares the
 * tasks handed to it, so placing this in front of one adds a step that pool
 * does not need; it is harmless, because
 * {@link SubjectPreservingTasks#wrap(Runnable)} hands back a task that is
 * already prepared, but there is nothing to gain from it.
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
   * Stops the wrapped service at once and returns the tasks that had not
   * started, as they were submitted.
   * <p>
   * The wrapped service queued the tasks this class prepared for it, and hands
   * those back. A caller is owed what it submitted, so each one is returned in
   * that form; the wrapped service's own shutdown behaviour is untouched.
   *
   * @return the tasks that never started, each as it was submitted
   */
  @Override
  public List<Runnable> shutdownNow() {
    return SubjectPreservingTasks.unwrapAll(super.shutdownNow());
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
