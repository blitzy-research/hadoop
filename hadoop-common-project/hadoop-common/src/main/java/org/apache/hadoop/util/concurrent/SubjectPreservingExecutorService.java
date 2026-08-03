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
 * An executor service that prepares the tasks it forwards so that they carry the
 * submitting thread's JAAS subject.
 * <p>
 * Every task goes through {@link SubjectPreservingTasks#wrap(Runnable)} or
 * {@link SubjectPreservingTasks#wrap(Callable)} on its way in, always on the
 * submitting thread. Where that utility wraps -- wherever
 * {@code SubjectUtil.THREAD_INHERITS_SUBJECT} is {@code false}, JDK 25 among
 * those runtimes -- a task runs under the identity of its own submitter rather
 * than the identity current when a worker was created, which is what keeps a
 * pool correct when one reused worker serves several submitters. Where the
 * runtime propagates the subject itself, or a submission carries none at all,
 * the task is forwarded as it arrived and observes whatever its worker holds.
 * <p>
 * Nothing else is altered: every other call, the whole of the service's
 * lifecycle included, goes straight to the wrapped service, and the methods that
 * take a collection of tasks forward the prepared tasks in the order the
 * collection yields them, leaving the collection itself as it was.
 * <p>
 * It forwards rather than extends because {@link HadoopThreadPoolExecutor} is
 * final and the single-thread services {@link HadoopExecutors} obtains from
 * {@link java.util.concurrent.Executors} are implementations Hadoop cannot see
 * into, with particular semantics it has deliberately not reproduced. It belongs
 * in front of such a service and nothing else: a pool Hadoop owns already
 * prepares what it is handed, so a task prepared here and then submitted to one
 * with {@link ExecutorService#submit(Runnable)} arrives inside a future that pool
 * prepares in turn, leaving two layers where
 * {@link SubjectPreservingTasks#unwrap(Runnable)} takes one off.
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

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    return super.submit(SubjectPreservingTasks.wrap(task));
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    return super.submit(SubjectPreservingTasks.wrap(task), result);
  }

  @Override
  public Future<?> submit(Runnable task) {
    return super.submit(SubjectPreservingTasks.wrap(task));
  }

  @Override
  public void execute(Runnable command) {
    super.execute(SubjectPreservingTasks.wrap(command));
  }

  /**
   * Prepares every task in {@code tasks} on the terms the class contract states.
   * <p>
   * The preparation happens here, on the thread calling one of the bulk
   * submission methods, which is what makes the subject the submitting thread's
   * own. {@code null} is handed on as it is, so that a wrapped service given
   * {@code null} still refuses it exactly as it would have.
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

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
      throws InterruptedException {
    return super.invokeAll(prepare(tasks));
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks,
      long timeout, TimeUnit unit) throws InterruptedException {
    return super.invokeAll(prepare(tasks), timeout, unit);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
      throws InterruptedException, ExecutionException {
    return super.invokeAny(prepare(tasks));
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout,
      TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return super.invokeAny(prepare(tasks), timeout, unit);
  }
}
