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
 * what keeps a pool correct when one reused worker serves several submitters,
 * and a submission made with no identity at all runs under none rather than
 * under whatever identity its worker was left holding.
 * <p>
 * Nothing else is altered. Every other call goes straight to the wrapped
 * service, which keeps its own behaviour in full. The one place where forwarding
 * alone would not be enough is
 * {@link java.util.concurrent.ExecutorService#shutdownNow()}: the wrapped
 * service gives back the tasks it never started in the form they were queued in,
 * which is the prepared form, and a caller shutting a pool down is owed the
 * tasks it submitted. They are therefore returned as they were handed over, so
 * that no caller has to know that anything was prepared at all.
 * <p>
 * The methods that take a whole collection of tasks hand the wrapped service a
 * view of it, through {@link SubjectPreservingTasks#wrapEach(Collection)},
 * rather than a list prepared in advance. The wrapped service therefore reads
 * the collection itself, and keeps sole charge of what that means: a wait it was
 * given still covers the reading, it still hands over only as many tasks as it
 * needs, and it still refuses a collection it would have refused before. The
 * collection passed in is left as it was, and the futures returned line up with
 * it one for one, in its order.
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
   * Stops the wrapped service at once and returns the tasks that never started,
   * as they were submitted.
   * <p>
   * The wrapped service hands them back in the form it queued them in, which is
   * the form this class prepared, so each is returned to the form it arrived in.
   * A caller shutting a pool down is owed the tasks it handed over, so that it
   * can run them elsewhere or report on them; leaving them prepared would make
   * every such caller responsible for undoing a detail of this class, and one
   * that did not know to would be holding tasks it could not identify.
   *
   * @return the tasks that never started, each as it was submitted
   */
  @Override
  public List<Runnable> shutdownNow() {
    return SubjectPreservingTasks.unwrapAll(super.shutdownNow());
  }

  /**
   * Runs every one of the given tasks under the subject current on this
   * thread, and returns once all of them have finished.
   * <p>
   * The tasks are prepared as the wrapped service reaches them, so it reads the
   * collection given here itself and keeps sole charge of the order in which it
   * hands the tasks over.
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
    return super.invokeAll(SubjectPreservingTasks.wrapEach(tasks));
  }

  /**
   * Runs every one of the given tasks under the subject current on this
   * thread, and returns once all of them have finished or the wait has run
   * out.
   * <p>
   * The wait is the wrapped service's own, measured from the moment it is
   * called: the tasks are prepared as it reaches them, inside the wait, rather
   * than beforehand, so the time spent reading the collection counts against
   * the wait exactly as it does without this class in the way.
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
    return super.invokeAll(SubjectPreservingTasks.wrapEach(tasks), timeout,
        unit);
  }

  /**
   * Runs the given tasks under the subject current on this thread and returns
   * the result of one that finished without failing.
   * <p>
   * Only as many tasks are handed over as are needed, because they are prepared
   * as the wrapped service reaches them: it starts with one and takes another
   * only while none has finished, exactly as it does without this class in the
   * way.
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
    return super.invokeAny(SubjectPreservingTasks.wrapEach(tasks));
  }

  /**
   * Runs the given tasks under the subject current on this thread and returns
   * the result of one that finished without failing before the wait ran out.
   * <p>
   * As with the untimed form, only as many tasks are handed over as are needed;
   * and as with the other timed form, the wait is the wrapped service's own and
   * covers the reading of the collection.
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
    return super.invokeAny(SubjectPreservingTasks.wrapEach(tasks), timeout,
        unit);
  }
}
