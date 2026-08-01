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

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


/** An extension of ThreadPoolExecutor that provides additional functionality.
 *  */
public final class HadoopThreadPoolExecutor extends ThreadPoolExecutor {

  private static final Logger LOG = LoggerFactory
      .getLogger(HadoopThreadPoolExecutor.class);

  public HadoopThreadPoolExecutor(int corePoolSize,
      int maximumPoolSize,
      long keepAliveTime,
      TimeUnit unit,
      BlockingQueue<Runnable> workQueue) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
  }

  public HadoopThreadPoolExecutor(int corePoolSize,
      int maximumPoolSize,
      long keepAliveTime,
      TimeUnit unit,
      BlockingQueue<Runnable> workQueue,
      ThreadFactory threadFactory) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue,
        threadFactory);
  }

  public HadoopThreadPoolExecutor(int corePoolSize,
      int maximumPoolSize,
      long keepAliveTime,
      TimeUnit unit,
      BlockingQueue<Runnable> workQueue,
      RejectedExecutionHandler handler) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue,
        handler);
  }

  public HadoopThreadPoolExecutor(int corePoolSize,
      int maximumPoolSize,
      long keepAliveTime,
      TimeUnit unit,
      BlockingQueue<Runnable> workQueue,
      ThreadFactory threadFactory,
      RejectedExecutionHandler handler) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue,
        threadFactory, handler);
  }

  /**
   * Hands {@code command} to the pool so that it runs under the JAAS subject of
   * the thread that submitted it.
   * <p>
   * Every way of giving work to this executor arrives here: the JDK routes each
   * {@code submit}, {@code invokeAll} and {@code invokeAny} overload through
   * this method. Preparing the task once, at this single point, therefore
   * reaches every submission and leaves none of them out, and no other method
   * of this class prepares one, so the submitted task stays behind the single
   * layer that {@link SubjectPreservingTasks} takes back off.
   * <p>
   * This method can also be reached a second time with the very same task: a
   * rejection policy that retries a task it was handed, as
   * {@link ThreadPoolExecutor.DiscardOldestPolicy} does, calls back into it once
   * the pool has made room. An already prepared task is handed on as it is, so
   * the retry neither adds a layer nor replaces the identity captured when the
   * task was first submitted.
   * <p>
   * A worker of this pool outlives the task it runs and serves one submitter
   * after another, so the identity to run under has to be read from the
   * submitting thread as the task is handed over, and not once, when the worker
   * is created. Reading it here is what gives every task the identity of its
   * own submitter rather than that of whoever first caused a worker to exist —
   * including a submitter that carries no identity, whose task then runs under
   * none rather than under whatever the worker was left holding.
   * <p>
   * A task this pool rejects and is offered again, as
   * {@link java.util.concurrent.ThreadPoolExecutor.DiscardOldestPolicy} offers
   * it, arrives here a second time already prepared, and is passed on as it is:
   * one layer, one identity, the identity of the submission it belongs to. The
   * task this pool reports on, and the future returned to whoever submitted it,
   * both still describe the task as submitted.
   *
   * @param command the task to run; a {@code null} task is passed on so that
   *                the pool rejects it as it always has
   */
  @Override
  public void execute(Runnable command) {
    super.execute(SubjectPreservingTasks.wrap(command));
  }

  /**
   * Stops the pool at once and returns the tasks that had not started, as they
   * were submitted.
   * <p>
   * A caller of this method is owed the tasks it handed over, so that it can
   * run them elsewhere or report on them, and not the form this pool queued
   * them in.
   *
   * @return the tasks that never started, each as it was submitted
   */
  @Override
  public List<Runnable> shutdownNow() {
    return SubjectPreservingTasks.unwrapAll(super.shutdownNow());
  }

  /**
   * Takes a task off the queue if it is still waiting there.
   * <p>
   * The caller names the task it submitted, which the queue may hold in the
   * form this pool prepared, so a queued task that stands for the one named is
   * removed as well. Nothing else changes: removal still reports whether a task
   * was waiting, and a pool left empty by it still terminates.
   *
   * @param task the task to remove, as it was submitted
   * @return whether the task was waiting on the queue and was removed
   */
  @Override
  public boolean remove(Runnable task) {
    if (super.remove(task)) {
      return true;
    }
    if (task == null) {
      return false;
    }
    for (Runnable queued : getQueue().toArray(new Runnable[0])) {
      if (task.equals(SubjectPreservingTasks.unwrap(queued))) {
        return super.remove(queued);
      }
    }
    return false;
  }

  /**
   * Drops the cancelled tasks that are still occupying the queue.
   * <p>
   * A cancelled task reports itself as such through the future it was submitted
   * as, which the queue may hold in the form this pool prepared, so each queued
   * task is asked about the task it stands for before the pool is asked to
   * reclaim the rest.
   */
  @Override
  public void purge() {
    for (Runnable queued : getQueue().toArray(new Runnable[0])) {
      Runnable task = SubjectPreservingTasks.unwrap(queued);
      if (task != queued && task instanceof Future<?>
          && ((Future<?>) task).isCancelled()) {
        getQueue().remove(queued);
      }
    }
    super.purge();
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
