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
   * A worker of this pool outlives the task it runs and serves one submitter
   * after another, so an identity to run under has to be read from the
   * submitting thread as the task is handed over, and not once, when the worker
   * is created. Reading it here is what gives a task the identity of its own
   * submitter rather than that of whoever first caused a worker to exist, and
   * establishing it for the task's own extent is what keeps the worker from
   * carrying it into the task after it. Every submission is prepared this way,
   * including one made by a thread that holds no identity: such a task runs
   * with none, which is what its submitter would have observed, rather than
   * under whichever user its worker was left holding.
   * <p>
   * This method can also be reached a second time with the very same task: a
   * rejection policy that offers a task back, as
   * {@link ThreadPoolExecutor.DiscardOldestPolicy} does, calls into it again
   * once the pool has made room. An already prepared task is handed on as it is,
   * so a second offer neither adds a layer nor replaces the identity read when
   * the task was first submitted. The task this pool reports on, and the future
   * returned to whoever submitted it, both still describe the task as
   * submitted.
   *
   * @param command the task to run
   */
  @Override
  public void execute(Runnable command) {
    super.execute(SubjectPreservingTasks.wrap(command));
  }

  /**
   * Stops this pool at once and returns the tasks that never started, as they
   * were submitted.
   * <p>
   * A caller shutting a pool down is owed the tasks it handed over, so that it
   * can run them somewhere else or report on them, and not the form this pool
   * queued them in. Handing back the prepared form would make every caller
   * responsible for recognising and undoing a detail of this class, and a caller
   * that did not know to would be left holding tasks it could not identify.
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
   * The caller names the task it submitted, while the queue may be holding the
   * prepared form of it, so the task standing for the one named is removed as
   * well. The queue is only searched when the task is not found as it is, which
   * is what happens for a task that was never prepared, so the common case still
   * costs one queue removal and nothing more.
   * <p>
   * Nothing else about removal changes: it still reports whether the task was
   * waiting, and a pool left empty by it still terminates, because the removal
   * itself is still the pool's own.
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
   * Drops the cancelled tasks that are still taking up room on the queue.
   * <p>
   * A cancelled task says so through the future it was submitted as, and the
   * queue may be holding the prepared form of that future rather than the future
   * itself, which the pool's own sweep does not recognise. Each queued task is
   * therefore asked about the task it stands for first, and only entries that
   * are prepared are examined here, because the pool's own sweep afterwards
   * already handles the rest.
   * <p>
   * Reclaiming these entries is what releases the identity each one captured.
   * A cancelled task left on the queue keeps its submitter's subject, and the
   * credentials in it, reachable until something else displaces it -- which for
   * a queue that has stopped draining may be never.
   */
  @Override
  public void purge() {
    for (Runnable queued : getQueue().toArray(new Runnable[0])) {
      Runnable task = SubjectPreservingTasks.unwrap(queued);
      if (task != queued && task instanceof Future<?>
          && ((Future<?>) task).isCancelled()) {
        super.remove(queued);
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
