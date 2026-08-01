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

import java.util.concurrent.BlockingQueue;
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
   * submitter rather than that of whoever first caused a worker to exist. Two
   * kinds of submission need nothing read for them and are handed straight on: a
   * submission made on a runtime that gives a thread the identity of the thread
   * that starts it, which has already given the worker one; and a submission
   * made by a thread that holds no identity, since binding an absent identity
   * would take the place of whatever the code that goes on to run the task would
   * otherwise have observed, rather than leave it alone.
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
