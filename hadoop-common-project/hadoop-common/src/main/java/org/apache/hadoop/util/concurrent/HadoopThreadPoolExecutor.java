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
 * <p>
 * A task handed to this pool passes through
 * {@link SubjectPreservingTasks#wrap(Runnable)} in {@link #execute(Runnable)},
 * which is the whole of the pool's intake, so it is prepared exactly once
 * however it was submitted. Where that utility wraps -- wherever
 * {@code SubjectUtil.THREAD_INHERITS_SUBJECT} is {@code false}, JDK 25 among
 * those runtimes -- the task runs under the subject current on the thread that
 * handed it over, even on a worker another submitter brought into existence.
 * Where the runtime propagates the subject itself, or a submission carries none
 * at all, the task is passed on unchanged and observes whatever its worker
 * holds: the identity in force when that worker was created, or none where the
 * worker was given none.
 * Either way the code that describes a queued task rather than running it --
 * {@link #beforeExecute(Thread, Runnable)} here and {@code ExecutorHelper} from
 * {@link #afterExecute(Runnable, Throwable)} -- takes that single layer back off
 * first.
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
   * Hands a task to the pool, prepared on the terms the class contract states.
   * <p>
   * This is the only place a task entering this pool is prepared, and it covers
   * every way of entering it: {@code AbstractExecutorService} routes all three
   * {@code submit} overloads, both {@code invokeAll} overloads and both
   * {@code invokeAny} overloads through this method, and this class overrides
   * neither those nor {@code newTaskFor}, so a submission passes exactly one
   * preparing point and a prepared task has exactly one layer for
   * {@link SubjectPreservingTasks#unwrap(Runnable)} to take back off.
   *
   * @param command the task to run
   */
  @Override
  public void execute(Runnable command) {
    super.execute(SubjectPreservingTasks.wrap(command));
  }

  @Override
  protected void beforeExecute(Thread t, Runnable r) {
    if (LOG.isDebugEnabled()) {
      // Report the task as it was submitted: a prepared task is not of the class
      // the submitter passed in, and this line is read by operators.
      LOG.debug("beforeExecute in thread: " + Thread.currentThread()
          .getName() + ", runnable type: "
          + SubjectPreservingTasks.unwrap(r).getClass().getName());
    }
  }

  @Override
  protected void afterExecute(Runnable r, Throwable t) {
    super.afterExecute(r, t);
    ExecutorHelper.logThrowableFromAfterExecute(r, t);
  }
}
