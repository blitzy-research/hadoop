/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.util.concurrent;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;

import org.apache.hadoop.security.authentication.util.SubjectUtil;
import org.apache.hadoop.util.BlockingThreadPoolExecutorService;
import org.apache.hadoop.util.Daemon;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Asserts that a thread created and started through Hadoop's own thread utilities
 * observes the Subject of the thread that created and started it.
 * <p>
 * The utilities exercised here are {@link SubjectInheritingThread}, {@link Daemon},
 * and the two thread factories built on them, {@link Daemon.DaemonFactory} and
 * {@link BlockingThreadPoolExecutorService#newDaemonThreadFactory(String)}. Each of
 * them owes that guarantee to its callers, and losing it is silent: the code still
 * compiles, throws nothing and logs nothing, while the work runs under no identity at
 * all. An identity is what decides an authorization and what an audit record names,
 * so work that runs without one is denied where it should be allowed, or recorded
 * against nobody. That is why the guarantee is asserted here directly, rather than
 * inferred from the behaviour of anything built on top of it.
 * <p>
 * Every assertion below is an outcome, namely the Subject that the new thread
 * observes, and never the mechanism that delivers it. That mechanism is not the same
 * on every runtime: one hands the current Subject to a thread it is asked to create,
 * and the utilities then simply let it through, while another does not, and the
 * utilities capture the Subject themselves and re-establish it in the new thread.
 * The outcome is identical either way, so the same assertions hold on every
 * supported runtime, with no branch on the runtime and no test skipped on any of
 * them.
 * <p>
 * Each test therefore both creates and starts its thread inside the scope that
 * establishes the Subject, because that is what the two mechanisms between them
 * require: one takes its copy when the thread is constructed, the other when it is
 * started. Creating a thread outside that scope and starting it inside, or the
 * reverse, would satisfy only one of them, and the resulting test would pass on one
 * runtime and fail on the other.
 * <p>
 * Every constructor of both utilities is covered, since every one of them is a way
 * for production code to create a thread, and the propagation guarantee has to hold
 * for all of them. The distinction that makes complete coverage worthwhile is that
 * {@link SubjectInheritingThread} runs a Runnable only when the Runnable reached it
 * through a constructor that accepts one; the constructors that take no Runnable
 * need {@link SubjectInheritingThread#work()} overridden instead, and are covered in
 * that form.
 */
public class TestThreadFactorySubjectPropagation {

  /**
   * How long to wait for a thread under test to finish.
   * <p>
   * Every wait in this class is bounded by this value, and every test then asserts
   * that the thread it waited for really did terminate, so a wait that expires is
   * reported as such rather than being allowed to surface as a missing observation.
   */
  private static final long JOIN_TIMEOUT_MILLIS = 1000;

  /**
   * The realm every principal created here belongs to.
   * <p>
   * Principal names are qualified with it explicitly so that building one resolves
   * no default realm and therefore needs no Kerberos configuration of any kind.
   */
  private static final String REALM = "@EXAMPLE.COM";

  /**
   * The name, or name prefix, given to a thread under test whose constructor or
   * factory requires one.
   * <p>
   * One value serves all of them because the name is only an argument that has to be
   * supplied, and never anything this class examines: thread names are output that
   * operators read, so no assertion here looks at one.
   */
  private static final String THREAD_NAME = "test-subject-propagation";

  /**
   * Builds a Subject carrying a single, distinctly named principal.
   * <p>
   * The principal is what keeps each Subject in this class distinguishable from
   * every other one: a Subject with no principals and no credentials compares equal
   * to any other such Subject, so asserting against a bare Subject could pass
   * against the wrong instance entirely.
   *
   * @param name the principal name, given without a realm
   * @return a Subject holding exactly that one principal
   */
  private static Subject creatorSubject(String name) {
    Subject subject = new Subject();
    subject.getPrincipals().add(new KerberosPrincipal(name + REALM));
    return subject;
  }

  /**
   * Asserts that the thread under test finished, and that it observed the Subject
   * established on the thread which created it.
   * <p>
   * Two separate things are needed here, and they are checked in that order. What
   * the thread under test observed reaches this thread through an
   * {@link AtomicReference}, which is what publishes the value from the one thread to
   * the other. Whether the thread got as far as writing it is a different question,
   * and {@link Thread#join(long)} does not answer it: join returns as readily because
   * its timeout expired as because the thread ended. Asserting that the thread is no
   * longer alive is what answers it, and asserting that first reports a thread that
   * overran as exactly that, instead of leaving it to be diagnosed from the empty
   * observation it would otherwise leave behind.
   * <p>
   * The Subject is then compared by identity rather than by equality, which is both
   * available and exact here: the utilities re-establish the very instance they were
   * given, on every runtime.
   *
   * @param creator  the Subject established on the creating thread
   * @param observed what the thread under test read back while it was running
   * @param worker   the thread under test
   */
  private static void assertObservedCreatorSubject(Subject creator,
      AtomicReference<Subject> observed, AtomicReference<Thread> worker) {
    assertFalse(worker.get().isAlive(),
        "thread under test did not finish within " + JOIN_TIMEOUT_MILLIS + " ms");
    assertSame(creator, observed.get(),
        "thread under test did not observe the creating thread's Subject");
  }

  @Test
  @Timeout(value = 30)
  public void testSubjectInheritingThreadWorkOverride() {
    final Subject creator = creatorSubject("sit-work-override");
    final AtomicReference<Subject> observed = new AtomicReference<>();
    final AtomicReference<Thread> worker = new AtomicReference<>();

    SubjectUtil.callAs(creator, new Callable<Void>() {
      public Void call() throws InterruptedException {
        SubjectInheritingThread t = new SubjectInheritingThread() {
          @Override
          public void work() {
            observed.set(SubjectUtil.current());
          }
        };
        worker.set(t);
        t.start();
        t.join(JOIN_TIMEOUT_MILLIS);
        return (Void) null;
      }
    });

    assertObservedCreatorSubject(creator, observed, worker);
  }

  @Test
  @Timeout(value = 30)
  public void testSubjectInheritingThreadRunnableTarget() {
    final Subject creator = creatorSubject("sit-runnable-target");
    final AtomicReference<Subject> observed = new AtomicReference<>();
    final AtomicReference<Thread> worker = new AtomicReference<>();

    SubjectUtil.callAs(creator, new Callable<Void>() {
      public Void call() throws InterruptedException {
        Runnable r = new Runnable() {
          @Override
          public void run() {
            observed.set(SubjectUtil.current());
          }
        };

        SubjectInheritingThread t = new SubjectInheritingThread(r);
        worker.set(t);
        t.start();
        t.join(JOIN_TIMEOUT_MILLIS);
        return (Void) null;
      }
    });

    assertObservedCreatorSubject(creator, observed, worker);
  }

  @Test
  @Timeout(value = 30)
  public void testSubjectInheritingThreadGroupedRunnableTarget() {
    final Subject creator = creatorSubject("sit-grouped-runnable-target");
    final AtomicReference<Subject> observed = new AtomicReference<>();
    final AtomicReference<Thread> worker = new AtomicReference<>();

    SubjectUtil.callAs(creator, new Callable<Void>() {
      public Void call() throws InterruptedException {
        Runnable r = new Runnable() {
          @Override
          public void run() {
            observed.set(SubjectUtil.current());
          }
        };

        ThreadGroup group = Thread.currentThread().getThreadGroup();
        SubjectInheritingThread t = new SubjectInheritingThread(group, r);
        worker.set(t);
        t.start();
        t.join(JOIN_TIMEOUT_MILLIS);
        return (Void) null;
      }
    });

    assertObservedCreatorSubject(creator, observed, worker);
  }

  @Test
  @Timeout(value = 30)
  public void testSubjectInheritingThreadNamedRunnableTarget() {
    final Subject creator = creatorSubject("sit-named-runnable-target");
    final AtomicReference<Subject> observed = new AtomicReference<>();
    final AtomicReference<Thread> worker = new AtomicReference<>();

    SubjectUtil.callAs(creator, new Callable<Void>() {
      public Void call() throws InterruptedException {
        Runnable r = new Runnable() {
          @Override
          public void run() {
            observed.set(SubjectUtil.current());
          }
        };

        SubjectInheritingThread t = new SubjectInheritingThread(r, THREAD_NAME);
        worker.set(t);
        t.start();
        t.join(JOIN_TIMEOUT_MILLIS);
        return (Void) null;
      }
    });

    assertObservedCreatorSubject(creator, observed, worker);
  }

  @Test
  @Timeout(value = 30)
  public void testSubjectInheritingThreadGroupedNamedRunnableTarget() {
    final Subject creator = creatorSubject("sit-grouped-named-runnable-target");
    final AtomicReference<Subject> observed = new AtomicReference<>();
    final AtomicReference<Thread> worker = new AtomicReference<>();

    SubjectUtil.callAs(creator, new Callable<Void>() {
      public Void call() throws InterruptedException {
        Runnable r = new Runnable() {
          @Override
          public void run() {
            observed.set(SubjectUtil.current());
          }
        };

        ThreadGroup group = Thread.currentThread().getThreadGroup();
        SubjectInheritingThread t = new SubjectInheritingThread(group, r, THREAD_NAME);
        worker.set(t);
        t.start();
        t.join(JOIN_TIMEOUT_MILLIS);
        return (Void) null;
      }
    });

    assertObservedCreatorSubject(creator, observed, worker);
  }

  @Test
  @Timeout(value = 30)
  public void testSubjectInheritingThreadNamedWorkOverride() {
    final Subject creator = creatorSubject("sit-named-work-override");
    final AtomicReference<Subject> observed = new AtomicReference<>();
    final AtomicReference<Thread> worker = new AtomicReference<>();

    SubjectUtil.callAs(creator, new Callable<Void>() {
      public Void call() throws InterruptedException {
        SubjectInheritingThread t = new SubjectInheritingThread(THREAD_NAME) {
          @Override
          public void work() {
            observed.set(SubjectUtil.current());
          }
        };
        worker.set(t);
        t.start();
        t.join(JOIN_TIMEOUT_MILLIS);
        return (Void) null;
      }
    });

    assertObservedCreatorSubject(creator, observed, worker);
  }

  @Test
  @Timeout(value = 30)
  public void testSubjectInheritingThreadGroupedNamedWorkOverride() {
    final Subject creator = creatorSubject("sit-grouped-named-work-override");
    final AtomicReference<Subject> observed = new AtomicReference<>();
    final AtomicReference<Thread> worker = new AtomicReference<>();

    SubjectUtil.callAs(creator, new Callable<Void>() {
      public Void call() throws InterruptedException {
        ThreadGroup group = Thread.currentThread().getThreadGroup();
        SubjectInheritingThread t = new SubjectInheritingThread(group, THREAD_NAME) {
          @Override
          public void work() {
            observed.set(SubjectUtil.current());
          }
        };
        worker.set(t);
        t.start();
        t.join(JOIN_TIMEOUT_MILLIS);
        return (Void) null;
      }
    });

    assertObservedCreatorSubject(creator, observed, worker);
  }

  @Test
  @Timeout(value = 30)
  public void testDaemonWorkOverride() {
    final Subject creator = creatorSubject("daemon-work-override");
    final AtomicReference<Subject> observed = new AtomicReference<>();
    final AtomicReference<Thread> worker = new AtomicReference<>();

    SubjectUtil.callAs(creator, new Callable<Void>() {
      public Void call() throws InterruptedException {
        Daemon t = new Daemon() {
          @Override
          public void work() {
            observed.set(SubjectUtil.current());
          }
        };
        worker.set(t);
        t.start();
        t.join(JOIN_TIMEOUT_MILLIS);
        return (Void) null;
      }
    });

    assertObservedCreatorSubject(creator, observed, worker);
    assertTrue(worker.get().isDaemon(), "Daemon was not created as a daemon thread");
  }

  @Test
  @Timeout(value = 30)
  public void testDaemonRunnableTarget() {
    final Subject creator = creatorSubject("daemon-runnable-target");
    final AtomicReference<Subject> observed = new AtomicReference<>();
    final AtomicReference<Thread> worker = new AtomicReference<>();

    SubjectUtil.callAs(creator, new Callable<Void>() {
      public Void call() throws InterruptedException {
        Runnable r = new Runnable() {
          @Override
          public void run() {
            observed.set(SubjectUtil.current());
          }
        };

        Daemon t = new Daemon(r);
        worker.set(t);
        t.start();
        t.join(JOIN_TIMEOUT_MILLIS);
        return (Void) null;
      }
    });

    assertObservedCreatorSubject(creator, observed, worker);
    assertTrue(worker.get().isDaemon(), "Daemon was not created as a daemon thread");
  }

  @Test
  @Timeout(value = 30)
  public void testDaemonGroupedRunnableTarget() {
    final Subject creator = creatorSubject("daemon-grouped-runnable-target");
    final AtomicReference<Subject> observed = new AtomicReference<>();
    final AtomicReference<Thread> worker = new AtomicReference<>();

    SubjectUtil.callAs(creator, new Callable<Void>() {
      public Void call() throws InterruptedException {
        Runnable r = new Runnable() {
          @Override
          public void run() {
            observed.set(SubjectUtil.current());
          }
        };

        ThreadGroup group = Thread.currentThread().getThreadGroup();
        Daemon t = new Daemon(group, r);
        worker.set(t);
        t.start();
        t.join(JOIN_TIMEOUT_MILLIS);
        return (Void) null;
      }
    });

    assertObservedCreatorSubject(creator, observed, worker);
    assertTrue(worker.get().isDaemon(), "Daemon was not created as a daemon thread");
  }

  /**
   * The factory is asked for the thread inside the establishing scope, which is where
   * production code asks for one: the factory call is the moment the thread comes
   * into existence, so it is the moment the identity has to be available.
   */
  @Test
  @Timeout(value = 30)
  public void testDaemonFactoryNewThread() {
    final Subject creator = creatorSubject("daemon-factory");
    final AtomicReference<Subject> observed = new AtomicReference<>();
    final AtomicReference<Thread> worker = new AtomicReference<>();

    SubjectUtil.callAs(creator, new Callable<Void>() {
      public Void call() throws InterruptedException {
        Runnable r = new Runnable() {
          @Override
          public void run() {
            observed.set(SubjectUtil.current());
          }
        };

        ThreadFactory factory = new Daemon.DaemonFactory();
        Thread t = factory.newThread(r);
        worker.set(t);
        t.start();
        t.join(JOIN_TIMEOUT_MILLIS);
        return (Void) null;
      }
    });

    assertObservedCreatorSubject(creator, observed, worker);
    assertTrue(worker.get().isDaemon(),
        "DaemonFactory did not produce a daemon thread");
  }

  /**
   * As with the other factory, the thread is requested inside the establishing scope.
   * That this factory also guarantees a daemon thread is asserted as well, since that
   * is part of what it promises its callers.
   */
  @Test
  @Timeout(value = 30)
  public void testDaemonThreadFactoryNewThread() {
    final Subject creator = creatorSubject("daemon-thread-factory");
    final AtomicReference<Subject> observed = new AtomicReference<>();
    final AtomicReference<Thread> worker = new AtomicReference<>();

    SubjectUtil.callAs(creator, new Callable<Void>() {
      public Void call() throws InterruptedException {
        Runnable r = new Runnable() {
          @Override
          public void run() {
            observed.set(SubjectUtil.current());
          }
        };

        ThreadFactory factory =
            BlockingThreadPoolExecutorService.newDaemonThreadFactory(THREAD_NAME);
        Thread t = factory.newThread(r);
        worker.set(t);
        t.start();
        t.join(JOIN_TIMEOUT_MILLIS);
        return (Void) null;
      }
    });

    assertObservedCreatorSubject(creator, observed, worker);
    assertTrue(worker.get().isDaemon(),
        "newDaemonThreadFactory did not produce a daemon thread");
  }

}
