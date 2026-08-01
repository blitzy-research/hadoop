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
 * Asserts that a thread created through Hadoop's own thread utilities observes the
 * Subject of the thread that created it.
 * <p>
 * The utilities exercised here are {@link SubjectInheritingThread}, {@link Daemon},
 * and the two thread factories built on them, {@link Daemon.DaemonFactory} and
 * {@link BlockingThreadPoolExecutorService#newDaemonThreadFactory(String)}. All of
 * them already carry the Subject across correctly, so this class is not uncovering a
 * defect; it pins the guarantee down, so that a later change to any of them cannot
 * drop the identity quietly. Dropping it still compiles, throws nothing and logs
 * nothing, and shows up only much later as a denied authorization or an anonymous
 * audit record, which is precisely why the guarantee is worth asserting directly.
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
   * Termination is checked first, and deliberately so. {@link Thread#join(long)} can
   * return because its timeout expired rather than because the thread ended, and it
   * is the thread ending that makes what the thread wrote visible to this one.
   * Checking it first reports a thread that overran directly, instead of leaving it
   * to be diagnosed from the empty observation it would otherwise leave behind.
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

  /**
   * A no-argument {@link SubjectInheritingThread} whose payload is supplied by
   * overriding {@link SubjectInheritingThread#work()}.
   */
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

  /**
   * A {@link SubjectInheritingThread} whose payload is a Runnable passed to the
   * constructor.
   */
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

  /**
   * A {@link SubjectInheritingThread} created in an explicit thread group, with its
   * payload passed as a Runnable.
   */
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

  /**
   * A named {@link SubjectInheritingThread} with its payload passed as a Runnable.
   */
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

  /**
   * A named {@link SubjectInheritingThread} created in an explicit thread group, with
   * its payload passed as a Runnable.
   */
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

  /**
   * A named {@link SubjectInheritingThread} that takes no Runnable, so its payload
   * has to be supplied by overriding {@link SubjectInheritingThread#work()}.
   */
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

  /**
   * A named {@link SubjectInheritingThread} in an explicit thread group that takes no
   * Runnable, so its payload has to be supplied by overriding
   * {@link SubjectInheritingThread#work()}.
   */
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

  /**
   * A no-argument {@link Daemon} whose payload is supplied by overriding
   * {@link Daemon#work()}.
   */
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

  /**
   * A {@link Daemon} whose payload is a Runnable passed to the constructor.
   */
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

  /**
   * A {@link Daemon} created in an explicit thread group, with its payload passed as
   * a Runnable.
   */
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
   * A thread obtained from {@link Daemon.DaemonFactory}, the thread factory Hadoop
   * hands to executor constructors.
   * <p>
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
   * A thread obtained from
   * {@link BlockingThreadPoolExecutorService#newDaemonThreadFactory(String)}, the
   * named daemon thread factory that Hadoop's blocking thread pool is built on.
   * <p>
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
