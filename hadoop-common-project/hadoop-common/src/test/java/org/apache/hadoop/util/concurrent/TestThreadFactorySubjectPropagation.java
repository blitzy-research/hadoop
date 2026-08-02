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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.SubjectUtil;
import org.apache.hadoop.util.BlockingThreadPoolExecutorService;
import org.apache.hadoop.util.Daemon;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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
 * <p>
 * A Subject observed is necessary but not sufficient, so each utility is covered a
 * second time through {@link UserGroupInformation}, which is the interface production
 * code actually authorizes and audits against. That second reading matters because
 * {@link UserGroupInformation#getCurrentUser()} falls back to whoever the process
 * logged in as whenever it finds no Subject: a thread that lost its creator's
 * identity entirely would still report *a* user, and work would run, be authorized
 * and be recorded against the wrong one. These tests therefore install a login user
 * of their own whose name no test ever creates a thread under, and assert both that
 * the observed user is exactly the creating one and that it is not that login user,
 * so the fall back is a failure rather than a silent pass.
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
   * The name of the user the process is taken to have logged in as.
   * <p>
   * No thread in this class is ever created under this user, so observing it inside a
   * thread under test can only mean that the creating user's identity failed to reach
   * that thread and
   * {@link UserGroupInformation#getCurrentUser()} fell back to the login. That is
   * exactly the outcome a test asserting only that *some* user was observed would let
   * through, which is why the login user is named distinctly and asserted against.
   */
  private static final String LOGIN_USER = "sentinel-login-user";

  /**
   * Installs a known login user before each test, so that a fall back to it is
   * recognisable.
   * <p>
   * The identity machinery keeps process-wide state, so it is put into a known state
   * here rather than being taken as found, and the login user is set explicitly
   * instead of being left to whichever operating-system account happens to be running
   * the build.
   */
  @BeforeEach
  public void setUpTheLoginUser() {
    UserGroupInformation.reset();
    UserGroupInformation.setConfiguration(new Configuration());
    UserGroupInformation.setLoginUser(
        UserGroupInformation.createRemoteUser(LOGIN_USER));
  }

  /** Leaves no logged in user behind for the next test to find. */
  @AfterEach
  public void forgetTheLoginUser() {
    UserGroupInformation.setLoginUser(null);
    UserGroupInformation.reset();
  }

  /**
   * Reads the user in force on the calling thread.
   * <p>
   * A thread under test cannot declare a checked exception, and a failure to read the
   * identity at all is a failure of the test rather than an observation, so it is
   * reported as an unchecked exception and leaves the observation unwritten.
   *
   * @return the user in force, which is the login user when no Subject is in force
   */
  private static UserGroupInformation currentUser() {
    try {
      return UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      throw new IllegalStateException("the user in force could not be read", e);
    }
  }

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

  /**
   * Asserts that the thread under test finished, and that it ran as exactly the user
   * that created it rather than as the process login.
   * <p>
   * Three readings are needed, and each rules out a different way of appearing to
   * work. The Subject is compared by identity against the one the creating thread held
   * inside its own scope, which is the strongest available form and the one the
   * utilities can satisfy, since they re-establish the very instance. The user is then
   * compared as an identity, which {@link UserGroupInformation#equals(Object)} decides
   * by comparing Subjects by reference, so a second user carrying the same name cannot
   * satisfy it. Finally the observed user's name is required to differ from the login
   * user's, which is what catches an identity that never arrived: without that
   * reading, a thread running as the process login would report a perfectly valid user
   * and the assertion would pass.
   *
   * @param creator        the user whose scope the thread was created and started in
   * @param creatorSubject the Subject in force on the creating thread, read there
   * @param observedSubject the Subject the thread under test read back
   * @param observedUser   the user the thread under test read back
   * @param worker         the thread under test
   */
  private static void assertObservedCreatorUser(UserGroupInformation creator,
      AtomicReference<Subject> creatorSubject,
      AtomicReference<Subject> observedSubject,
      AtomicReference<UserGroupInformation> observedUser,
      AtomicReference<Thread> worker) {
    assertFalse(worker.get().isAlive(),
        "thread under test did not finish within " + JOIN_TIMEOUT_MILLIS + " ms");
    assertNotNull(creatorSubject.get(),
        "the creating thread held no Subject inside the scope of the user that "
            + "established it, so this assertion would prove nothing");
    assertSame(creatorSubject.get(), observedSubject.get(),
        "thread under test did not observe the Subject in force on the thread that "
            + "created it");
    assertNotNull(observedUser.get(),
        "thread under test never read back the user it was running as");
    assertEquals(creator, observedUser.get(),
        "thread under test did not run as the user that created it");
    assertEquals(creator.getUserName(), observedUser.get().getUserName(),
        "thread under test ran as a user of another name");
    assertNotEquals(LOGIN_USER, observedUser.get().getUserName(),
        "thread under test ran as the process login user, so the creating user's "
            + "identity never reached it");
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

  /**
   * A thread created and started inside a user's scope runs as that user.
   * <p>
   * This is the same guarantee as the Subject tests above, read through the interface
   * production code authorizes and audits against, and it is the reading that would
   * catch an identity replaced by the process login rather than merely absent.
   *
   * @throws Exception if the identity cannot be established or a wait is interrupted
   */
  @Test
  @Timeout(value = 30)
  public void testSubjectInheritingThreadServesTheCreatingUser() throws Exception {
    final UserGroupInformation creator =
        UserGroupInformation.createRemoteUser("sit-creating-user");
    final AtomicReference<Subject> creatorSubject = new AtomicReference<>();
    final AtomicReference<Subject> observedSubject = new AtomicReference<>();
    final AtomicReference<UserGroupInformation> observedUser =
        new AtomicReference<>();
    final AtomicReference<Thread> worker = new AtomicReference<>();

    creator.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws InterruptedException {
        creatorSubject.set(SubjectUtil.current());
        SubjectInheritingThread t = new SubjectInheritingThread() {
          @Override
          public void work() {
            observedSubject.set(SubjectUtil.current());
            observedUser.set(currentUser());
          }
        };
        worker.set(t);
        t.start();
        t.join(JOIN_TIMEOUT_MILLIS);
        return null;
      }
    });

    assertObservedCreatorUser(creator, creatorSubject, observedSubject, observedUser,
        worker);
  }

  /**
   * A daemon created and started inside a user's scope runs as that user.
   *
   * @throws Exception if the identity cannot be established or a wait is interrupted
   */
  @Test
  @Timeout(value = 30)
  public void testDaemonServesTheCreatingUser() throws Exception {
    final UserGroupInformation creator =
        UserGroupInformation.createRemoteUser("daemon-creating-user");
    final AtomicReference<Subject> creatorSubject = new AtomicReference<>();
    final AtomicReference<Subject> observedSubject = new AtomicReference<>();
    final AtomicReference<UserGroupInformation> observedUser =
        new AtomicReference<>();
    final AtomicReference<Thread> worker = new AtomicReference<>();

    creator.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws InterruptedException {
        creatorSubject.set(SubjectUtil.current());
        Runnable r = new Runnable() {
          @Override
          public void run() {
            observedSubject.set(SubjectUtil.current());
            observedUser.set(currentUser());
          }
        };

        Daemon t = new Daemon(r);
        worker.set(t);
        t.start();
        t.join(JOIN_TIMEOUT_MILLIS);
        return null;
      }
    });

    assertObservedCreatorUser(creator, creatorSubject, observedSubject, observedUser,
        worker);
    assertTrue(worker.get().isDaemon(), "Daemon was not created as a daemon thread");
  }

  /**
   * A thread obtained from the daemon factory inside a user's scope runs as that user.
   *
   * @throws Exception if the identity cannot be established or a wait is interrupted
   */
  @Test
  @Timeout(value = 30)
  public void testDaemonFactoryServesTheCreatingUser() throws Exception {
    final UserGroupInformation creator =
        UserGroupInformation.createRemoteUser("daemon-factory-creating-user");
    final AtomicReference<Subject> creatorSubject = new AtomicReference<>();
    final AtomicReference<Subject> observedSubject = new AtomicReference<>();
    final AtomicReference<UserGroupInformation> observedUser =
        new AtomicReference<>();
    final AtomicReference<Thread> worker = new AtomicReference<>();

    creator.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws InterruptedException {
        creatorSubject.set(SubjectUtil.current());
        Runnable r = new Runnable() {
          @Override
          public void run() {
            observedSubject.set(SubjectUtil.current());
            observedUser.set(currentUser());
          }
        };

        ThreadFactory factory = new Daemon.DaemonFactory();
        Thread t = factory.newThread(r);
        worker.set(t);
        t.start();
        t.join(JOIN_TIMEOUT_MILLIS);
        return null;
      }
    });

    assertObservedCreatorUser(creator, creatorSubject, observedSubject, observedUser,
        worker);
    assertTrue(worker.get().isDaemon(),
        "DaemonFactory did not produce a daemon thread");
  }

  /**
   * A thread obtained from the blocking pool's factory inside a user's scope runs as
   * that user.
   *
   * @throws Exception if the identity cannot be established or a wait is interrupted
   */
  @Test
  @Timeout(value = 30)
  public void testDaemonThreadFactoryServesTheCreatingUser() throws Exception {
    final UserGroupInformation creator =
        UserGroupInformation.createRemoteUser("daemon-thread-factory-creating-user");
    final AtomicReference<Subject> creatorSubject = new AtomicReference<>();
    final AtomicReference<Subject> observedSubject = new AtomicReference<>();
    final AtomicReference<UserGroupInformation> observedUser =
        new AtomicReference<>();
    final AtomicReference<Thread> worker = new AtomicReference<>();

    creator.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws InterruptedException {
        creatorSubject.set(SubjectUtil.current());
        Runnable r = new Runnable() {
          @Override
          public void run() {
            observedSubject.set(SubjectUtil.current());
            observedUser.set(currentUser());
          }
        };

        ThreadFactory factory =
            BlockingThreadPoolExecutorService.newDaemonThreadFactory(THREAD_NAME);
        Thread t = factory.newThread(r);
        worker.set(t);
        t.start();
        t.join(JOIN_TIMEOUT_MILLIS);
        return null;
      }
    });

    assertObservedCreatorUser(creator, creatorSubject, observedSubject, observedUser,
        worker);
    assertTrue(worker.get().isDaemon(),
        "newDaemonThreadFactory did not produce a daemon thread");
  }

  /**
   * A thread created with no user in force runs as the process login and as nobody
   * else.
   * <p>
   * This is what makes the assertions above mean something. It establishes that the
   * login user really is observable when no identity was established, so a test that
   * observes the creating user instead has observed something the machinery had to
   * carry there, and it establishes that a thread created outside any scope does not
   * pick up the identity of some earlier creator.
   *
   * @throws Exception if a wait is interrupted
   */
  @Test
  @Timeout(value = 30)
  public void testThreadCreatedWithNoUserRunsAsTheLoginUser() throws Exception {
    final AtomicReference<Subject> observedSubject = new AtomicReference<>();
    final AtomicReference<UserGroupInformation> observedUser =
        new AtomicReference<>();

    assertNull(SubjectUtil.current(),
        "a Subject was already in force on the thread creating this one, so this "
            + "assertion would not be about a thread created without one");
    SubjectInheritingThread t = new SubjectInheritingThread() {
      @Override
      public void work() {
        observedSubject.set(SubjectUtil.current());
        observedUser.set(currentUser());
      }
    };
    t.start();
    t.join(JOIN_TIMEOUT_MILLIS);

    assertFalse(t.isAlive(),
        "thread under test did not finish within " + JOIN_TIMEOUT_MILLIS + " ms");
    assertNull(observedSubject.get(),
        "a thread created with no Subject in force observed one");
    assertNotNull(observedUser.get(),
        "thread under test never read back the user it was running as");
    assertEquals(LOGIN_USER, observedUser.get().getUserName(),
        "a thread created with no user in force did not run as the process login");
  }

}
