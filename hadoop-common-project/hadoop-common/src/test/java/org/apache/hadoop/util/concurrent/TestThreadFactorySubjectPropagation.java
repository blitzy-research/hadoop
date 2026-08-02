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
 * Asserts that a thread created and started through Hadoop's own thread
 * utilities observes the Subject of the thread that created and started it.
 * <p>
 * The utilities exercised here are {@link SubjectInheritingThread},
 * {@link Daemon}, and the two thread factories built on them,
 * {@link Daemon.DaemonFactory} and
 * {@link BlockingThreadPoolExecutorService#newDaemonThreadFactory(String)}. Each
 * of them owes that guarantee to its callers, and losing it is silent: the code
 * still compiles, throws nothing and logs nothing, while
 * {@link UserGroupInformation#getCurrentUser()} falls back to whoever the
 * process logged in as, so the work is authorized and recorded against that user
 * rather than the one it was created for. That is why the guarantee is asserted
 * here directly, rather than inferred from the behaviour of anything built on
 * top of it.
 * <p>
 * Every assertion below is an outcome, namely the Subject the new thread
 * observes, and never the mechanism that delivers it. Java 21 and earlier hand a
 * thread the Subject in force when it was created and the utilities let that
 * through; Java 24 and later do not, and the utilities take a copy themselves
 * and re-establish it in the new thread. The outcome is the same either way, so
 * every assertion here holds on both, with no branch on the runtime and no test
 * skipped on either.
 * <p>
 * Each test both creates and starts its thread inside the scope that establishes
 * the Subject, because the two mechanisms between them require it: one takes its
 * copy when the thread is constructed, the other when it is started. Creating
 * outside that scope and starting inside it, or the reverse, would satisfy only
 * one of them and would pass on one runtime and fail on the other.
 * <p>
 * Every constructor of both utilities is covered, since each is a way for
 * production code to create a thread. The distinction that makes that worthwhile
 * is that {@link SubjectInheritingThread} runs a Runnable only where the
 * Runnable reached it through a constructor that accepts one; the constructors
 * that take none need {@link SubjectInheritingThread#work()} overridden instead,
 * and are covered in that form.
 * <p>
 * A Subject observed is necessary but not sufficient, so each utility is covered
 * a second time through {@link UserGroupInformation}, which is what production
 * code authorizes and audits against. These tests install a login user that no
 * test ever creates a thread under and require the observed user to be the
 * creating one and not that login user, which turns the fall back described
 * above into a failure rather than a silent pass.
 */
public class TestThreadFactorySubjectPropagation {

  /**
   * Bound on every wait here. Each test then asserts that the thread it waited
   * for really did terminate, so an expired wait is reported as such.
   */
  private static final long JOIN_TIMEOUT_MILLIS = 1000;

  /**
   * The realm every principal here is qualified with, so that building one
   * resolves no default realm and needs no Kerberos configuration at all.
   */
  private static final String REALM = "@EXAMPLE.COM";

  private static final String THREAD_NAME = "test-subject-propagation";

  /**
   * The user the process is taken to have logged in as. No thread here is ever
   * created under it, so observing it inside a thread under test can only mean
   * that the creating identity failed to reach that thread and
   * {@link UserGroupInformation#getCurrentUser()} fell back to the login.
   */
  private static final String LOGIN_USER = "sentinel-login-user";

  /**
   * Installs a known login user before each test, so that a fall back to it is
   * recognisable. The identity machinery keeps process-wide state, so it is put
   * into a known state here rather than being taken as found.
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
   * Reads the user in force on the calling thread. A thread under test can
   * declare no checked exception, and failing to read the identity at all is a
   * failure of the test rather than an observation, so it is reported unchecked
   * and leaves the observation unwritten.
   *
   * @return the user in force, which is the login user when no Subject is
   */
  private static UserGroupInformation currentUser() {
    try {
      return UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      throw new IllegalStateException("the user in force could not be read", e);
    }
  }

  /**
   * Builds a Subject carrying a single, distinctly named principal. The
   * principal is what keeps each Subject here distinguishable: a Subject with no
   * principals and no credentials compares equal to any other such Subject, so
   * an assertion against a bare one could pass against the wrong instance.
   */
  private static Subject creatorSubject(String name) {
    Subject subject = new Subject();
    subject.getPrincipals().add(new KerberosPrincipal(name + REALM));
    return subject;
  }

  /**
   * Asserts that the thread under test finished, and that it observed the
   * Subject established on the thread which created it.
   * <p>
   * Both are needed, in that order. {@link Thread#join(long)} returns as readily
   * because its timeout expired as because the thread ended, so it answers only
   * the first question when it is followed by an assertion that the thread is no
   * longer alive; making that assertion first reports a thread that overran as
   * exactly that, rather than leaving it to be diagnosed from the empty
   * observation it would leave behind. The Subject itself is compared by
   * reference, which is both exact and available: the utilities re-establish the
   * very instance they were given.
   */
  private static void assertObservedCreatorSubject(Subject creator,
      AtomicReference<Subject> observed, AtomicReference<Thread> worker) {
    assertFalse(worker.get().isAlive(),
        "thread under test did not finish within " + JOIN_TIMEOUT_MILLIS + " ms");
    assertSame(creator, observed.get(),
        "thread under test did not observe the creating thread's Subject");
  }

  /**
   * Asserts that the thread under test finished, and that it ran as exactly the
   * user that created it rather than as the process login.
   * <p>
   * Three readings are made, each ruling out a different way of appearing to
   * work: the Subject by reference against the one the creating thread held
   * inside its own scope; the user as an identity, which
   * {@link UserGroupInformation#equals(Object)} decides by comparing Subjects by
   * reference, so a second user of the same name cannot satisfy it; and the
   * observed user's name against the login user's, which is what catches an
   * identity that never arrived at all.
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
   * A thread created with no user in force runs as the process login and as
   * nobody else. This is the control that makes the assertions above mean
   * something: it establishes that the login user really is observable where no
   * identity was established, so a test that observes the creating user instead
   * has observed something the machinery had to carry there.
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
