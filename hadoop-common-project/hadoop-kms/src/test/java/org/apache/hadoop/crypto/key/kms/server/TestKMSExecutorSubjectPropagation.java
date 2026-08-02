/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.crypto.key.kms.server;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.util.SubjectUtil;
import org.apache.hadoop.util.concurrent.HadoopScheduledThreadPoolExecutor;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers the two places in the KMS that used to build a plain JDK timer and now
 * build one of Hadoop's own: the timer that reloads the ACL file and the timer
 * that clears out aggregated audit events.
 * <p>
 * Each is pinned down three ways, and the three fail for different reasons. The
 * timer the place builds is asked for its type; work is submitted to that very
 * timer under a named identity and the identity the work sees is asserted; and
 * the compiled class is read for the name of the factory it calls, which holds
 * whether or not the place can be built at all.
 * <p>
 * What the submitted work is expected to see depends on the runtime. On a
 * runtime that hands the identity to a thread when the thread is made, Hadoop
 * leaves submitted work alone, so work landing on a thread made earlier sees
 * nothing and the only thing that must never happen is seeing somebody else's
 * identity. On a runtime that confines the identity to the call that
 * established it, the capture taken at submission is the only thing that can
 * carry it across, so the work must see exactly its submitter.
 */
public class TestKMSExecutorSubjectPropagation {

  /** How long any single wait in this class is allowed to take. */
  private static final int TIMEOUT_SECONDS = 10;

  /** The name the submitting identity is built under. */
  private static final String SUBMITTER = "kms-submitter";

  /** Where Hadoop's own executor factory lives, as a class file names it. */
  private static final String HADOOP_EXECUTORS =
      "org/apache/hadoop/util/concurrent/HadoopExecutors";

  /** The JDK factory both places were moved off. */
  private static final String JDK_EXECUTORS = "java/util/concurrent/Executors";

  /**
   * Builds an identity that can be told apart from any other.
   *
   * @param name the name to build it under
   * @return an identity carrying a single principal of that name
   */
  private static Subject newSubject(String name) {
    Set<Principal> principals = new HashSet<>();
    principals.add(new KerberosPrincipal(name + "@EXAMPLE.COM"));
    return new Subject(false, principals, new HashSet<>(), new HashSet<>());
  }

  /**
   * The identity of the thread asking, or null when it has none.
   *
   * @return the identity in force on this thread
   */
  private static Subject currentSubject() {
    return SubjectUtil.current();
  }

  /**
   * Reads a field a class keeps to itself.
   *
   * @param owner the class that declares the field
   * @param target the instance to read it from
   * @param name the name of the field
   * @return whatever the field holds
   * @throws Exception if the field is not there or cannot be read
   */
  private static Object readField(Class<?> owner, Object target, String name)
      throws Exception {
    Field field = owner.getDeclaredField(name);
    field.setAccessible(true);
    return field.get(target);
  }

  /**
   * Runs one piece of work on a timer and waits for it, so that the timer is
   * known to have a thread of its own before the identity is established.
   *
   * @param pool the timer to warm
   * @throws Exception if the warming work does not finish in time
   */
  private static void prewarm(ExecutorService pool) throws Exception {
    CountDownLatch warmed = new CountDownLatch(1);
    pool.execute(warmed::countDown);
    assertTrue(warmed.await(TIMEOUT_SECONDS, TimeUnit.SECONDS),
        "the timer never ran the work that was meant to warm it");
  }

  /**
   * Submits work to a timer under an identity and reports what the work saw.
   *
   * @param pool the timer to submit to
   * @param submitter the identity to submit under
   * @return the identity the work saw, which may be null
   * @throws Exception if the work does not finish in time
   */
  private static Subject observeFrom(ExecutorService pool, Subject submitter)
      throws Exception {
    Future<Subject> observed = SubjectUtil.callAs(submitter,
        () -> pool.submit(
            TestKMSExecutorSubjectPropagation::currentSubject));
    return observed.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  /**
   * Asserts that work submitted under an identity ran under that identity.
   * <p>
   * The identity is read at each submission and established again around the
   * run, so this holds on every runtime and whichever worker the work lands
   * on, including one an earlier submission brought into being.
   *
   * @param submitter the identity the work was submitted under
   * @param observed the identity the work saw
   * @param what the place being asserted about, for the failure message
   */
  private static void assertSubmitterObserved(Subject submitter,
      Subject observed, String what) {
    assertSame(submitter, observed,
        what + " did not run its work under the identity that submitted it");
  }

  /**
   * The bytes of a compiled class, read as text so that the names it refers to
   * can be looked for.
   *
   * @param binaryName the name of the class, as source writes it
   * @return the class file, byte for byte, as text
   * @throws IOException if the class file cannot be read
   */
  private static String compiled(String binaryName) throws IOException {
    String resource = "/" + binaryName.replace('.', '/') + ".class";
    try (InputStream in = TestKMSExecutorSubjectPropagation.class
        .getResourceAsStream(resource)) {
      assertNotNull(in, "the compiled form of " + binaryName
          + " was not found alongside the tests");
      return new String(in.readAllBytes(), StandardCharsets.ISO_8859_1);
    }
  }

  /**
   * The timer that reloads the ACL file runs its work under the identity that
   * started the reloading.
   *
   * @throws Exception if anything the test relies on cannot be reached
   */
  @Test
  @Timeout(value = 60)
  public void testAclReloaderRunsUnderTheIdentityThatStartedIt()
      throws Exception {
    KMSACLs acls = new KMSACLs(new Configuration(false));
    acls.startReloader();
    try {
      ScheduledExecutorService reloader = (ScheduledExecutorService)
          readField(KMSACLs.class, acls, "executorService");
      assertNotNull(reloader, "starting the reloader left no timer behind");
      assertSame(HadoopScheduledThreadPoolExecutor.class, reloader.getClass(),
          "the ACL reloader timer is not one of Hadoop's own");

      prewarm(reloader);
      Subject submitter = newSubject(SUBMITTER);
      assertSubmitterObserved(submitter, observeFrom(reloader, submitter),
          "the ACL reloader timer");
    } finally {
      acls.stopReloader();
    }
  }

  /**
   * The timer that clears out aggregated audit events runs its work under the
   * identity that built the audit.
   *
   * @throws Exception if anything the test relies on cannot be reached
   */
  @Test
  @Timeout(value = 60)
  public void testAuditCleanupRunsUnderTheIdentityThatBuiltIt()
      throws Exception {
    Configuration conf = new Configuration(false);
    conf.setLong(KMSConfiguration.KMS_AUDIT_AGGREGATION_WINDOW,
        TimeUnit.HOURS.toMillis(1));
    KMSAudit audit = new KMSAudit(conf);
    try {
      ScheduledExecutorService cleanup = (ScheduledExecutorService)
          readField(KMSAudit.class, audit, "executor");
      assertNotNull(cleanup, "building the audit left no timer behind");
      assertSame(HadoopScheduledThreadPoolExecutor.class, cleanup.getClass(),
          "the audit cleanup timer is not one of Hadoop's own");

      prewarm(cleanup);
      Subject submitter = newSubject(SUBMITTER);
      assertSubmitterObserved(submitter, observeFrom(cleanup, submitter),
          "the audit cleanup timer");
    } finally {
      audit.shutdown();
    }
  }

  /**
   * Both KMS places still name Hadoop's own factory rather than the JDK's.
   * <p>
   * This holds whether or not either place can be built, so it catches a revert
   * that the tests above could not reach.
   *
   * @throws Exception if a compiled class cannot be read
   */
  @Test
  @Timeout(value = 60)
  public void testBothKmsPlacesStillNameHadoopsOwnFactory() throws Exception {
    for (String place : new String[] {
        "org.apache.hadoop.crypto.key.kms.server.KMSACLs",
        "org.apache.hadoop.crypto.key.kms.server.KMSAudit"}) {
      String bytes = compiled(place);
      assertTrue(bytes.contains(HADOOP_EXECUTORS),
          place + " no longer refers to " + HADOOP_EXECUTORS
              + ", so it cannot be building the timer it is meant to build");
      assertTrue(bytes.contains("newScheduledThreadPool"),
          place + " no longer asks for a scheduled pool by name");
      assertFalse(bytes.contains(JDK_EXECUTORS),
          place + " still refers to " + JDK_EXECUTORS
              + ", which the migration moved it off");
    }
  }
}
