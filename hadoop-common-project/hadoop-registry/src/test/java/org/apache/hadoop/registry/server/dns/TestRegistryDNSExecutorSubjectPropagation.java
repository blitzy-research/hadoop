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
package org.apache.hadoop.registry.server.dns;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.xbill.DNS.EDNSOption;
import org.xbill.DNS.Flags;
import org.xbill.DNS.Lookup;
import org.xbill.DNS.Message;
import org.xbill.DNS.Name;
import org.xbill.DNS.Resolver;
import org.xbill.DNS.Section;
import org.xbill.DNS.TSIG;
import org.xbill.DNS.Type;

import org.apache.hadoop.security.authentication.util.SubjectUtil;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers the place a registry DNS lookup used to run on a plain JDK pool and now
 * runs on one of Hadoop's own, so that the identity of whoever asked for the
 * lookup reaches the thread that performs it.
 * <p>
 * The pool this place builds never leaves the method that builds it, and the
 * work it submits performs a name lookup rather than calling anything a test
 * supplies. So the lookup itself is what reports back: the resolver every
 * lookup goes through is replaced with one that records the identity in force
 * and the thread it was called on, and the lookup is then asked for under a
 * named identity. That records the identity from inside the very work the pool
 * ran.
 * <p>
 * The class is also read for the name of the factory it calls. That matters
 * here more than elsewhere, because this class builds a second pool through the
 * same factory by a different name; asking for the name of this call keeps the
 * two apart.
 * <p>
 * What the lookup is expected to see depends on the runtime. On a runtime that
 * hands the identity to a thread when the thread is made, the pool's thread is
 * made while the identity is in force, so the lookup sees it either way; the one
 * thing that must never happen is seeing somebody else's identity. On a runtime
 * that confines the identity to the call that established it, a thread never
 * inherits anything, so the capture taken at submission is the only thing that
 * can carry it and the lookup must see exactly its asker.
 */
public class TestRegistryDNSExecutorSubjectPropagation {

  /** The name the asking identity is built under. */
  private static final String ASKER = "dns-asker";

  /** Where Hadoop's own executor factory lives, as a class file names it. */
  private static final String HADOOP_EXECUTORS =
      "org/apache/hadoop/util/concurrent/HadoopExecutors";

  /** The JDK factory this place was moved off. */
  private static final String JDK_EXECUTORS = "java/util/concurrent/Executors";

  /** The resolver that was in place before this test replaced it. */
  private Resolver resolverBeforeTheTest;

  /**
   * Puts the resolver back, whether the test passed or not.
   */
  @AfterEach
  public void releaseWhatTheTestTook() {
    if (resolverBeforeTheTest != null) {
      Lookup.setDefaultResolver(resolverBeforeTheTest);
      resolverBeforeTheTest = null;
    }
  }

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
   * The bytes of a compiled class, read as text so that the names it refers to
   * can be looked for.
   *
   * @param binaryName the name of the class, as source writes it
   * @return the class file, byte for byte, as text
   * @throws IOException if the class file cannot be read
   */
  private static String compiled(String binaryName) throws IOException {
    String resource = "/" + binaryName.replace('.', '/') + ".class";
    try (InputStream in = TestRegistryDNSExecutorSubjectPropagation.class
        .getResourceAsStream(resource)) {
      assertNotNull(in, "the compiled form of " + binaryName
          + " was not found alongside the tests");
      return new String(in.readAllBytes(), StandardCharsets.ISO_8859_1);
    }
  }

  /**
   * A resolver that answers nothing and records who asked and from where.
   * <p>
   * Answering with an empty reply is enough: what is being observed is the
   * identity in force while the lookup runs, not the records it finds.
   */
  private static final class RecordingResolver implements Resolver {

    /** The identity in force when the lookup reached this resolver. */
    private final AtomicReference<Subject> identity = new AtomicReference<>();

    /** The thread the lookup reached this resolver on. */
    private final AtomicReference<Thread> thread = new AtomicReference<>();

    /** Whether the lookup reached this resolver at all. */
    private volatile boolean reached;

    @Override
    public Message send(Message query) {
      identity.set(SubjectUtil.current());
      thread.set(Thread.currentThread());
      reached = true;
      Message answer = new Message(query.getHeader().getID());
      answer.getHeader().setFlag(Flags.QR);
      answer.addRecord(query.getQuestion(), Section.QUESTION);
      return answer;
    }

    @Override
    public void setPort(int port) {
    }

    @Override
    public void setTCP(boolean flag) {
    }

    @Override
    public void setIgnoreTruncation(boolean flag) {
    }

    @Override
    public void setEDNS(int version, int payloadSize, int flags,
        List<EDNSOption> options) {
    }

    @Override
    public void setTSIGKey(TSIG key) {
    }

    @Override
    public void setTimeout(Duration timeout) {
    }
  }

  /**
   * A registry DNS lookup runs under the identity that asked for it.
   *
   * @throws Exception if anything the test relies on cannot be reached
   */
  @Test
  @Timeout(value = 60)
  public void testDnsLookupRunsUnderTheIdentityThatAskedForIt()
      throws Exception {
    RecordingResolver recording = new RecordingResolver();
    resolverBeforeTheTest = Lookup.getDefaultResolver();
    Lookup.setDefaultResolver(recording);

    RegistryDNS dns = new RegistryDNS("registry-dns-under-test");
    Name asked = Name.fromString("subject-propagation.example.");
    Subject asker = newSubject(ASKER);

    SubjectUtil.callAs(asker, () -> dns.getRecords(asked, Type.A));

    assertTrue(recording.reached,
        "the lookup never reached the resolver, so nothing was observed");
    assertNotSame(Thread.currentThread(), recording.thread.get(),
        "the lookup ran on the asking thread rather than on the pool this"
            + " place builds");

    Subject observed = recording.identity.get();
    assertSame(asker, observed,
        "the registry DNS lookup did not run under the identity that asked"
            + " for it");
  }

  /**
   * The lookup still asks Hadoop's own factory for a single-threaded pool.
   * <p>
   * The name of the call is asserted, not merely the factory, because this class
   * asks the same factory for a cached pool elsewhere; without the name a revert
   * of one call would hide behind the other.
   *
   * @throws Exception if the compiled class cannot be read
   */
  @Test
  @Timeout(value = 60)
  public void testLookupStillNamesHadoopsOwnSingleThreadedFactory()
      throws Exception {
    String place = "org.apache.hadoop.registry.server.dns.RegistryDNS";
    String bytes = compiled(place);
    assertTrue(bytes.contains(HADOOP_EXECUTORS),
        place + " no longer refers to " + HADOOP_EXECUTORS
            + ", so it cannot be building the pool it is meant to build");
    assertTrue(bytes.contains("newSingleThreadExecutor"),
        place + " no longer asks for a single-threaded pool by name, which is"
            + " the call the lookup makes");
    assertTrue(bytes.contains("newCachedThreadPool"),
        place + " no longer asks for a cached pool by name, which is the other"
            + " call this class makes through the same factory");
    assertFalse(bytes.contains(JDK_EXECUTORS),
        place + " still refers to " + JDK_EXECUTORS
            + ", which the migration moved it off");
  }
}
