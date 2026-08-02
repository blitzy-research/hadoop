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
package org.apache.hadoop.util.concurrent;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.kms.ValueQueue;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ha.HAServiceTarget;
import org.apache.hadoop.ha.ZKFailoverController;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ReadaheadPool;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableRollingAverages;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.SubjectUtil;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.util.AsyncDiskService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers the places that used to build a plain JDK thread pool and now build one
 * of Hadoop's own, so that the identity of whoever submits work to them reaches
 * the thread that runs it.
 * <p>
 * Each place is pinned down twice, and the two ways fail for different reasons:
 * <ol>
 *   <li>The pool the place builds is asked for its type, and where the place
 *       hands the pool no further than a private field the field is read for
 *       it. Putting a plain JDK pool back would produce a pool of the wrong
 *       type, whichever runtime the test runs on.</li>
 *   <li>Work is submitted to that very pool under a named identity, and the
 *       identity the work sees is asserted. Building the pool by a route that
 *       does not carry identity would leave the work with no identity at all
 *       on a runtime that hands identity over at submission.</li>
 * </ol>
 * A third way covers the two places that keep their pool out of reach — one
 * builds it in a field of a class with no cheap concrete form and one keeps it
 * to a single method — by reading what each compiled class refers to. A class
 * that no longer refers to Hadoop's factory cannot be calling it.
 * <p>
 * What the submitted work is expected to see depends on the runtime, and the
 * difference is not incidental. On a runtime that hands the identity to a
 * thread when the thread is made, Hadoop leaves submitted work alone, so work
 * that lands on a thread made earlier sees nothing; the one thing that must
 * never happen there is seeing somebody else's identity. On a runtime that
 * confines the identity to the call that established it, the submission-time
 * capture is the only thing that can carry it across, so the work must see
 * exactly its submitter.
 */
public class TestExecutorRedirectSubjectPropagation {

  /** How long any single wait in this class is allowed to take. */
  private static final int TIMEOUT_SECONDS = 10;

  /** The name the submitting identity is built under. */
  private static final String SUBMITTER = "submitter";

  /** Where Hadoop's own executor factory lives, as a class file names it. */
  private static final String HADOOP_EXECUTORS =
      "org/apache/hadoop/util/concurrent/HadoopExecutors";

  /** Hadoop's own pool, as a class file names it. */
  private static final String HADOOP_POOL =
      "org/apache/hadoop/util/concurrent/HadoopThreadPoolExecutor";

  /** Hadoop's own scheduled pool, as a class file names it. */
  private static final String HADOOP_SCHEDULED_POOL =
      "org/apache/hadoop/util/concurrent/HadoopScheduledThreadPoolExecutor";

  /** The JDK factory every redirected factory call was moved off. */
  private static final String JDK_EXECUTORS = "java/util/concurrent/Executors";

  /** Pools this test built or took hold of, to be shut down afterwards. */
  private final List<ExecutorService> pools = new ArrayList<>();

  /** Anything else this test has to put back afterwards. */
  private final List<Runnable> restores = new ArrayList<>();

  /**
   * Puts back everything the test took hold of, whether it passed or not.
   */
  @AfterEach
  public void releaseWhatTheTestTook() {
    for (Runnable restore : restores) {
      restore.run();
    }
    restores.clear();
    for (ExecutorService pool : pools) {
      pool.shutdownNow();
    }
    pools.clear();
  }

  /**
   * Remembers a pool so that it is shut down when the test finishes.
   *
   * @param pool the pool to shut down afterwards
   * @param <T> the kind of pool
   * @return the same pool
   */
  private <T extends ExecutorService> T register(T pool) {
    pools.add(pool);
    return pool;
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
   * @param target the instance to read it from, or null for a static field
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
   * Runs one piece of work on a pool and waits for it, so that the pool is
   * known to have a thread of its own before the identity is established.
   * <p>
   * This is what makes the later assertion say something: work that reaches a
   * thread made before the identity existed can only have been given that
   * identity at submission.
   *
   * @param pool the pool to warm
   * @throws Exception if the warming work does not finish in time
   */
  private void prewarm(ExecutorService pool) throws Exception {
    CountDownLatch warmed = new CountDownLatch(1);
    pool.execute(warmed::countDown);
    assertTrue(warmed.await(TIMEOUT_SECONDS, TimeUnit.SECONDS),
        "the pool never ran the work that was meant to warm it");
  }

  /**
   * Submits work to a pool under an identity and reports what the work saw.
   *
   * @param pool the pool to submit to
   * @param submitter the identity to submit under
   * @return the identity the work saw, which may be null
   * @throws Exception if the work does not finish in time
   */
  private static Subject observeFrom(ExecutorService pool, Subject submitter)
      throws Exception {
    Future<Subject> observed = SubjectUtil.callAs(submitter,
        () -> pool.submit(
            TestExecutorRedirectSubjectPropagation::currentSubject));
    return observed.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  /**
   * Submits work under an identity through a door that takes work but gives
   * nothing back, and reports what the work saw.
   *
   * @param door the way work is handed in at the place under test
   * @param submitter the identity to submit under
   * @return the identity the work saw, which may be null
   * @throws Exception if the work does not finish in time
   */
  private static Subject observeThrough(Consumer<Runnable> door,
      Subject submitter) throws Exception {
    AtomicReference<Subject> seen = new AtomicReference<>();
    CountDownLatch ran = new CountDownLatch(1);
    SubjectUtil.callAs(submitter, () -> {
      door.accept(() -> {
        seen.set(currentSubject());
        ran.countDown();
      });
      return null;
    });
    assertTrue(ran.await(TIMEOUT_SECONDS, TimeUnit.SECONDS),
        "the work handed in at this place never ran");
    return seen.get();
  }

  /**
   * The pool a decorating executor hands its work on to.
   *
   * @param decorator the executor that wraps another
   * @return the pool underneath it
   * @throws Exception if no pool can be found underneath
   */
  private static ExecutorService poolUnderneath(ExecutorService decorator)
      throws Exception {
    for (Class<?> level = decorator.getClass(); level != null;
        level = level.getSuperclass()) {
      for (Field field : level.getDeclaredFields()) {
        if (ExecutorService.class.isAssignableFrom(field.getType())) {
          field.setAccessible(true);
          Object held = field.get(decorator);
          if (held instanceof ExecutorService) {
            return (ExecutorService) held;
          }
        }
      }
    }
    throw new AssertionError(
        "no pool was found underneath " + decorator.getClass().getName());
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
   * can be looked for. Every name a class refers to is spelled out in it.
   *
   * @param binaryName the name of the class, as source writes it
   * @return the class file, byte for byte, as text
   * @throws IOException if the class file cannot be read
   */
  private static String compiled(String binaryName) throws IOException {
    String resource = "/" + binaryName.replace('.', '/') + ".class";
    try (InputStream in = TestExecutorRedirectSubjectPropagation.class
        .getResourceAsStream(resource)) {
      assertNotNull(in, "the compiled form of " + binaryName
          + " was not found alongside the tests");
      return new String(in.readAllBytes(), StandardCharsets.ISO_8859_1);
    }
  }

  /**
   * Asserts that a compiled class refers to every one of the given names.
   *
   * @param binaryName the class to look in
   * @param names the names it has to refer to
   * @throws IOException if the class file cannot be read
   */
  private static void assertRefersTo(String binaryName, String... names)
      throws IOException {
    String bytes = compiled(binaryName);
    for (String name : names) {
      assertTrue(bytes.contains(name),
          binaryName + " no longer refers to " + name
              + ", so it cannot be building the pool it is meant to build");
    }
  }

  /**
   * Asserts that a compiled class refers to none of the given names.
   *
   * @param binaryName the class to look in
   * @param names the names it must not refer to
   * @throws IOException if the class file cannot be read
   */
  private static void assertDoesNotReferTo(String binaryName, String... names)
      throws IOException {
    String bytes = compiled(binaryName);
    for (String name : names) {
      assertFalse(bytes.contains(name),
          binaryName + " still refers to " + name
              + ", which the migration moved it off");
    }
  }

  /**
   * The pool that renews kerberos credentials runs its work under the identity
   * that asked for the renewal.
   * <p>
   * The method that builds the pool keeps to itself and the pool it builds is
   * kept in a field of the same class, so both are reached the only way they
   * can be. Handing it no work to do is enough: the pool is built before the
   * work is offered to it, so the refusal of the work leaves the pool built.
   *
   * @throws Exception if anything the test relies on cannot be reached
   */
  @Test
  @Timeout(value = 60)
  @SuppressWarnings("unchecked")
  public void testKerberosRenewalRunsUnderTheIdentityThatAskedForIt()
      throws Exception {
    UserGroupInformation asked =
        UserGroupInformation.createRemoteUser("tgt-renewal-user");
    Field held = UserGroupInformation.class
        .getDeclaredField("kerberosLoginRenewalExecutor");
    held.setAccessible(true);
    final Object beforeTheTest = held.get(null);
    restores.add(() -> {
      try {
        held.set(null, beforeTheTest);
      } catch (IllegalAccessException unreachable) {
        throw new AssertionError(unreachable);
      }
    });

    Method build = UserGroupInformation.class.getDeclaredMethod(
        "executeAutoRenewalTask", String.class, Class.forName(
            "org.apache.hadoop.security.UserGroupInformation"
                + "$AutoRenewalForUserCredsRunnable"));
    build.setAccessible(true);
    try {
      build.invoke(asked, "tgt-renewal-user", null);
    } catch (InvocationTargetException refusedTheWork) {
      // Offering no work is refused; the pool is already built by then.
      assertNotNull(refusedTheWork.getCause());
    }

    Optional<ExecutorService> built =
        (Optional<ExecutorService>) held.get(null);
    assertTrue(built.isPresent(),
        "renewing kerberos credentials did not leave a pool behind");
    ExecutorService renewal = register(built.get());
    assertSame(SubjectPreservingExecutorService.class, renewal.getClass(),
        "the kerberos renewal pool is not one of Hadoop's own");

    prewarm(renewal);
    Subject submitter = newSubject(SUBMITTER);
    assertSubmitterObserved(submitter, observeFrom(renewal, submitter),
        "the kerberos renewal pool");
  }

  /**
   * The timer a failover controller delays its own rechecks on runs its work
   * under the identity that scheduled it.
   * <p>
   * The controller builds the timer while it is being constructed, so the
   * smallest possible concrete controller is built to get at it. Nothing else
   * about the controller is exercised, and nothing it needs is stood up.
   *
   * @throws Exception if anything the test relies on cannot be reached
   */
  @Test
  @Timeout(value = 60)
  public void testFailoverRecheckTimerRunsUnderTheIdentityThatScheduledIt()
      throws Exception {
    ZKFailoverController controller =
        new DelayTimerOnlyController(new Configuration());
    ScheduledExecutorService delay = register(
        (ScheduledExecutorService) readField(ZKFailoverController.class,
            controller, "delayExecutor"));
    assertSame(HadoopScheduledThreadPoolExecutor.class, delay.getClass(),
        "the failover recheck timer is not one of Hadoop's own");

    prewarm(delay);
    Subject submitter = newSubject(SUBMITTER);
    assertSubmitterObserved(submitter, observeFrom(delay, submitter),
        "the failover recheck timer");
  }

  /**
   * A failover controller stripped to nothing but the timer it builds when it
   * is constructed. None of the methods below is called by this test.
   */
  private static final class DelayTimerOnlyController
      extends ZKFailoverController {

    DelayTimerOnlyController(Configuration conf) {
      super(conf, null);
    }

    @Override
    protected byte[] targetToData(HAServiceTarget target) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected HAServiceTarget dataToTarget(byte[] data) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected void loginAsFCUser() {
      throw new UnsupportedOperationException();
    }

    @Override
    protected void checkRpcAdminAccess() {
      throw new UnsupportedOperationException();
    }

    @Override
    protected InetSocketAddress getRpcAddressToBindTo() {
      throw new UnsupportedOperationException();
    }

    @Override
    protected PolicyProvider getPolicyProvider() {
      throw new UnsupportedOperationException();
    }

    @Override
    protected List<HAServiceTarget> getAllOtherNodes() {
      throw new UnsupportedOperationException();
    }

    @Override
    protected boolean isSSLEnabled() {
      throw new UnsupportedOperationException();
    }

    @Override
    protected String getScopeInsideParentNode() {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * The pool that drains the output of a shell command is one of Hadoop's own,
   * and a pool built the way that place builds it carries identity across.
   * <p>
   * The place keeps its pool to a single method and hands the work it submits
   * nothing that could report back, so what it builds is read off the compiled
   * class and a pool built the same way is then asked to carry an identity.
   * Together those say the work that drains the command runs under the identity
   * that started it.
   *
   * @throws Exception if anything the test relies on cannot be reached
   */
  @Test
  @Timeout(value = 60)
  public void testShellOutputDrainingUsesAPoolThatCarriesIdentity()
      throws Exception {
    assertRefersTo("org.apache.hadoop.fs.FileUtil",
        HADOOP_EXECUTORS, "newFixedThreadPool");
    assertDoesNotReferTo("org.apache.hadoop.fs.FileUtil", JDK_EXECUTORS);

    ExecutorService asThatPlaceBuildsIt =
        register(HadoopExecutors.newFixedThreadPool(2));
    assertSame(HadoopThreadPoolExecutor.class,
        asThatPlaceBuildsIt.getClass(),
        "a pool built the way shell output draining builds one is not one of"
            + " Hadoop's own");

    prewarm(asThatPlaceBuildsIt);
    Subject submitter = newSubject(SUBMITTER);
    assertSubmitterObserved(submitter,
        observeFrom(asThatPlaceBuildsIt, submitter),
        "a pool built the way shell output draining builds one");
  }

  /**
   * The timer that rolls quantile metrics over runs its work under the identity
   * that scheduled it.
   *
   * @throws Exception if anything the test relies on cannot be reached
   */
  @Test
  @Timeout(value = 60)
  public void testQuantileRolloverRunsUnderTheIdentityThatScheduledIt()
      throws Exception {
    ScheduledExecutorService rollover = (ScheduledExecutorService)
        readField(MutableQuantiles.class, null, "scheduler");
    assertSame(HadoopScheduledThreadPoolExecutor.class, rollover.getClass(),
        "the quantile rollover timer is not one of Hadoop's own");

    prewarm(rollover);
    Subject submitter = newSubject(SUBMITTER);
    assertSubmitterObserved(submitter, observeFrom(rollover, submitter),
        "the quantile rollover timer");
  }

  /**
   * The timer that rolls averages over runs its work under the identity that
   * scheduled it.
   *
   * @throws Exception if anything the test relies on cannot be reached
   */
  @Test
  @Timeout(value = 60)
  public void testAverageRolloverRunsUnderTheIdentityThatScheduledIt()
      throws Exception {
    ScheduledExecutorService rollover = (ScheduledExecutorService)
        readField(MutableRollingAverages.class, null, "SCHEDULER");
    assertSame(HadoopScheduledThreadPoolExecutor.class, rollover.getClass(),
        "the average rollover timer is not one of Hadoop's own");

    prewarm(rollover);
    Subject submitter = newSubject(SUBMITTER);
    assertSubmitterObserved(submitter, observeFrom(rollover, submitter),
        "the average rollover timer");
  }

  /**
   * The pool that reads ahead on file descriptors runs its work under the
   * identity that asked for the read.
   * <p>
   * The shared instance is only handed out where the native library is loaded,
   * so this builds one of its own the only way there is. That is the same code
   * that the shared instance is built by.
   *
   * @throws Exception if anything the test relies on cannot be reached
   */
  @Test
  @Timeout(value = 60)
  public void testReadaheadRunsUnderTheIdentityThatAskedForIt()
      throws Exception {
    Constructor<ReadaheadPool> howItIsBuilt =
        ReadaheadPool.class.getDeclaredConstructor();
    howItIsBuilt.setAccessible(true);
    ReadaheadPool ownedByThisTest = howItIsBuilt.newInstance();
    ThreadPoolExecutor readahead = register((ThreadPoolExecutor)
        readField(ReadaheadPool.class, ownedByThisTest, "pool"));
    assertSame(HadoopThreadPoolExecutor.class, readahead.getClass(),
        "the readahead pool is not one of Hadoop's own");

    prewarm(readahead);
    Subject submitter = newSubject(SUBMITTER);
    assertSubmitterObserved(submitter, observeFrom(readahead, submitter),
        "the readahead pool");
  }

  /**
   * The pool that reloads group memberships in the background runs its work
   * under the identity that asked for the reload.
   * <p>
   * The pool is wrapped before it is kept, so the type assertion looks through
   * the wrapper. Background reloading has to be turned on for the pool to be
   * built at all.
   *
   * @throws Exception if anything the test relies on cannot be reached
   */
  @Test
  @Timeout(value = 60)
  public void testBackgroundGroupReloadRunsUnderTheIdentityThatAskedForIt()
      throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(CommonConfigurationKeysPublic
        .HADOOP_SECURITY_GROUPS_CACHE_BACKGROUND_RELOAD, true);
    conf.setInt(CommonConfigurationKeysPublic
        .HADOOP_SECURITY_GROUPS_CACHE_BACKGROUND_RELOAD_THREADS, 1);
    Groups groups = new Groups(conf);

    Class<?> loaderClass =
        Class.forName("org.apache.hadoop.security.Groups$GroupCacheLoader");
    Constructor<?> howItIsBuilt =
        loaderClass.getDeclaredConstructor(Groups.class);
    howItIsBuilt.setAccessible(true);
    Object loader = howItIsBuilt.newInstance(groups);
    ExecutorService reload = register((ExecutorService)
        readField(loaderClass, loader, "executorService"));
    assertSame(HadoopThreadPoolExecutor.class,
        poolUnderneath(reload).getClass(),
        "the background group reload pool is not one of Hadoop's own");

    prewarm(reload);
    Subject submitter = newSubject(SUBMITTER);
    assertSubmitterObserved(submitter, observeFrom(reload, submitter),
        "the background group reload pool");
  }

  /**
   * The pool that refills key queues runs its work under the identity that
   * asked for the refill.
   * <p>
   * The queue this pool draws from only accepts the refill work the queue
   * itself names, so the pool is given room for a thread per piece of work and
   * is not warmed beforehand; work offered while the pool is below its core
   * size is handed straight to a thread.
   *
   * @throws Exception if anything the test relies on cannot be reached
   */
  @Test
  @Timeout(value = 60)
  public void testKeyQueueRefillRunsUnderTheIdentityThatAskedForIt()
      throws Exception {
    ValueQueue<String> keys = new ValueQueue<>(10, 0.5f,
        TimeUnit.MINUTES.toMillis(5), 2,
        ValueQueue.SyncGenerationPolicy.ALL, new NothingToRefill());
    ThreadPoolExecutor refill = register((ThreadPoolExecutor)
        readField(ValueQueue.class, keys, "executor"));
    assertSame(HadoopThreadPoolExecutor.class, refill.getClass(),
        "the key refill pool is not one of Hadoop's own");

    Subject submitter = newSubject(SUBMITTER);
    assertSubmitterObserved(submitter, observeFrom(refill, submitter),
        "the key refill pool");
  }

  /**
   * A refiller that adds nothing, so that building a key queue asks nothing of
   * the world outside the test.
   */
  private static final class NothingToRefill
      implements ValueQueue.QueueRefiller<String> {

    @Override
    public void fillQueueForKey(String keyName, Queue<String> keyQueue,
        int numValues) {
    }
  }

  /**
   * The pool a multi-threaded copy runs its copies on runs them under the
   * identity that asked for the copy.
   * <p>
   * A concrete copy command is built and told to make its pool, without any
   * file system or argument being involved.
   *
   * @throws Exception if anything the test relies on cannot be reached
   */
  @Test
  @Timeout(value = 60)
  public void testMultiThreadedCopyRunsUnderTheIdentityThatAskedForIt()
      throws Exception {
    Class<?> concreteCopy =
        Class.forName("org.apache.hadoop.fs.shell.CopyCommands$Put");
    Constructor<?> howItIsBuilt = concreteCopy.getDeclaredConstructor();
    howItIsBuilt.setAccessible(true);
    Object copy = howItIsBuilt.newInstance();

    Class<?> multiThreaded = Class
        .forName("org.apache.hadoop.fs.shell.CopyCommandWithMultiThread");
    Method setThreadCount =
        multiThreaded.getDeclaredMethod("setThreadCount", String.class);
    setThreadCount.setAccessible(true);
    setThreadCount.invoke(copy, "1");
    Method makeThePool =
        multiThreaded.getDeclaredMethod("initThreadPoolExecutor");
    makeThePool.setAccessible(true);
    makeThePool.invoke(copy);
    Method thePool = multiThreaded.getDeclaredMethod("getExecutor");
    thePool.setAccessible(true);

    ThreadPoolExecutor copies =
        register((ThreadPoolExecutor) thePool.invoke(copy));
    assertSame(HadoopThreadPoolExecutor.class, copies.getClass(),
        "the multi-threaded copy pool is not one of Hadoop's own");

    prewarm(copies);
    Subject submitter = newSubject(SUBMITTER);
    assertSubmitterObserved(submitter, observeFrom(copies, submitter),
        "the multi-threaded copy pool");
  }

  /**
   * Work handed to the asynchronous disk service runs under the identity that
   * handed it in.
   * <p>
   * This place is reached entirely through what it offers publicly, so the work
   * goes in the way callers send it and the identity is read from inside the
   * work itself.
   *
   * @throws Exception if anything the test relies on cannot be reached
   */
  @Test
  @Timeout(value = 60)
  public void testAsyncDiskWorkRunsUnderTheIdentityThatHandedItIn()
      throws Exception {
    final String volume = "volume-under-test";
    AsyncDiskService service = new AsyncDiskService(new String[] {volume});
    Map<?, ?> perVolume = (Map<?, ?>)
        readField(AsyncDiskService.class, service, "executors");
    ExecutorService forThatVolume =
        register((ExecutorService) perVolume.get(volume));
    assertSame(HadoopThreadPoolExecutor.class, forThatVolume.getClass(),
        "the pool the asynchronous disk service keeps per volume is not one of"
            + " Hadoop's own");

    CountDownLatch warmed = new CountDownLatch(1);
    service.execute(volume, warmed::countDown);
    assertTrue(warmed.await(TIMEOUT_SECONDS, TimeUnit.SECONDS),
        "the asynchronous disk service never ran the warming work");

    Subject submitter = newSubject(SUBMITTER);
    assertSubmitterObserved(submitter,
        observeThrough(work -> service.execute(volume, work), submitter),
        "the asynchronous disk service");
    service.shutdown();
  }

  /**
   * The timer that keeps an RPC server's metrics up to date runs its work under
   * the identity that scheduled it.
   * <p>
   * The smallest possible concrete server is built on a port the system picks,
   * with the update interval put far enough out that the server's own update
   * never runs during the test, and is stopped again afterwards.
   *
   * @throws Exception if anything the test relies on cannot be reached
   */
  @Test
  @Timeout(value = 60)
  public void testRpcMetricsUpdaterRunsUnderTheIdentityThatScheduledIt()
      throws Exception {
    Configuration conf = new Configuration();
    conf.setLong(CommonConfigurationKeysPublic
        .IPC_SERVER_METRICS_UPDATE_RUNNER_INTERVAL, TimeUnit.HOURS
        .toMillis(1));
    Server server = new MetricsUpdaterOnlyServer(conf);
    try {
      ScheduledExecutorService updater = (ScheduledExecutorService)
          readField(Server.class, server, "scheduledExecutorService");
      assertSame(HadoopScheduledThreadPoolExecutor.class, updater.getClass(),
          "the RPC metrics updater timer is not one of Hadoop's own");

      prewarm(updater);
      Subject submitter = newSubject(SUBMITTER);
      assertSubmitterObserved(submitter, observeFrom(updater, submitter),
          "the RPC metrics updater timer");
    } finally {
      server.stop();
    }
  }

  /**
   * An RPC server stripped to nothing but the metrics updater it schedules when
   * it is constructed. It answers no calls and is never started.
   */
  private static final class MetricsUpdaterOnlyServer extends Server {

    MetricsUpdaterOnlyServer(Configuration conf) throws IOException {
      super("localhost", 0, LongWritable.class, 1, conf);
    }

    @Override
    public Writable call(RPC.RpcKind rpcKind, String protocol, Writable param,
        long receiveTime) {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Every place that was moved onto Hadoop's own pools still names them.
   * <p>
   * This covers the two places whose pool cannot be reached from a test at all,
   * and covers every other place a second time. A place that no longer names
   * Hadoop's factory, or names the JDK's again, cannot be building the pool it
   * is meant to build, whichever runtime the test runs on.
   *
   * @throws Exception if a compiled class cannot be read
   */
  @Test
  @Timeout(value = 60)
  public void testEveryRedirectedPlaceStillNamesHadoopsOwnPool()
      throws Exception {
    String[][] throughHadoopsFactory = {
        {"org.apache.hadoop.security.UserGroupInformation",
            "newSingleThreadExecutor"},
        {"org.apache.hadoop.ha.ZKFailoverController",
            "newScheduledThreadPool"},
        {"org.apache.hadoop.fs.FileUtil", "newFixedThreadPool"},
        {"org.apache.hadoop.metrics2.lib.MutableQuantiles",
            "newScheduledThreadPool"},
        {"org.apache.hadoop.metrics2.lib.MutableRollingAverages",
            "newScheduledThreadPool"},
    };
    for (String[] place : throughHadoopsFactory) {
      assertRefersTo(place[0], HADOOP_EXECUTORS, place[1]);
      assertDoesNotReferTo(place[0], JDK_EXECUTORS);
    }

    String[] buildingHadoopsPoolDirectly = {
        "org.apache.hadoop.io.ReadaheadPool",
        "org.apache.hadoop.security.Groups$GroupCacheLoader",
        "org.apache.hadoop.crypto.key.kms.ValueQueue",
        "org.apache.hadoop.fs.shell.CopyCommandWithMultiThread",
        "org.apache.hadoop.util.AsyncDiskService",
    };
    for (String place : buildingHadoopsPoolDirectly) {
      assertRefersTo(place, HADOOP_POOL);
    }

    assertRefersTo("org.apache.hadoop.ipc.Server", HADOOP_SCHEDULED_POOL);

    assertEquals(11,
        throughHadoopsFactory.length + buildingHadoopsPoolDirectly.length + 1,
        "the places named here no longer add up to the places that were"
            + " moved onto Hadoop's own pools in this module");
  }
}
