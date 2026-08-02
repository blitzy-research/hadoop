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

package org.apache.hadoop.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * Asserts that {@link PlatformName} can still tell whether a class of the
 * running Java runtime is available.
 * <p>
 * That question used to be asked from inside a privileged block, which the
 * runtime no longer treats as anything other than a plain call. The block is
 * gone and the lookup is made directly, so what has to be shown is that the
 * lookup still answers correctly: {@code true} for a class the runtime has and
 * {@code false} for one it does not, including for a name no class could ever
 * have. The answer decides whether Hadoop configures itself for a vendor's own
 * Kerberos login module, so an answer of {@code false} where it should be
 * {@code true} would silently change how a secure cluster logs in.
 * <p>
 * The lookup is not public, because callers are meant to read
 * {@link PlatformName#IBM_JAVA} rather than ask about a class themselves. On
 * the runtimes this project is built and tested with, that constant is decided
 * by the vendor name alone and never reaches the lookup, which is why the
 * lookup is called here directly. The constant is asserted too, against the
 * same lookup applied to the same module names, so that the two cannot drift
 * apart on any vendor.
 */
public class TestPlatformName {

  /**
   * The login modules whose presence marks an IBM Java Technology Edition
   * runtime, as {@link PlatformName} lists them.
   */
  private static final List<String> IBM_LOGIN_MODULES = Arrays.asList(
      "com.ibm.security.auth.module.JAASLoginModule",
      "com.ibm.security.auth.module.Win64LoginModule",
      "com.ibm.security.auth.module.NTLoginModule",
      "com.ibm.security.auth.module.AIX64LoginModule",
      "com.ibm.security.auth.module.LinuxLoginModule",
      "com.ibm.security.auth.module.Krb5LoginModule");

  /**
   * Asks {@link PlatformName} whether a class of the running runtime is
   * available.
   *
   * @param className the name of the class to ask about
   * @return what the lookup answered
   * @throws Exception if the lookup cannot be reached or fails outright
   */
  private static boolean isSystemClassAvailable(String className)
      throws Exception {
    Method lookup = PlatformName.class
        .getDeclaredMethod("isSystemClassAvailable", String.class);
    lookup.setAccessible(true);
    return (Boolean) lookup.invoke(null, className);
  }

  /**
   * A class the running runtime has is reported as available.
   * <p>
   * Each of these is loaded by the runtime itself rather than from a jar of
   * this project, which is what the lookup exists to find, and one of them is
   * the very class the security packages this decision feeds into are written
   * against.
   *
   * @throws Exception if the lookup cannot be reached or fails outright
   */
  @Test
  public void testClassOfTheRuntimeIsAvailable() throws Exception {
    assertTrue(isSystemClassAvailable("java.lang.String"),
        "a class every runtime has was reported as unavailable");
    assertTrue(isSystemClassAvailable("javax.security.auth.Subject"),
        "the class the security packages are written against was reported as "
            + "unavailable");
    assertTrue(isSystemClassAvailable("javax.security.auth.login.LoginContext"),
        "the class a login is performed through was reported as unavailable");
  }

  /**
   * A class the running runtime does not have is reported as unavailable, and
   * so is a name no class could have.
   * <p>
   * The second of those is what shows the lookup reports rather than throws:
   * an outright rejection of the name by the runtime has to become an answer of
   * {@code false} like any other absence, or asking about a vendor's login
   * module on another vendor's runtime would fail instead of answering.
   *
   * @throws Exception if the lookup cannot be reached or fails outright
   */
  @Test
  public void testClassTheRuntimeDoesNotHaveIsUnavailable() throws Exception {
    assertFalse(isSystemClassAvailable(
        "org.apache.hadoop.util.NoSuchClassOnAnyRuntime"),
        "a class no runtime has was reported as available");
    assertFalse(isSystemClassAvailable("not a class name at all"),
        "a name no class could have was reported as available");
    assertFalse(isSystemClassAvailable(""),
        "an empty name was reported as a class that is available");
  }

  /**
   * The IBM Java Technology Edition constant agrees with the lookup it is
   * derived from.
   * <p>
   * On a runtime of another vendor the vendor name decides the constant on its
   * own and the lookup is never reached, so recomputing the constant here from
   * the same vendor name and the same module names is what ties the two
   * together on every vendor: were the lookup to start answering wrongly, the
   * two would disagree on an IBM runtime and this would say so.
   *
   * @throws Exception if the lookup cannot be reached or fails outright
   */
  @Test
  public void testIbmJavaAgreesWithTheLookupItComesFrom() throws Exception {
    boolean anyModuleAvailable = false;
    for (String module : IBM_LOGIN_MODULES) {
      anyModuleAvailable |= isSystemClassAvailable(module);
    }
    boolean expected =
        PlatformName.JAVA_VENDOR_NAME.contains("IBM") && anyModuleAvailable;

    assertEquals(expected, PlatformName.IBM_JAVA,
        "the IBM Java Technology Edition constant does not agree with the "
            + "vendor name and the login modules it is derived from");
  }

  /**
   * The platform description is reported as the running runtime describes
   * itself.
   * <p>
   * This is read by callers outside this project and is built from the same
   * three runtime properties on every platform, so it is asserted against them
   * rather than against any fixed text.
   */
  @Test
  public void testPlatformNameDescribesTheRunningRuntime() {
    String operatingSystem = System.getProperty("os.name");
    String expected =
        (operatingSystem.startsWith("Windows") ? System.getenv("os")
            : operatingSystem)
            + "-" + System.getProperty("os.arch")
            + "-" + System.getProperty("sun.arch.data.model");

    assertEquals(expected, PlatformName.PLATFORM_NAME,
        "the platform description does not describe the running runtime");
    assertEquals(System.getProperty("java.vendor"),
        PlatformName.JAVA_VENDOR_NAME,
        "the vendor name is not the vendor name of the running runtime");
    assertNotNull(PlatformName.PLATFORM_NAME,
        "the platform has no description at all");
  }
}
