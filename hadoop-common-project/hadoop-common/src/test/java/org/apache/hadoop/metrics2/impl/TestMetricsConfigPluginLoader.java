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

package org.apache.hadoop.metrics2.impl;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.hadoop.metrics2.MetricsPlugin;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Asserts that the loader {@link MetricsConfig} builds for metrics plugins is
 * still built over the locations the configuration names.
 * <p>
 * That loader used to be built from inside a privileged block, which the
 * runtime no longer treats as anything other than a plain call. The block is
 * gone and the loader is built directly, and nothing about that is visible from
 * the outside unless something is actually loaded through it: a loader built
 * over no locations at all would still answer every request by asking the
 * ordinary classpath, so every plugin already on the classpath would go on
 * being found and only a plugin supplied from elsewhere would quietly stop
 * being.
 * <p>
 * A loader asks the classpath before it looks anywhere of its own, so finding
 * something the classpath already has would say nothing. Each of these tests
 * therefore puts a file in a directory of its own, names that directory in the
 * configuration, and first asserts that the ordinary classpath cannot find the
 * file at all. Finding it through the loader is then only possible by way of
 * the configured location, which is exactly what has to be shown. The
 * locations the loader was built over, the classpath it falls back on and the
 * building of it only once are asserted alongside.
 */
public class TestMetricsConfigPluginLoader {

  /** The file placed where a plugin would be, and looked for through it. */
  private static final String MARKER = "metrics-plugin-marker.properties";

  /** A second such file, for the case of more than one location. */
  private static final String SECOND_MARKER =
      "metrics-plugin-second-marker.properties";

  /**
   * Returns a configuration naming the given locations for plugins.
   *
   * @param pluginUrls the locations, separated by commas, or {@code null} to
   *                   name none at all
   * @return a configuration to ask for a plugin loader
   */
  private static MetricsConfig configNaming(String pluginUrls) {
    PropertiesConfiguration properties = new PropertiesConfiguration();
    if (pluginUrls != null) {
      properties.addProperty("*." + MetricsConfig.PLUGIN_URLS_KEY, pluginUrls);
    }
    return new MetricsConfig(properties, "*");
  }

  /**
   * Puts a file with the given name in the given directory.
   *
   * @param directory where to put the file
   * @param name what to call it
   * @return the location of the directory, as a loader would be given it
   * @throws Exception if the file cannot be written
   */
  private static String placeMarkerIn(Path directory, String name)
      throws Exception {
    Files.write(directory.resolve(name),
        ("placed.by=" + TestMetricsConfigPluginLoader.class.getName() + "\n")
            .getBytes(StandardCharsets.UTF_8));
    return directory.toUri().toString();
  }

  /**
   * Naming no location for plugins leaves the ordinary classpath in place.
   * <p>
   * A configuration that says nothing about where plugins come from has to be
   * answered with the classpath this class was itself loaded from, and not with
   * a loader over nothing.
   */
  @Test
  public void testNamingNoLocationLeavesTheClasspathInPlace() {
    MetricsConfig config = configNaming(null);

    ClassLoader loader = config.getPluginLoader();

    assertSame(MetricsConfig.class.getClassLoader(), loader,
        "a configuration naming no location for plugins was answered with "
            + "something other than the ordinary classpath");
  }

  /**
   * A named location is searched for plugins, and searched through a loader
   * built only once.
   *
   * @param pluginDirectory a directory of this test's own, standing in for one
   *                        a plugin would be supplied from
   * @throws Exception if the directory cannot be written to or read back
   */
  @Test
  public void testNamedLocationIsSearchedForPlugins(@TempDir Path pluginDirectory)
      throws Exception {
    String location = placeMarkerIn(pluginDirectory, MARKER);
    assertNull(MetricsConfig.class.getClassLoader().getResource(MARKER),
        "the file this test looks for is on the ordinary classpath, so "
            + "finding it would say nothing about the named location");
    MetricsConfig config = configNaming(location);

    ClassLoader loader = config.getPluginLoader();

    assertTrue(loader instanceof URLClassLoader,
        "a configuration naming a location for plugins was not answered with "
            + "a loader that searches locations");
    assertArrayEquals(new URL[] {URI.create(location).toURL()},
        ((URLClassLoader) loader).getURLs(),
        "the loader was not built over the location the configuration named");
    assertSame(MetricsConfig.class.getClassLoader(), loader.getParent(),
        "the loader does not fall back on the ordinary classpath");
    assertNotNull(loader.getResource(MARKER),
        "the loader did not find what is in the location the configuration "
            + "named");
    assertSame(MetricsPlugin.class,
        loader.loadClass(MetricsPlugin.class.getName()),
        "the loader could not reach the plugin interface itself");
    assertSame(loader, config.getPluginLoader(),
        "a second request built a second loader instead of reusing the first");
  }

  /**
   * Every named location is searched, in the order it was named.
   *
   * @param firstDirectory the first location a plugin would be supplied from
   * @param secondDirectory the second such location
   * @throws Exception if a directory cannot be written to or read back
   */
  @Test
  public void testEveryNamedLocationIsSearched(@TempDir Path firstDirectory,
      @TempDir Path secondDirectory) throws Exception {
    String first = placeMarkerIn(firstDirectory, MARKER);
    String second = placeMarkerIn(secondDirectory, SECOND_MARKER);
    MetricsConfig config = configNaming(first + "," + second);

    ClassLoader loader = config.getPluginLoader();

    assertArrayEquals(
        new URL[] {URI.create(first).toURL(), URI.create(second).toURL()},
        ((URLClassLoader) loader).getURLs(),
        "the loader was not built over both named locations in the order they "
            + "were named");
    assertNotNull(loader.getResource(MARKER),
        "the loader did not find what is in the first named location");
    assertNotNull(loader.getResource(SECOND_MARKER),
        "the loader did not find what is in the second named location");
  }

  /**
   * A location that is not a location at all is reported rather than ignored.
   * <p>
   * Carrying on with the ordinary classpath instead would leave a plugin that
   * was configured and expected simply absent, with nothing said about why.
   */
  @Test
  public void testLocationThatIsNotALocationIsReported() {
    MetricsConfig config = configNaming("not a location at all");

    assertThrows(MetricsConfigException.class, config::getPluginLoader,
        "a location that is not a location at all was accepted");
  }
}
