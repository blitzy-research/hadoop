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

package org.apache.hadoop.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

/**
 * Asserts that {@link FastByteComparisons} still reaches the faster of its two
 * ways of comparing bytes, and that both of them order bytes correctly.
 * <p>
 * The faster way needs a handle on an internal class of the running runtime,
 * which used to be obtained from inside a privileged block. The runtime no
 * longer treats such a block as anything other than a plain call, so the block
 * is gone and the handle is obtained directly. Whether that still works is
 * invisible from the outside: obtaining the handle happens while a class is
 * being initialized and any failure at all is deliberately swallowed, leaving
 * the slower way in its place. Every comparison would go on giving the same
 * answers, only more slowly, and every existing test would go on passing. This
 * therefore asserts which of the two was chosen, and asserts it directly.
 * <p>
 * Both ways are then asserted to order bytes as the runtime's own comparison of
 * unsigned byte ranges does, over cases chosen to cross the boundaries the
 * faster one works in: a difference inside the first block it reads eight bytes
 * at a time, one exactly at the end of such a block, one in the bytes left over
 * after the last whole block, a prefix of another range, an empty range, and
 * bytes above 127, which a comparison that treated them as signed would order
 * the wrong way round. Ordering byte ranges wrongly would silently reorder
 * every sorted key Hadoop writes.
 */
public class TestFastByteComparisons {

  /** The holder that decides, once, which way of comparing is used. */
  private static final String HOLDER =
      "org.apache.hadoop.io.FastByteComparisons$LexicographicalComparerHolder";

  /** The faster way, which needs a handle on an internal runtime class. */
  private static final String UNSAFE_COMPARER = HOLDER + "$UnsafeComparer";

  /** The slower way, which needs nothing of the runtime. */
  private static final String PURE_JAVA_COMPARER = HOLDER + "$PureJavaComparer";

  /**
   * Returns the way of comparing that was chosen when the holder was
   * initialized.
   *
   * @return the chosen comparer
   * @throws Exception if the holder cannot be read
   */
  private static Object chosenComparer() throws Exception {
    Field best = Class.forName(HOLDER).getDeclaredField("BEST_COMPARER");
    best.setAccessible(true);
    return best.get(null);
  }

  /**
   * Returns the single instance of one of the two ways of comparing.
   *
   * @param className the name of the way to return
   * @return its only instance
   * @throws Exception if that way cannot be reached
   */
  private static Object comparer(String className) throws Exception {
    return Class.forName(className).getEnumConstants()[0];
  }

  /**
   * Compares two ranges of bytes with the given way of comparing.
   *
   * @param comparer the way of comparing to use
   * @param buffer1 the bytes on the left
   * @param offset1 where the range on the left starts
   * @param length1 how many bytes to compare on the left
   * @param buffer2 the bytes on the right
   * @param offset2 where the range on the right starts
   * @param length2 how many bytes to compare on the right
   * @return negative, zero or positive as the left range sorts before, with or
   *         after the right one
   * @throws Exception if the comparison cannot be reached
   */
  private static int compareWith(Object comparer, byte[] buffer1, int offset1,
      int length1, byte[] buffer2, int offset2, int length2) throws Exception {
    Method compareTo = comparer.getClass().getMethod("compareTo", byte[].class,
        int.class, int.class, byte[].class, int.class, int.class);
    compareTo.setAccessible(true);
    return (Integer) compareTo.invoke(comparer, buffer1, offset1, length1,
        buffer2, offset2, length2);
  }

  /**
   * Asserts that every way of comparing orders the two ranges as the runtime's
   * own comparison of unsigned byte ranges does.
   * <p>
   * Only the direction matters, never the magnitude, because the two ways
   * legitimately return different amounts for the same ordering.
   *
   * @param buffer1 the bytes on the left
   * @param offset1 where the range on the left starts
   * @param length1 how many bytes to compare on the left
   * @param buffer2 the bytes on the right
   * @param offset2 where the range on the right starts
   * @param length2 how many bytes to compare on the right
   * @throws Exception if a comparison cannot be reached
   */
  private static void assertOrderedAsTheRuntimeDoes(byte[] buffer1, int offset1,
      int length1, byte[] buffer2, int offset2, int length2) throws Exception {
    int expected = Integer.signum(Arrays.compareUnsigned(buffer1, offset1,
        offset1 + length1, buffer2, offset2, offset2 + length2));
    String where = "comparing " + length1 + " bytes from " + offset1 + " with "
        + length2 + " bytes from " + offset2;

    assertEquals(expected,
        Integer.signum(FastByteComparisons.compareTo(buffer1, offset1, length1,
            buffer2, offset2, length2)),
        "the comparison callers use ordered bytes differently from the "
            + "runtime when " + where);
    assertEquals(expected,
        Integer.signum(compareWith(comparer(PURE_JAVA_COMPARER), buffer1,
            offset1, length1, buffer2, offset2, length2)),
        "the way of comparing that needs nothing of the runtime ordered bytes "
            + "differently from the runtime when " + where);
    if (UNSAFE_COMPARER.equals(chosenComparer().getClass().getName())) {
      assertEquals(expected,
          Integer.signum(compareWith(comparer(UNSAFE_COMPARER), buffer1,
              offset1, length1, buffer2, offset2, length2)),
          "the faster way of comparing ordered bytes differently from the "
              + "runtime when " + where);
    }
  }

  /**
   * Returns bytes counting up from one, so that every position differs from
   * every other.
   *
   * @param length how many bytes to return
   * @return that many distinct bytes
   */
  private static byte[] countingUp(int length) {
    byte[] bytes = new byte[length];
    for (int i = 0; i < length; i++) {
      bytes[i] = (byte) (i + 1);
    }
    return bytes;
  }

  /**
   * The faster way of comparing is the one chosen, and the handle it needs was
   * obtained.
   * <p>
   * A runtime whose bytes have to be read one at a time is deliberately given
   * the slower way instead, so that architecture decides which of the two is
   * expected here. On every other architecture the faster way has to be the one
   * chosen, and the handle it holds has to be a real one: were obtaining it to
   * fail, the failure would be swallowed and the slower way would take its
   * place with nothing else to show for it.
   *
   * @throws Exception if the holder or the handle cannot be read
   */
  @Test
  public void testTheFasterWayOfComparingIsTheOneChosen() throws Exception {
    String chosen = chosenComparer().getClass().getName();

    if (System.getProperty("os.arch").toLowerCase().startsWith("sparc")) {
      assertEquals(PURE_JAVA_COMPARER, chosen,
          "an architecture whose bytes are read one at a time was not given "
              + "the way of comparing that needs nothing of the runtime");
      return;
    }

    assertEquals(UNSAFE_COMPARER, chosen,
        "the faster way of comparing was not chosen, so the handle it needs "
            + "was not obtained");

    Class<?> faster = Class.forName(UNSAFE_COMPARER);
    Field handle = faster.getDeclaredField("theUnsafe");
    handle.setAccessible(true);
    assertNotNull(handle.get(null),
        "the faster way of comparing was chosen while holding no handle at "
            + "all");
    Field baseOffset = faster.getDeclaredField("BYTE_ARRAY_BASE_OFFSET");
    baseOffset.setAccessible(true);
    assertTrue((Integer) baseOffset.get(null) > 0,
        "the faster way of comparing did not learn where the bytes of an "
            + "array begin");
  }

  /**
   * Bytes above 127 sort after bytes below it.
   * <p>
   * A comparison that treated bytes as signed would order these the other way
   * round, which would reorder sorted keys rather than fail outright, so it is
   * asserted on its own as well as within the wider set of cases.
   *
   * @throws Exception if a comparison cannot be reached
   */
  @Test
  public void testBytesAboveOneHundredAndTwentySevenSortLast()
      throws Exception {
    byte[] high = {(byte) 0x80};
    byte[] justBelow = {(byte) 0x7f};
    byte[] highest = {(byte) 0xff};
    byte[] lowest = {(byte) 0x00};

    assertTrue(
        FastByteComparisons.compareTo(high, 0, 1, justBelow, 0, 1) > 0,
        "a byte above 127 did not sort after one below it");
    assertTrue(
        FastByteComparisons.compareTo(highest, 0, 1, lowest, 0, 1) > 0,
        "the highest byte did not sort after the lowest");
    assertOrderedAsTheRuntimeDoes(high, 0, 1, justBelow, 0, 1);
    assertOrderedAsTheRuntimeDoes(highest, 0, 1, lowest, 0, 1);
  }

  /**
   * Equal ranges compare equal, whether or not they are the same range of the
   * same array.
   * <p>
   * The same range of the same array is answered without comparing anything at
   * all, which is a path of its own and is therefore asserted alongside the
   * ordinary case of two arrays holding the same bytes.
   *
   * @throws Exception if a comparison cannot be reached
   */
  @Test
  public void testEqualRangesCompareEqual() throws Exception {
    byte[] bytes = countingUp(20);
    byte[] sameBytes = countingUp(20);

    assertEquals(0, FastByteComparisons.compareTo(bytes, 0, 20, bytes, 0, 20),
        "the same range of the same array did not compare equal to itself");
    assertEquals(0, FastByteComparisons.compareTo(bytes, 3, 9, bytes, 3, 9),
        "the same range of the same array did not compare equal to itself");
    assertEquals(0,
        FastByteComparisons.compareTo(bytes, 0, 20, sameBytes, 0, 20),
        "two arrays holding the same bytes did not compare equal");
    assertOrderedAsTheRuntimeDoes(bytes, 0, 20, bytes, 0, 20);
    assertOrderedAsTheRuntimeDoes(bytes, 3, 9, bytes, 3, 9);
    assertOrderedAsTheRuntimeDoes(bytes, 0, 20, sameBytes, 0, 20);
    assertOrderedAsTheRuntimeDoes(bytes, 4, 8, sameBytes, 4, 8);
  }

  /**
   * Every way of comparing orders bytes as the runtime does, across the
   * boundaries the faster way works in.
   * <p>
   * The faster way reads eight bytes at a time and then finishes what is left
   * over one byte at a time, so a difference is placed inside the first such
   * block, at its last byte, at the first byte of the next, and among the bytes
   * left over. Ranges at a non-zero offset, ranges of unequal length, a range
   * that is a prefix of another and an empty range are covered as well, since
   * each is answered by a different part of the comparison.
   *
   * @throws Exception if a comparison cannot be reached
   */
  @Test
  public void testOrderingMatchesTheRuntimeAcrossEveryBoundary()
      throws Exception {
    byte[] twenty = countingUp(20);

    // A difference inside the first block read eight bytes at a time, at its
    // last byte, at the first byte of the next block, and among the bytes left
    // over after the last whole block.
    for (int at : new int[] {0, 3, 7, 8, 9, 15, 16, 17, 19}) {
      byte[] differing = countingUp(20);
      differing[at] = (byte) 0xfe;
      assertOrderedAsTheRuntimeDoes(twenty, 0, 20, differing, 0, 20);
      assertOrderedAsTheRuntimeDoes(differing, 0, 20, twenty, 0, 20);
    }

    // One range a prefix of the other, of every length either side of a whole
    // block.
    for (int shorter : new int[] {0, 1, 7, 8, 9, 19}) {
      assertOrderedAsTheRuntimeDoes(twenty, 0, shorter, twenty, 0, 20);
      assertOrderedAsTheRuntimeDoes(twenty, 0, 20, twenty, 0, shorter);
    }

    // Ranges that do not start where their array does, including one that ends
    // where its array does.
    assertOrderedAsTheRuntimeDoes(twenty, 1, 19, twenty, 0, 19);
    assertOrderedAsTheRuntimeDoes(twenty, 5, 10, twenty, 5, 10);
    assertOrderedAsTheRuntimeDoes(twenty, 5, 10, twenty, 6, 10);
    assertOrderedAsTheRuntimeDoes(twenty, 12, 8, twenty, 4, 8);

    // Empty ranges, against each other and against a range holding bytes.
    byte[] empty = new byte[0];
    assertOrderedAsTheRuntimeDoes(empty, 0, 0, empty, 0, 0);
    assertOrderedAsTheRuntimeDoes(empty, 0, 0, twenty, 0, 20);
    assertOrderedAsTheRuntimeDoes(twenty, 0, 20, empty, 0, 0);
    assertOrderedAsTheRuntimeDoes(twenty, 20, 0, twenty, 0, 1);
  }
}
