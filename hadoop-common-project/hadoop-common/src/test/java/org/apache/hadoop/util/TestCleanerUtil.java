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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.util.CleanerUtil.BufferCleaner;
import org.junit.jupiter.api.Test;

/**
 * Asserts that {@link CleanerUtil} can still release the memory behind a buffer
 * the runtime allocated outside the heap.
 * <p>
 * Reaching the runtime's own way of doing that used to happen from inside a
 * privileged block, and releasing the memory used to happen from inside a
 * second one. The runtime no longer treats such a block as anything other than
 * a plain call, so both are gone and both are done directly. Whether the first
 * still works is invisible from the outside, because any failure while reaching
 * the runtime's way is deliberately turned into a recorded reason and a
 * decision not to support releasing at all; callers then simply keep the memory
 * of every file they map until the collector happens to release it, which on a
 * long-running service shows up as exhaustion far from its cause rather than as
 * a failure. Whether the second still works is invisible in a different way: it
 * exists only to turn a failure while releasing into a reported one, so
 * removing it would lose the report and not the release.
 * <p>
 * This therefore asserts that releasing is supported on the runtime under test,
 * that the three things published about it agree with each other and that the
 * one of them that does the releasing actually releases, that releasing a
 * buffer allocated outside the heap succeeds, and that both ways of failing to
 * release one are reported: a buffer that is not outside the heap is refused
 * outright, and a buffer the runtime itself will not release is reported as a
 * failure to release rather than escaping as whatever the runtime threw.
 * <p>
 * Every one of those readings is taken through what {@link CleanerUtil}
 * publishes. All three published items are derived, once, from the very
 * decision the first removed privileged block used to wrap, and every test here
 * triggers that derivation, so observing the outcome of that decision needs no
 * reflective reach into the decision itself.
 */
public class TestCleanerUtil {

  /**
   * What is published about releasing memory hangs together, and the part of it
   * that does the releasing actually releases.
   * <p>
   * All three published items come from one decision, reached during class
   * initialization by the very code the first removed privileged block used to
   * wrap, and any test in this class triggers that initialization. Reading them
   * afterwards therefore observes that decision's outcome, and reading them
   * against each other is what catches an outcome no caller could act on:
   * either releasing is supported, in which case there is a way of doing it, it
   * works on a buffer allocated outside the heap, and no reason is recorded
   * against it, or it is not, in which case there is no way of doing it and a
   * reason says why. Any other combination would leave a caller reading one
   * thing and finding another.
   *
   * @throws IOException if a buffer that should be released is not
   */
  @Test
  public void testWhatIsPublishedAboutReleasingHangsTogether()
      throws IOException {
    if (CleanerUtil.UNMAP_SUPPORTED) {
      BufferCleaner published = CleanerUtil.getCleaner();
      assertNotNull(published,
          "releasing is published as supported with no way of doing it");
      assertNull(CleanerUtil.UNMAP_NOT_SUPPORTED_REASON,
          "releasing is published as supported with a reason recorded against "
              + "it");

      published.freeBuffer(ByteBuffer.allocateDirect(4096));
    } else {
      assertNull(CleanerUtil.getCleaner(),
          "releasing is published as unsupported with a way of doing it");
      String reason = CleanerUtil.UNMAP_NOT_SUPPORTED_REASON;
      assertNotNull(reason,
          "releasing is published as unsupported without the reason it is");
      assertFalse(reason.isEmpty(),
          "releasing is published as unsupported with an empty reason");
    }
  }

  /**
   * Releasing memory outside the heap is supported on the runtime under test.
   * <p>
   * Every runtime this project is built and tested with offers a way of doing
   * it, so the decision has to find one. Were reaching it to fail, the failure
   * would be swallowed into the recorded reason and nothing else would say so,
   * which is why the reason is quoted here if this ever does fail.
   */
  @Test
  public void testReleasingIsSupportedOnThisRuntime() {
    assertTrue(CleanerUtil.UNMAP_SUPPORTED,
        "releasing memory outside the heap is unsupported on this runtime, "
            + "because: " + CleanerUtil.UNMAP_NOT_SUPPORTED_REASON);
    assertNotNull(CleanerUtil.getCleaner(),
        "releasing is supported with no way of doing it");
  }

  /**
   * A buffer allocated outside the heap is released.
   * <p>
   * The buffer is never touched again afterwards, since the memory behind it is
   * gone.
   *
   * @throws IOException if the buffer cannot be released
   */
  @Test
  public void testBufferOutsideTheHeapIsReleased() throws IOException {
    ByteBuffer outsideTheHeap = ByteBuffer.allocateDirect(4096);
    outsideTheHeap.put(0, (byte) 7);
    assertTrue(outsideTheHeap.isDirect(),
        "the buffer this test allocated is not outside the heap");

    CleanerUtil.getCleaner().freeBuffer(outsideTheHeap);
  }

  /**
   * A buffer that is not outside the heap is refused.
   * <p>
   * There is no memory of its own behind such a buffer, so releasing it would
   * mean releasing something else; refusing it is what keeps that from
   * happening.
   */
  @Test
  public void testBufferInsideTheHeapIsRefused() {
    ByteBuffer insideTheHeap = ByteBuffer.allocate(4096);

    IllegalArgumentException refused =
        assertThrows(IllegalArgumentException.class,
            () -> CleanerUtil.getCleaner().freeBuffer(insideTheHeap),
            "a buffer with no memory of its own was accepted for releasing");
    assertTrue(refused.getMessage().contains("direct buffers"),
        "a buffer inside the heap was refused for some other reason: "
            + refused.getMessage());
  }

  /**
   * A buffer the runtime will not release is reported as a failure to release.
   * <p>
   * A buffer that shares the memory of another one is outside the heap and is
   * of the right kind, so it gets as far as the runtime, which then refuses it
   * because the memory is not its to release. That refusal has to arrive as a
   * failure to release, carrying what the runtime said as its cause: this is
   * the path that exists solely to report such a failure, and it would be
   * silently lost if what the runtime threw were left to escape on its own
   * terms.
   */
  @Test
  public void testBufferTheRuntimeWillNotReleaseIsReported() {
    ByteBuffer outsideTheHeap = ByteBuffer.allocateDirect(4096);
    ByteBuffer sharingItsMemory = outsideTheHeap.duplicate();
    assertTrue(sharingItsMemory.isDirect(),
        "a buffer sharing the memory of one outside the heap is not itself "
            + "outside it");

    IOException reported = assertThrows(IOException.class,
        () -> CleanerUtil.getCleaner().freeBuffer(sharingItsMemory),
        "a buffer the runtime will not release was reported as released");
    assertTrue(reported.getMessage().contains("Unable to unmap"),
        "the failure was reported as something other than a failure to "
            + "release: " + reported.getMessage());
    assertNotNull(reported.getCause(),
        "the failure was reported without what the runtime said about it");
  }
}
