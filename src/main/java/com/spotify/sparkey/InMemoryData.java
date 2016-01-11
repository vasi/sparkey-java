/*
 * Copyright (c) 2011-2013 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.spotify.sparkey;

import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.io.FileOutputStream;
import java.io.IOException;

final class InMemoryData extends LargeByteBuffer {

  private final long size;

  InMemoryData(long size) throws IOException {
    super(getChunks(size), size);
    this.size = size;
  }

  private static UnsafeBuffer[] getChunks(long size) {
    if (size < 0) {
      throw new IllegalArgumentException("Negative size: " + size);
    }
    final long numFullMaps = (size - 1) >> 30;
    if (numFullMaps >= Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Too large size: " + size);
    }
    long sizeFullMaps = numFullMaps * MAP_SIZE;

    final int numChunks = (int) (numFullMaps + 1);
    final UnsafeBuffer[] chunks = new UnsafeBuffer[numChunks];
    for (int i = 0; i < numFullMaps; i++) {
      chunks[i] = new UnsafeBuffer(new byte[(int) MAP_SIZE]);
    }
    final long lastSize = size - sizeFullMaps;
    if (lastSize > 0) {
      chunks[numChunks - 1] = new UnsafeBuffer(new byte[(int) lastSize]);
    }
    return chunks;
  }

  void close() {
    for (int i = 0; i < chunks.length; i++) {
      chunks[i] = null;
    }
  }

  @Override
  public String toString() {
    return "InMemoryData{" +
            "size=" + size +
            '}';
  }

  public void flushToFile(FileOutputStream stream) throws IOException {
    for (UnsafeBuffer chunk : chunks) {
      stream.write(chunk.byteArray());
    }
  }
}
