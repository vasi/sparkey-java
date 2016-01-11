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

import java.io.IOException;

class LargeByteBuffer implements RandomAccessData {
  static final long MAP_SIZE = 1 << 30;
  static final long BITMASK_30 = ((1L << 30) - 1);

  protected final UnsafeBuffer[] chunks;
  protected final long size;

  protected LargeByteBuffer(final UnsafeBuffer[] chunks, long size) throws IOException {
    this.chunks = chunks;
    this.size = size;
  }

  @Override
  public long readLong(long offset) throws IOException {
    validateRange(offset);
    final int chunkIndex = (int) (offset >>> 30);
    final int chunkOffset = (int) (offset & BITMASK_30);
    final UnsafeBuffer chunk = chunks[chunkIndex];
    validateOpen(chunk);
    if (chunkOffset < MAP_SIZE - 8) {
      return chunk.getLong(chunkOffset);
    } else {
      // TODO: overflow bits and stuff?
      return ((long) readInt(offset)) << 30 | (long) readInt(offset + 4);
    }
  }

  @Override
  public int readInt(long offset) throws IOException {
    validateRange(offset);
    final int chunkIndex = (int) (offset >>> 30);
    final int chunkOffset = (int) (offset & BITMASK_30);
    final UnsafeBuffer chunk = chunks[chunkIndex];
    if (chunk == null) {
      throw corruptionException(offset);
    }
    if (chunkOffset < MAP_SIZE - 4) {
      return chunk.getInt(chunkOffset);
    } else {
      // Shouldn't be reachable since all hash tables have values aligned by 4 bytes.
      throw corruptionException(offset);
    }
  }

  public int readUnsignedByte(long offset) throws IOException {
    validateRange(offset);
    final int chunkIndex = (int) (offset >>> 30);
    final int chunkOffset = (int) (offset & BITMASK_30);
    final UnsafeBuffer chunk = chunks[chunkIndex];
    validateOpen(chunk);
    return ((int) chunk.getByte(chunkOffset)) & 0xFF;
  }

  private void validateRange(long offset) throws IOException {
    if (offset < 0 || offset >= size) {
      throw corruptionException(offset);
    }
  }

  public void writeInt(long offset, int value) throws IOException {
    validateRange(offset);
    final int chunkIndex = (int) (offset >>> 30);
    final int chunkOffset = (int) (offset & BITMASK_30);
    final UnsafeBuffer chunk = chunks[chunkIndex];
    validateOpen(chunk);
    if (chunkOffset < MAP_SIZE - 4) {
      chunk.putInt(chunkOffset, value);
    } else {
      // Shouldn't be reachable since all hash tables have values aligned by 4 bytes.
      throw corruptionException(offset);
    }
  }

  private void validateOpen(UnsafeBuffer chunk) throws SparkeyReaderClosedException {
    if (chunk == null) {
      throw closedException();
    }
  }

  public void writeLong(long offset, long value) throws IOException {
    validateRange(offset);
    final int chunkIndex = (int) (offset >>> 30);
    final int chunkOffset = (int) (offset & BITMASK_30);
    final UnsafeBuffer chunk = chunks[chunkIndex];
    validateOpen(chunk);
    if (chunkOffset < MAP_SIZE - 4) {
      chunk.putLong(chunkOffset, value);
    } else {
      // TODO: overflow bits and stuff?
      writeInt(offset, (int) (value >>> 30));
      writeInt(offset, (int) (value & BITMASK_30));
    }
  }

  public void readFully(long offset, byte[] buffer, int offset2, int length) throws IOException {
    validateRange(offset);
    while (length > 0) {
      final int chunkIndex = (int) (offset >>> 30);
      final int chunkOffset = (int) (offset & BITMASK_30);
      final UnsafeBuffer chunk = chunks[chunkIndex];
      if (chunk == null) {
        throw corruptionException(offset);
      }
      long available = size - offset;
      int toRead = (int) Math.min(available, Math.min(length, MAP_SIZE - chunkOffset));

      chunk.getBytes(chunkOffset, buffer, offset2, toRead);
      offset += toRead;
      offset2 += toRead;
      length -= toRead;
    }
  }

  private IOException corruptionException(long offset) {
    return new CorruptedIndexException("Index is likely corrupt, referencing data outside of range: trying to read from offset " + offset + " but size is " + size);
  }

  private SparkeyReaderClosedException closedException() {
    return new SparkeyReaderClosedException("Reader has been closed");
  }
}
