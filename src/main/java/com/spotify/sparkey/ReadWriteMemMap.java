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

import com.google.common.base.Throwables;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

final class ReadWriteMemMap implements ReadWriteData {
  private static final long CHUNK_SIZE = 1 << 30;
  private static final long BITMASK_30 = ((1L << 30) - 1);
  private final File file;

  private volatile MappedByteBuffer[] chunks;
  private final RandomAccessFile randomAccessFile;
  private final long size;
  private final int numChunks;

  private int curChunkIndex;
  private volatile MappedByteBuffer curChunk;

  ReadWriteMemMap(File file) throws IOException {
    this.file = file;

    this.randomAccessFile = new RandomAccessFile(file, "rw");
    try {
      this.size = file.length();
      if (size <= 0) {
        throw new IllegalArgumentException("Non-positive size: " + size);
      }
      long numFullMaps = (size - 1) >> 30;
      if (numFullMaps >= Integer.MAX_VALUE) {
        throw new IllegalArgumentException("Too large size: " + size);
      }
      long sizeFullMaps = numFullMaps * CHUNK_SIZE;

      numChunks = (int) (numFullMaps + 1);
      chunks = new MappedByteBuffer[numChunks];
      long offset = 0;
      for (int i = 0; i < numFullMaps; i++) {
        chunks[i] = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_ONLY, offset, CHUNK_SIZE);
        offset += CHUNK_SIZE;
      }
      long lastSize = size - sizeFullMaps;
      if (lastSize > 0) {
        chunks[numChunks - 1] = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_ONLY, offset, lastSize);
      }

      curChunkIndex = 0;
      curChunk = chunks[0];
      curChunk.position(0);
    } catch (Exception e) {
      this.randomAccessFile.close();
      Throwables.propagateIfPossible(e, IOException.class);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void close() throws IOException {
    curChunk = null;
    randomAccessFile.close();
  }

  @Override
  public void seek(long pos) throws IOException {
    if (pos > size) {
      throw corruptionException();
    }
    int partIndex = (int) (pos >>> 30);
    curChunkIndex = partIndex;
    MappedByteBuffer[] chunks = getChunks();
    MappedByteBuffer curChunk = chunks[partIndex];
    curChunk.position((int) (pos & BITMASK_30));
    this.curChunk = curChunk;
  }

  @Override
  public void writeUnsignedByte(int value) throws IOException {
    MappedByteBuffer curChunk = getCurChunk();
    if (curChunk.remaining() == 0) {
      next();
      curChunk = getCurChunk();
    }
    curChunk.put((byte) value);
  }

  @Override
  public void flush(IndexHeader header, boolean fsync) throws IOException {
    ByteBuffer byteBuffer = header.writeToByteBuffer();
    randomAccessFile.seek(0);
    randomAccessFile.write(byteBuffer.array());
    if (fsync) {
      randomAccessFile.getFD().sync();
    }
  }

  private void next() throws IOException {
    MappedByteBuffer[] chunks = getChunks();
    curChunkIndex++;
    if (curChunkIndex >= chunks.length) {
      throw corruptionException();
    }
    MappedByteBuffer curChunk = chunks[curChunkIndex];
    if (curChunk == null) {
      throw new RuntimeException("chunk == null");
    }
    curChunk.position(0);
    this.curChunk = curChunk;
  }

  @Override
  public int readUnsignedByte() throws IOException {
    MappedByteBuffer curChunk = getCurChunk();
    if (curChunk.remaining() == 0) {
      next();
      curChunk = getCurChunk();
    }
    return ((int) curChunk.get()) & 0xFF;
  }

  private MappedByteBuffer[] getChunks() throws IOException {
    MappedByteBuffer[] localChunks = chunks;
    if (localChunks == null) {
      throw closedException();
    }
    return localChunks;
  }

  private IOException closedException() {
    return new IOException("File is closed");
  }

  private MappedByteBuffer getCurChunk() throws IOException {
    MappedByteBuffer curChunk = this.curChunk;
    if (curChunk == null) {
      throw closedException();
    }
    return curChunk;
  }

  private IOException corruptionException() {
    return new CorruptedIndexException("Index is likely corrupt (" + file.getPath() + "), referencing data outside of range");
  }

  public ReadWriteMemMap duplicate() {
    throw new RuntimeException("Unsupported operation");
  }

  @Override
  public String toString() {
    return "ReadWriteMemMap{" +
            ", randomAccessFile=" + randomAccessFile +
            ", size=" + size +
            '}';
  }

}
