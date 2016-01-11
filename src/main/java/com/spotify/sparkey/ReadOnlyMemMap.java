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

import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

final class ReadOnlyMemMap extends LargeByteBuffer {
  private static final ScheduledExecutorService CLEANER = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
    @Override
    public Thread newThread(Runnable r) {
      Thread thread = new Thread(r);
      thread.setName(ReadOnlyMemMap.class.getSimpleName() + "-cleaner");
      thread.setDaemon(true);
      return thread;
    }
  });

  private final File file;

  ReadOnlyMemMap(File file) throws IOException {
    super(getChunks(file), file.length());
    this.file = file;
  }

  private static UnsafeBuffer[] getChunks(File file) throws IOException {
    final RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
    try {
      final long size = file.length();
      if (size <= 0) {
        throw new IllegalArgumentException("Non-positive size: " + size);
      }
      final long numFullMaps = (size - 1) >> 30;
      if (numFullMaps >= Integer.MAX_VALUE) {
        throw new IllegalArgumentException("Too large size: " + size);
      }
      final long sizeFullMaps = numFullMaps * MAP_SIZE;

      final int numChunks = (int) (numFullMaps + 1);
      final UnsafeBuffer[] chunks = new UnsafeBuffer[numChunks];
      long offset = 0;
      for (int i = 0; i < numFullMaps; i++) {
        final MappedByteBuffer map = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_ONLY, offset, MAP_SIZE);
        map.order(ByteOrder.LITTLE_ENDIAN);
        chunks[i] = new UnsafeBuffer(map);
        offset += MAP_SIZE;
      }
      long lastSize = size - sizeFullMaps;
      if (lastSize > 0) {
        final MappedByteBuffer map = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_ONLY, offset, lastSize);
        map.order(ByteOrder.LITTLE_ENDIAN);
        chunks[numChunks - 1] = new UnsafeBuffer(map);
      }
      return chunks;
    } catch (Exception e) {
      Throwables.propagateIfPossible(e, IOException.class);
      throw Throwables.propagate(e);
    } finally {
      Util.nonThrowingClose(randomAccessFile);
    }
  }

  public void close() {
    final ByteBuffer[] buffers = new ByteBuffer[chunks.length];
    for (int i = 0; i < chunks.length; i++) {
      UnsafeBuffer chunk = chunks[i];
      if (chunk != null) {
        buffers[i] = chunk.byteBuffer();
      }
      chunks[i] = null;
    }

    // Wait a bit with closing so that all threads have a chance to see the that
    // chunks and curChunks are null
    CLEANER.schedule(new Runnable() {
      @Override
      public void run() {
        for (int i = 0; i < chunks.length; i++) {
          ByteBuffer buffer = buffers[i];
          if (buffer != null) {
            ByteBufferCleaner.cleanMapping(buffer);
          }
        }
      }
    }, 1000, TimeUnit.MILLISECONDS);
  }

  @Override
  public String toString() {
    return "ReadOnlyMemMap{" +
            ", file=" + file +
            '}';
  }

}
