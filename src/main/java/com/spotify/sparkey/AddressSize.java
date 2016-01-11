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

import java.io.IOException;

enum AddressSize {
  LONG(8) {
    @Override
    long readAddress(RandomAccessData data, long offset) throws IOException {
      return Util.readLittleEndianLong(data, offset);
    }

    @Override
    void writeAddress(long address, InMemoryData data, long offset) throws IOException {
      Util.writeLittleEndianLong(address, data, offset);
    }
  },
  INT(4) {
    @Override
    long readAddress(RandomAccessData data, long offset) throws IOException {
      return Util.readLittleEndianInt(data, offset) & INT_MASK;
    }

    @Override
    void writeAddress(long address, InMemoryData data, long offset) throws IOException {
      Util.writeLittleEndianInt((int) address, data, offset);
    }
  };

  private static final long INT_MASK = (1L << 32) - 1;

  private final int size;

  AddressSize(int size) {
    this.size = size;
  }

  abstract long readAddress(RandomAccessData data, long offset) throws IOException;

  abstract void writeAddress(long address, InMemoryData data, long offset) throws IOException;

  public int size() {
    return size;
  }
}
