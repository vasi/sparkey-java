package com.spotify.sparkey;

import java.io.IOException;

public interface ReadWriteData extends ReadableData {
  void close() throws IOException;

  void seek(long pos) throws IOException;

  void writeUnsignedByte(int value) throws IOException;

  void flush(IndexHeader header, boolean fsync) throws IOException;
}
