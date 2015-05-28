package com.spotify.sparkey;

import com.fasterxml.sort.DataReader;
import com.fasterxml.sort.DataReaderFactory;
import com.fasterxml.sort.DataWriter;
import com.fasterxml.sort.DataWriterFactory;
import com.fasterxml.sort.SortConfig;
import com.fasterxml.sort.Sorter;
import com.google.common.primitives.UnsignedLong;
import com.google.common.primitives.UnsignedLongs;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Comparator;

class HashSorter {
  private final SortConfig config = new SortConfig();
  private final MyDataReaderFactory myDataReaderFactory = new MyDataReaderFactory();
  private final MyDataWriterFactory myDataWriterFactory = new MyDataWriterFactory();

  void sort(File logFile, long hashCapacity) throws IOException {
    MyComparator comparator = new MyComparator(hashCapacity);
    Sorter<HashSorterEntry> sorter = new Sorter<HashSorterEntry>(config, myDataReaderFactory, myDataWriterFactory, comparator);

    InputStream inputStream = null;
    OutputStream outputStream = null;
    sorter.sort(new SparkeyLogConverter(logFile), null);

  }

  private static class MyComparator implements Comparator<HashSorterEntry> {
    private final long hashCapacity;

    private MyComparator(long hashCapacity) {
      this.hashCapacity = hashCapacity;
    }

    @Override
    public int compare(HashSorterEntry o1, HashSorterEntry o2) {
      long o1Slot = IndexHash.getWantedSlot(o1.hash, hashCapacity);
      long o2Slot = IndexHash.getWantedSlot(o2.hash, hashCapacity);
      if (o1Slot != o2Slot) {
        return Long.compare(o1Slot, o2Slot);
      }

      if (o1.hash != o2.hash) {
        return UnsignedLongs.compare(o1.hash, o2.hash);
      }
      return Long.compare(o1.offset, o2.offset);
    }
  }

  private static class MyDataReader extends DataReader<HashSorterEntry> {
    @Override
    public HashSorterEntry readNext() throws IOException {
      return null;
    }

    @Override
    public int estimateSizeInBytes(HashSorterEntry item) {
      return 0;
    }

    @Override
    public void close() throws IOException {

    }
  }

  private static class MyDataWriter extends DataWriter<HashSorterEntry> {
    @Override
    public void writeEntry(HashSorterEntry item) throws IOException {

    }

    @Override
    public void close() throws IOException {

    }
  }

  private static class MyDataReaderFactory extends DataReaderFactory<HashSorterEntry> {
    @Override
    public DataReader<HashSorterEntry> constructReader(InputStream inputStream) throws IOException {
      return new MyDataReader();
    }
  }

  private static class MyDataWriterFactory extends DataWriterFactory<HashSorterEntry> {
    @Override
    public DataWriter<HashSorterEntry> constructWriter(OutputStream outputStream) throws IOException {
      return new MyDataWriter();
    }
  }

  private static class SparkeyLogConverter extends DataReader<HashSorterEntry> {

    @Override
    public HashSorterEntry readNext() throws IOException {
      return null;
    }

    @Override
    public int estimateSizeInBytes(HashSorterEntry item) {
      return 0;
    }

    @Override
    public void close() throws IOException {

    }
  }
}
