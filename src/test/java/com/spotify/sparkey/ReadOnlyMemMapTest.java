package com.spotify.sparkey;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ReadOnlyMemMapTest {

  @Test
  public void testDontRunOutOfFileDescriptors() throws Exception {
    for (int iter = 0; iter < 10*1000; iter++) {
      ReadOnlyMemMap memMap = new ReadOnlyMemMap(new File("README.md"));
      ArrayList<ReadOnlyMemMap> maps = Lists.newArrayList();
      for (int i = 0; i < 100; i++) {
        maps.add(memMap);
      }
      memMap.close();
      for (ReadOnlyMemMap map : maps) {
        try {
          map.readUnsignedByte(0);
          fail();
        } catch (IOException e) {
        }
      }
    }
  }

  @Test
  public void testConcurrentReadWhileClosing() throws Exception {
    final AtomicBoolean running = new AtomicBoolean(true);
    final ReadOnlyMemMap memMap = new ReadOnlyMemMap(new File("README.md"));
    final List<Exception> failures = Collections.synchronizedList(Lists.<Exception>newArrayList());
    List<Thread> threads = Lists.newArrayList();
    for (int i = 0; i < 100; i++) {
      Thread thread = new Thread() {
        @Override
        public void run() {
          ReadOnlyMemMap map = memMap;
          while (running.get()) {
            try {
              map.readUnsignedByte(0);
            } catch (IOException e) {
              if (!e.getMessage().equals("Reader has been closed")) {
                e.printStackTrace();
                failures.add(e);
              }
            }
          }
        }
      };
      threads.add(thread);
      thread.start();
    }
    memMap.close();
    Thread.sleep(100);
    running.set(false);
    for (Thread thread : threads) {
      thread.join();
    }
    assertEquals(0, failures.size());

  }
}
