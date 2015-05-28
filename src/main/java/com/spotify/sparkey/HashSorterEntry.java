package com.spotify.sparkey;

public class HashSorterEntry {
  final long hash;
  final long offset;

  public HashSorterEntry(long hash, long offset) {
    this.hash = hash;
    this.offset = offset;
  }
}
