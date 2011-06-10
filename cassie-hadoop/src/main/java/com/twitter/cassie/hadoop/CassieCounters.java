package com.twitter.cassie.hadoop;

import org.apache.hadoop.mapreduce.Counters;

public class CassieCounters {
  public static enum Counters { SUCCESS, RETRY, FAILURE }
}