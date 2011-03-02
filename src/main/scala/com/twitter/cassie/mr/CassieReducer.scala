package com.twitter.cassie.mr

import org.apache.hadoop.mapreduce._
import org.apache.hadoop.io._
import java.nio.ByteBuffer
import com.twitter.cassie._
import codecs._
import clocks._
import scala.collection.JavaConversions._
import CassieReducer._

object CassieReducer {
  val DEFAULT_PAGE_SIZE = 1000
  val PAGE_SIZE = "page_size"
  val KEYSPACE = "keyspace"
  val COLUMN_FAMILY = "column_family"
  val HOSTS = "hosts"
}

class CassieReducer extends Reducer[BytesWritable, ColumnWritable, BytesWritable, BytesWritable] {
  
  val defaultReadConsistency = ReadConsistency.One
  val defaultWriteConsistency = WriteConsistency.One
  
  implicit val byteCodec = ByteArrayCodec
  
  var cluster: Cluster = null
  var keyspace: Keyspace = null
  var columnFamily: ColumnFamily[ByteBuffer, ByteBuffer, ByteBuffer] = null
  var page: Int = CassieReducer.DEFAULT_PAGE_SIZE
  var i = 0

  type ReducerContext = Reducer[BytesWritable, ColumnWritable, BytesWritable, BytesWritable]#Context
  
  override def setup(context: ReducerContext) = {
    def conf(key: String) = context.getConfiguration.get(key)
    cluster = new Cluster(conf(HOSTS))
    keyspace = cluster.keyspace(conf(KEYSPACE)).performMapping(false).connect()
    columnFamily = keyspace.columnFamily[ByteBuffer, ByteBuffer, ByteBuffer](conf(COLUMN_FAMILY), MicrosecondEpochClock)
  }

  override def reduce(key: BytesWritable, values: java.lang.Iterable[ColumnWritable], context: ReducerContext) = try {
    for (value <- values) {
      val bufKey = ByteBuffer.wrap(key.getBytes, 0, key.getLength)
      columnFamily.insert(bufKey, new Column(value.name, value.value)).get()
    }
  } catch {
    case e: Throwable => 
      e.printStackTrace
      throw(e)
  }
  
  override def cleanup(context: ReducerContext) = {
  }
}