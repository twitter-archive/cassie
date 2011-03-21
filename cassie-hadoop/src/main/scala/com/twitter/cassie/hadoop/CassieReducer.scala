package com.twitter.cassie.hadoop

import org.apache.hadoop.mapreduce._
import org.apache.hadoop.io._
import java.nio.ByteBuffer
import com.twitter.cassie._
import codecs._
import clocks._
import com.twitter.util._
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._
import CassieReducer._

object CassieReducer {
  val DEFAULT_PAGE_SIZE = 1000
  val DEFAULT_MAX_FUTURES = 1000
  val MAX_FUTURES = "max_futures"
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
  var maxFutures: Int = CassieReducer.DEFAULT_MAX_FUTURES
  var i = 0
  var batch: BatchMutationBuilder[ByteBuffer, ByteBuffer, ByteBuffer] = null
  var futures = new ListBuffer[Future[Void]]

  type ReducerContext = Reducer[BytesWritable, ColumnWritable, BytesWritable, BytesWritable]#Context
  
  override def setup(context: ReducerContext) = {
    def conf(key: String) = context.getConfiguration.get(key)
    cluster = new Cluster(conf(HOSTS))
    if(conf(PAGE_SIZE) != null ) page = Integer.valueOf(conf(PAGE_SIZE)).intValue
    if(conf(MAX_FUTURES) != null ) maxFutures = Integer.valueOf(conf(MAX_FUTURES)).intValue
    keyspace = cluster.keyspace(conf(KEYSPACE)).connect()
    columnFamily = keyspace.columnFamily[ByteBuffer, ByteBuffer, ByteBuffer](conf(COLUMN_FAMILY), MicrosecondEpochClock)
    batch = columnFamily.batch
  }

  override def reduce(key: BytesWritable, values: java.lang.Iterable[ColumnWritable], context: ReducerContext) = try {
    for (value <- values) {
      val bufKey = bufCopy(ByteBuffer.wrap(key.getBytes, 0, key.getLength))
      batch.insert(bufKey, new Column(bufCopy(value.name), bufCopy(value.value)))
      i += 1
      if (i % page == 0) {
        futures += batch.execute
        batch = columnFamily.batch
      }
      if(futures.size == maxFutures) {
        Future.join(futures)
        futures = new ListBuffer[Future[Void]]
      }
    }
  } catch {
    case e: Throwable => 
      e.printStackTrace
      throw(e)
  }
  
  private def bufCopy(old: ByteBuffer) = {
    val n = ByteBuffer.allocate(old.remaining)
    n.put(old.array, old.position, old.remaining)
    n.rewind
    n
  }
  
  override def cleanup(context: ReducerContext) = {
    futures += batch.execute
    Future.join(futures)
  }
}