package com.twitter.cassie.tests.util

import org.apache.thrift.transport.{TServerSocket, TFramedTransport}
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.server.TThreadPoolServer
import java.util.concurrent.CountDownLatch
import org.apache.cassandra.finagle.thrift._
import java.nio.ByteBuffer
import com.twitter.cassie.codecs.Utf8Codec
import scala.collection.JavaConversions._
import java.util.{HashSet => JHashSet, ArrayList => JArrayList, SortedSet => JSortedSet,
  TreeMap => JTreeMap, List => JList, TreeSet => JTreeSet, Map => JMap}
import java.util.Comparator

object FakeCassandra {
  class ServerThread(cassandra: Cassandra.Iface, port: Int) extends Thread {
    setDaemon(true)
    val serverTransport = new TServerSocket(port)
    val protFactory = new TBinaryProtocol.Factory(true, true)
    val transportFactory = new TFramedTransport.Factory()
    val processor = new Cassandra.Processor(cassandra)
    val server = new TThreadPoolServer(processor, serverTransport, transportFactory, protFactory)
    val latch = new CountDownLatch(1)

    override def run {
      latch.countDown()
      server.serve()
    }
  }
}

/**
 * This is a thrift-service that will do real operations on an in-memory data structure.
 * You don't have to create keyspaces or column families; this happens implicitly.  We
 * support a limited and expanding subset of the cassandra api.
 */
class FakeCassandra(val port: Int) extends Cassandra.Iface {
  // Taken from cassandra ByteBufferUtil#compareUnsigned
  // Questionable style because it's as straight a port as possible
  // replace this with a straight copy of the java or split the fake out into a subproject and depend on cassandra
  val comparator = new Comparator[ByteBuffer] {
    def compare(o1: ByteBuffer, o2: ByteBuffer): Int = {
      if(null == o1) {
        if(null == o2) return 0
        else return -1
      }

      val minLength = Math.min(o1.remaining(), o2.remaining())
      var x = 0
      var i = o1.position()
      var j = o2.position()
      while (x < minLength) {
        if (o1.get(i) != o2.get(j)) {

          // compare non-equal bytes as unsigned
          return if ((o1.get(i) & 0xFF) < (o2.get(j) & 0xFF)) -1 else 1
        }

        x += 1
        i += 1
        j += 1
      }

      return if (o1.remaining() == o2.remaining()) 0 else (if (o1.remaining() < o2.remaining()) -1 else 1)
    }
  }

  var thread: FakeCassandra.ServerThread = null
  var currentKeyspace = "default"

  //                     keyspace        CF              row         column
  val data = new JTreeMap[String, JTreeMap[String, JTreeMap[ByteBuffer, JTreeMap[ByteBuffer, ColumnOrSuperColumn]]]]

  def start() = {
    thread = new FakeCassandra.ServerThread(this, port)
    thread.start()
    thread.latch.await()
  }

  private def getColumnFamily(cp: ColumnParent): JTreeMap[ByteBuffer, JTreeMap[ByteBuffer, ColumnOrSuperColumn]] =
    getColumnFamily(cp.getColumn_family)
  private def getColumnFamily(cp: ColumnPath): JTreeMap[ByteBuffer, JTreeMap[ByteBuffer, ColumnOrSuperColumn]] = getColumnFamily(cp.getColumn_family)
  private def getColumnFamily(name: String): JTreeMap[ByteBuffer, JTreeMap[ByteBuffer, ColumnOrSuperColumn]]  = synchronized {
    var keyspace = data.get(currentKeyspace)
    if (keyspace == null) {
      keyspace = new JTreeMap[String, JTreeMap[ByteBuffer, JTreeMap[ByteBuffer, ColumnOrSuperColumn]]]
      data.put(currentKeyspace, keyspace)
    }
    var cf = keyspace.get(name)
    if (cf == null) {
      cf = new JTreeMap[ByteBuffer, JTreeMap[ByteBuffer, ColumnOrSuperColumn]](comparator)
      keyspace.put(name, cf)
    }
    cf
  }

  def stop() = {
    thread.server.stop()
    reset()
  }

  def reset() = data.clear()

  def set_keyspace(keyspace: String) = currentKeyspace = keyspace

  def insert(key: ByteBuffer, column_parent: ColumnParent, column: Column, consistency_level: ConsistencyLevel) = {
    val cf = getColumnFamily(column_parent)
    var row = cf.get(key)
    if(row == null) {
      row = new JTreeMap[ByteBuffer, ColumnOrSuperColumn](comparator)
      cf.put(key, row)
    }
    row.put(column.BufferForName, new ColumnOrSuperColumn().setColumn(column))
  }

  def get_slice(key: ByteBuffer, column_parent: ColumnParent, 
      predicate: SlicePredicate, consistency_level: ConsistencyLevel): JList[ColumnOrSuperColumn] = 
    get_slice(key, column_parent, predicate, consistency_level, System.currentTimeMillis, false)

  def get_slice(key: ByteBuffer, column_parent: ColumnParent, predicate: SlicePredicate, 
      consistency_level: ConsistencyLevel, asOf: Long, andDelete: Boolean): JList[ColumnOrSuperColumn] = {
    val cf = getColumnFamily(column_parent)
    var row = cf.get(key)
    if (row == null)
      return new JArrayList()
    var list: JArrayList[ColumnOrSuperColumn] = null

    if (predicate.isSetSlice_range() && predicate.isSetColumn_names) {
      //throw
    }
    if(predicate.isSetSlice_range()) {
      val sr = predicate.getSlice_range
      if(sr.isSetStart && sr.getStart().length > 0 &&
         sr.isSetFinish && sr.getFinish().length > 0) {
        list = new JArrayList(row.subMap(sr.BufferForStart, sr.BufferForFinish).values)
      } else if (sr.isSetStart() && sr.getStart.length > 0) {
        list = new JArrayList(row.tailMap(sr.BufferForStart).values)
      } else if (sr.isSetFinish() && sr.getFinish.length > 0){
        list = new JArrayList(row.headMap(sr.BufferForFinish).values)
      } else {
        list = new JArrayList(row.values)
      }
    } else if(predicate.isSetColumn_names) {
      list = new JArrayList
      val names = predicate.getColumn_names
      for (name <- names) {
        list.add(row.get(name))
      }
    }
    return list
  }

  def batch_mutate(mutation_Map: JMap[ByteBuffer, JMap[String, JList[Mutation]]],
      consistency_level: ConsistencyLevel) = {
    for((key, map) <- mutation_Map) {
      for((cf, mutations) <- map) {
        val cp = new ColumnParent
        cp.setColumn_family(cf)
        for(mutation <- mutations) {
          if(mutation.isSetColumn_or_supercolumn) {
            val cosc = mutation.getColumn_or_supercolumn
            if(cosc.isSetColumn) {
              insert(key, cp, cosc.getColumn, ConsistencyLevel.ANY)
            } else {
              throw new UnsupportedOperationException("no supercolumn support")
            }
          }
          if(mutation.isSetDeletion) {
            val deletion = mutation.getDeletion
            val time = if (deletion.isSetTimestamp) deletion.getTimestamp else System.currentTimeMillis
            for (name <- deletion.getPredicate.getColumn_names) {
              remove(key, (new ColumnPath).setColumn_family(cf).setColumn(name), deletion.getTimestamp, consistency_level)
            }
          }
        }
      }
    }
  }

  def login(auth_request: AuthenticationRequest ) = throw new UnsupportedOperationException

  def get(key: ByteBuffer, column_path: ColumnPath, consistency_level: ConsistencyLevel) =
    throw new UnsupportedOperationException

  def get_count(key: ByteBuffer, column_parent: ColumnParent, predicate: SlicePredicate,
      consistency_level: ConsistencyLevel) = throw new UnsupportedOperationException

  def multiget_slice(keys: JList[ByteBuffer], column_parent: ColumnParent,
      predicate: SlicePredicate, consistency_level: ConsistencyLevel) =
    throw new UnsupportedOperationException

  def multiget_count(keys: JList[ByteBuffer], column_parent: ColumnParent,
      predicate: SlicePredicate, consistency_level: ConsistencyLevel) =
    throw new UnsupportedOperationException

  def get_range_slices(column_parent: ColumnParent, predicate: SlicePredicate, 
      range: KeyRange, consistency_level: ConsistencyLevel) = throw new UnsupportedOperationException

  def get_indexed_slices(column_parent: ColumnParent, index_clause: IndexClause,
      column_predicate: SlicePredicate, consistency_level: ConsistencyLevel) = 
    throw new UnsupportedOperationException

  def remove(key: ByteBuffer, column_path: ColumnPath, timestamp: Long,
      consistency_level: ConsistencyLevel) = {
    val cf = getColumnFamily(column_path).get(key).remove(column_path.BufferForColumn)
  }

  def truncate(cfname: String) = throw new UnsupportedOperationException

  def add(key: ByteBuffer, column_parent: ColumnParent, column: CounterColumn,
      consistency_level: ConsistencyLevel) = synchronized {
    val cf = getColumnFamily(column_parent)
    var row = cf.get(key)
    if(row == null) {
      row = new JTreeMap[ByteBuffer, ColumnOrSuperColumn](comparator)
      cf.put(key, row)
    }
    var col = row.get(column.BufferForName)
    if (col != null) {
      column.setValue(column.getValue + row.get(column.BufferForName).getCounter_column.getValue)
    }
    row.put(column.BufferForName, new ColumnOrSuperColumn().setCounter_column(column))
  }

  def remove_counter(key: ByteBuffer, path: ColumnPath,
      consistency_level: ConsistencyLevel) = throw new UnsupportedOperationException

  def describe_schema_versions() = throw new UnsupportedOperationException
  def describe_keyspaces() = throw new UnsupportedOperationException
  def describe_cluster_name() = throw new UnsupportedOperationException
  def describe_version() = throw new UnsupportedOperationException
  def describe_ring(keyspace: String) = throw new UnsupportedOperationException
  def describe_partitioner() = throw new UnsupportedOperationException
  def describe_snitch() = throw new UnsupportedOperationException
  def describe_keyspace(keyspace: String) = throw new UnsupportedOperationException

  def describe_splits(cfName: String, start_token: String, end_token: String, 
      keys_per_split: Int) = throw new UnsupportedOperationException

  def system_add_column_family(cf_def: CfDef) = throw new UnsupportedOperationException
  def system_drop_column_family(column_family: String) = throw new UnsupportedOperationException
  def system_add_keyspace(ks_def: KsDef) = throw new UnsupportedOperationException
  def system_drop_keyspace(keyspace: String) = throw new UnsupportedOperationException
  def system_update_keyspace(ks_def: KsDef) = throw new UnsupportedOperationException
  def system_update_column_family(cf_def: CfDef) = throw new UnsupportedOperationException
  def execute_cql_query(query: ByteBuffer, compression: Compression) = throw new UnsupportedOperationException
}