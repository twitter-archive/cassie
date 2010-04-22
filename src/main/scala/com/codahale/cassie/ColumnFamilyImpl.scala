package com.codahale.cassie

import client.ClientProvider
import codecs.Codec
import scalaj.collection.Imports._
import org.apache.cassandra.thrift
import com.codahale.logula.Logging

/**
 * A concrete implementation of ColumnFamily.
 *
 * @author coda
 */
class ColumnFamilyImpl[Name, Value](val keyspace: String,
                                    val name: String,
                                    val provider: ClientProvider,
                                    val columnCodec: Codec[Name],
                                    val valueCodec: Codec[Value])
        extends ColumnFamily[Name, Value] with Logging {

  /*
   * Does a get_slice with a maximum count. Could potentially return a bunch
   * of data. Don't misuse.
   */
  def get(key: String, consistency: ReadConsistency): Map[Name, Column[Name, Value]] = {
    val cp = new thrift.ColumnParent(name)
    val pred = new thrift.SlicePredicate()
    pred.setSlice_range(new thrift.SliceRange(Array(), Array(), false, Int.MaxValue))
    log.fine("get_slice(%s, %s, %s, %s, %s)", keyspace, key, cp, pred, consistency.level)
    val result = provider.map { _.get_slice(keyspace, key, cp, pred, consistency.level) }
    result.asScala.map { r => val x = convert(r); (x.name, x) }.toMap
  }

  def get(key: String, columnNames: Set[Name], consistency: ReadConsistency): Map[Name, Column[Name, Value]] = {
    val cp = new thrift.ColumnParent(name)
    val pred = new thrift.SlicePredicate()
    pred.setColumn_names(columnNames.toList.map { columnCodec.encode(_) }.asJava)
    log.fine("get_slice(%s, %s, %s, %s, %s)", keyspace, key, cp, pred, consistency.level)
    val result = provider.map { _.get_slice(keyspace, key, cp, pred, consistency.level) }
    result.asScala.map { r => val x = convert(r); (x.name, x) }.toMap
  }

  def multiget(keys: Set[String], columnNames: Set[Name], consistency: ReadConsistency): Map[String, Map[Name, Column[Name, Value]]] = {
    val cp = new thrift.ColumnParent(name)
    val pred = new thrift.SlicePredicate()
    pred.setColumn_names(columnNames.toList.map { columnCodec.encode(_) }.asJava)
    log.fine("multiget_slice(%s, %s, %s, %s, %s)", keyspace, keys, cp, pred, consistency.level)
    val result = provider.map { _.multiget_slice(keyspace, keys.toList.asJava, cp, pred, consistency.level).asScala }
    return result.map { case (k, v) => (k, v.asScala.map { r => val x = convert(r); (x.name, x) }.toMap) }.toMap
  }

  def insert(key: String, column: Column[Name, Value], consistency: WriteConsistency) {
    val cp = new thrift.ColumnPath(name)
    cp.setColumn(columnCodec.encode(column.name))
    log.fine("insert(%s, %s, %s, %s, %d, %s)", keyspace, key, cp, column.value, column.timestamp, consistency.level)
    provider.map { _.insert(keyspace, key, cp, valueCodec.encode(column.value), column.timestamp, consistency.level) }
  }

  def remove(key: String, columnName: Name, timestamp: Long, consistency: WriteConsistency) {
    val cp = new thrift.ColumnPath(name)
    cp.setColumn(columnCodec.encode(columnName))
    log.fine("remove(%s, %s, %s, %d, %s)", keyspace, key, cp, timestamp, consistency.level)
    provider.map { _.remove(keyspace, key, cp, timestamp, consistency.level) }
  }

  def remove(key: String, timestamp: Long, consistency: WriteConsistency) {
    val cp = new thrift.ColumnPath(name)
    log.fine("remove(%s, %s, %s, %d, %s)", keyspace, key, cp, timestamp, consistency.level)
    provider.map { _.remove(keyspace, key, cp, timestamp, consistency.level) }
  }

  private def convert(colOrSCol: thrift.ColumnOrSuperColumn): Column[Name, Value] = {
    Column(
      columnCodec.decode(colOrSCol.column.name),
      valueCodec.decode(colOrSCol.column.value),
      colOrSCol.column.timestamp
    )
  }
}
