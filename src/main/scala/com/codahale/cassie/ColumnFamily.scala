package com.codahale.cassie

import clocks.Clock

/**
 * A Cassandra column family.
 *
 * @author coda
 */
trait ColumnFamily[Name, Value] {

  /**
   * Returns the optional value of a given column for a given key.
   */
  def get(key: String,
          columnName: Name,
          consistency: ReadConsistency): Option[Column[Name, Value]] = {
    get(key, Set(columnName), consistency).get(columnName)
  }

  /**
   * Returns a map of all column names to the columns for a given key.
   */
  def get(key: String,
          consistency: ReadConsistency): Map[Name, Column[Name, Value]]

  /**
   * Returns a map of the given column names to the columns for a given key.
   */
  def get(key: String,
          columnNames: Set[Name],
          consistency: ReadConsistency): Map[Name, Column[Name, Value]]

  /**
   * Returns a map of keys to given column for a set of keys.
   */
  def multiget(keys: Set[String],
               columnName: Name,
               consistency: ReadConsistency): Map[String, Column[Name, Value]] = {
    multiget(keys, Set(columnName), consistency).map { case (k, v) => (k, v.valuesIterator.next) }
  }

  /**
   * Returns a map of keys to a map of column names to the columns for a given
   * set of keys and columns.
   */
  def multiget(keys: Set[String],
               columnNames: Set[Name],
               consistency: ReadConsistency): Map[String, Map[Name, Column[Name, Value]]]

  /**
   * Inserts a column.
   */
  def insert(key: String,
             column: Column[Name, Value],
             consistency: WriteConsistency): Unit

  /**
   * Removes a column from a key.
   */
  def remove(key: String,
             columnName: Name,
             consistency: WriteConsistency)
            (implicit clock: Clock) {
    remove(key, columnName, clock.timestamp, consistency)
  }

  /**
   * Removes a column from a key with a specific timestamp.
   */
  def remove(key: String,
             columnName: Name,
             timestamp: Long,
             consistency: WriteConsistency): Unit

  /**
   * Removes a key.
   */
  def remove(key: String,
             consistency: WriteConsistency)
            (implicit clock: Clock) {
    remove(key, clock.timestamp, consistency)
  }

  /**
   * Removes a key with a specific timestamp.
   */
  def remove(key: String, timestamp: Long, consistency: WriteConsistency)
}


