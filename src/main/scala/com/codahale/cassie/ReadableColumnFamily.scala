package com.codahale.cassie

/**
 * A readable column family.
 *
 * @author coda
 */
trait ReadableColumnFamily[Name, Value] {
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
}
