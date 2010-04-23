package com.codahale.cassie

import clocks.Clock

/**
 * A readable column family.
 * 
 * @author coda
 */
trait WritableColumnFamily[Name, Value] {
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
