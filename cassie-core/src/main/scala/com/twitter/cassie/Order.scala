package com.twitter.cassie

/**
  * An ordering of columns. */
sealed case class Order(normal: Boolean) {
  val reversed = !normal
  override def toString = "Order(%s)".format(if (normal) "normal" else "reversed")
}


object Order {
  /**
    * Return the columns in normal order. */
  val Normal = Order(true)

  /**
    * Return the columns in reverse order. */
  val Reversed = Order(false)
}

