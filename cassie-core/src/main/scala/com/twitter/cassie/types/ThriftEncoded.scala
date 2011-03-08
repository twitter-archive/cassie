package com.twitter.cassie.types

import org.apache.thrift._

case class ThriftEncoded[T <: TBase[_, _]](value: T)
