package com.twitter.cassie.tests

import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers
import com.twitter.cassie.Column
import com.twitter.cassie.clocks.Clock
import com.twitter.conversions.time._

class ColumnTest extends Spec with MustMatchers {
  describe("a column with an explicit ttl") {
    val col = Column("id", 300, 400L, 1.minute)

    it("has a name") {
      col.name must equal("id")
    }

    it("has a value") {
      col.value must equal(300)
    }

    it("has a timestamp") {
      col.timestamp must equal(Some(400L))
    }

    it("has a ttl") {
      col.ttl must equal(Some(60.seconds))
    }

  }
}
