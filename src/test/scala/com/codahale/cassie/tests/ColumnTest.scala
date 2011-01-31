package com.twitter.cassie.tests

import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers
import com.twitter.cassie.Column
import com.twitter.cassie.clocks.Clock

class ColumnTest extends Spec with MustMatchers {
  describe("a column with an explicit timestamp") {
    val col = Column("id", 300, 400L)

    it("has a name") {
      col.name must equal("id")
    }

    it("has a value") {
      col.value must equal(300)
    }

    it("has a timestamp") {
      col.timestamp must equal(400L)
    }
  }

  describe("a column with an implicit timestamp") {
    implicit val clock = new Clock {
      def timestamp = 599238L
    }
    val col = Column("id", 300)

    it("has a name") {
      col.name must equal("id")
    }

    it("has a value") {
      col.value must equal(300)
    }

    it("has a timestamp from the implicit clock") {
      col.timestamp must equal(599238L)
    }
  }
}
