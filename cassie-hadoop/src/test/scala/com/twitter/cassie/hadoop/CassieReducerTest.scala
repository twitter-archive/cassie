package com.twitter.cassie.hadoop

import org.apache.hadoop.mapreduce._
import org.apache.hadoop.io._
import java.nio.ByteBuffer
import com.twitter.cassie._
import com.twitter.cassie.codecs._
import com.twitter.cassie.clocks._
import com.twitter.util._
import org.apache.hadoop.conf._
import org.apache.hadoop.util._
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._
import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers
import java.io._
import java.util._
import com.twitter.cassie.tests.util._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io._
import org.apache.hadoop.util._
import org.apache.hadoop.conf._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.{Mapper, Reducer, Job}
import org.apache.hadoop.mapreduce.lib.input._
import org.apache.hadoop.mapreduce.lib.output._
import CassieReducer._
import scala.math._
import com.twitter.conversions.time._
import com.twitter.cassie._

object Fake {
  class Map extends Mapper[LongWritable, Text, BytesWritable, ColumnWritable]{

    type MapperContext = Mapper[LongWritable, Text, BytesWritable, ColumnWritable]#Context

    override def map(lineNumber: LongWritable, line: Text, context: MapperContext): Unit = {
      val key = bb(lineNumber.get)
      val column = new ColumnWritable(bb("default"), bb(line))
      val bw = new BytesWritable
      bw.set(key.array(), key.position, key.remaining)
      context.write(bw, column)
    }

    private def bb(a: Any) = {
      val s = a.toString
      val b = s.getBytes
      ByteBuffer.wrap(b)
    }
  }
}

class NonMappingCassieReducer extends CassieReducer {
  override def configure(ksb: KeyspaceBuilder) = {
    ksb.mapHostsEvery(0.seconds)
  }
}

class TestScript extends Configured with Tool {

  def run(args: Array[String]): Int = {
    val path = "/tmp/cassie-test"
    val writer = new PrintStream(new File(path))
    for(arg <- args) writer.println(arg)
    writer.close

    val inputPath = new Path(path)
    val fs = inputPath.getFileSystem(getConf())

    val job = new Job(getConf())
    val jc = job.getConfiguration()

    jc.set(HOSTS, "127.0.0.1")
    jc.set(KEYSPACE, "ks")
    jc.set(COLUMN_FAMILY, "cf")

    job.setJarByClass(getClass)
    job.setJobName(getClass.getName)

    job.setMapperClass(classOf[Fake.Map])

    job.setMapOutputKeyClass(classOf[BytesWritable])
    job.setMapOutputValueClass(classOf[ColumnWritable])
    job.setReducerClass(classOf[NonMappingCassieReducer])
    job.setNumReduceTasks(1)

    job.setInputFormatClass(classOf[TextInputFormat])
    job.setOutputFormatClass(classOf[NullOutputFormat[_,_]])

    FileInputFormat.setInputPaths(job, inputPath)

    job.waitForCompletion(true)
    0
  }

}

class CassieReducerTest extends Spec with MustMatchers{

  describe("CassieReducer") {
    it("should go through a lifecycle") {
      val fake = new FakeCassandra(9160)
      fake.start()
      Thread.sleep(1000)
      ToolRunner.run(new Configuration(), new TestScript(), Array("hello", "world"))
      implicit val keyCodec = Utf8Codec
      val cluster = new Cluster("127.0.0.1")
      val ks = cluster.keyspace("ks").mapHostsEvery(0.seconds).connect()
      val cf = ks.columnFamily[String, String, String]("cf")

      cf.getRow("0").get().get("default").value must equal("hello")

      fake.stop()
    }

    it("should not blow up when empty input data") {
      val fake = new FakeCassandra(9160)
      fake.start()
      Thread.sleep(1000)
      ToolRunner.run(new Configuration(), new TestScript(), Array())
      implicit val keyCodec = Utf8Codec
      val cluster = new Cluster("127.0.0.1")
      val ks = cluster.keyspace("ks").mapHostsEvery(0.seconds).connect()
      val cf = ks.columnFamily[String, String, String]("cf")

      fake.stop()
    }
  }
}