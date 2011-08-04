import com.twitter.ostrich.admin.RuntimeEnvironment
import com.twitter.ostrich.admin.AdminHttpService
import com.twitter.cassie._
import java.util.{HashSet => JSet}
import com.twitter.cassie.codecs._
import com.twitter.finagle.stats.OstrichStatsReceiver

var a = new AdminHttpService(9990, 10, new RuntimeEnvironment(this))
a.start
val cluster = new Cluster("127.0.0.1", new OstrichStatsReceiver)
val keyspace = cluster.keyspace("test").connect();
val cf = keyspace.columnFamily("test", Utf8Codec, Utf8Codec, Utf8Codec)

cf.insert("foo", Column("bar", "bam"))()
cf.multigetColumns(new JSet[String](), new JSet[String]())()
cf.getRowSlice("foo", Some("bar"), None, 100, Order.Normal)()
val f = cf.columnsIteratee("foo").foreach(_ => Unit)
f()

val f2 = cf.rowsIteratee(10).foreach{case(_,_) => Unit}
f2()
cf.batch.insert("foo2", Column("bar2", "bam2")).execute()()
cf.removeRowWithTimestamp("foo", 1)()
cf.removeColumn("foo", "bar")()
