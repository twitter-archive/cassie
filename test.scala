import com.twitter.ostrich.admin.RuntimeEnvironment
import com.twitter.ostrich.admin.AdminHttpService
import com.twitter.conversions.time._
import com.twitter.cassie._
import types.{LexicalUUID, VarInt, FixedLong}
import com.twitter.cassie.Codecs._

var a = new AdminHttpService(9990, 10, new RuntimeEnvironment(this))
a.start
val cluster = new Cluster("127.0.0.1")
val keyspace = cluster.keyspace("test").connect();
val cf = keyspace.columnFamily[String, String, String]("test")
