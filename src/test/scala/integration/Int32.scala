package integration

import chdriver.core.Connection
import chdriver.core.Decoder
import chdriver.core.DriverProperties
import chdriver.core.columns.Column
import org.junit._
import org.testcontainers.containers.GenericContainer
import java.sql.{Array => SQLArray, _}

import chdriver.core.Client
import chdriver.core.ClickhouseProperties

import scala.collection.Iterator
import scala.util.Random

object Int32 {
  final val DB_URL = "jdbc:clickhouse://localhost"
  final val USER = "default"
  final val PASS = ""
  final val ROWS_NUMBER = 1000000

  var chServer: GenericContainer[_] =
    new GenericContainer("yandex/clickhouse-server:latest").withExposedPorts(8123, 9000)

  var conn: java.sql.Connection = _
  var stmt: Statement = _
  var scalaClient: Client = _
  var scalaClickhouseProperties: ClickhouseProperties = _

  @BeforeClass
  def setUp(): Unit = {
    chServer.start()
    val http = chServer.getMappedPort(8123)
    val tcp = chServer.getMappedPort(9000)
    Class.forName("ru.yandex.clickhouse.ClickHouseDriver")
    conn = DriverManager.getConnection(s"$DB_URL:$http", USER, PASS)
    stmt = conn.createStatement
    scalaClient = new Client(
      DriverProperties.DEFAULT_INSERT_BLOCK_SIZE,
      new Connection("localhost", tcp, "q", "default", "default", "")
    )
    scalaClickhouseProperties = new ClickhouseProperties
  }

  @AfterClass
  def setDown(): Unit = {
    conn.close()
    stmt.close()
    chServer.stop()
  }
}

class Int32 {
  import Int32._

  @Test
  def testSelectInt(): Unit = {
    createInt32Table()
    populateInt32Table()

    case class Foo(x: Int)

    implicit val FooDecoder = new Decoder[Foo] {
      override def validate(names: Array[String], types: Array[String]): Boolean = {
        names.sameElements(Array("x")) &&
          types.sameElements(Array("Int32"))
      }

      override def transpose(numberOfItems: Int, columns: Array[Column]): Iterator[Foo] = {
        val xs = new Array[Int](numberOfItems)
        System.arraycopy(columns(0).data, 0, xs, 0, numberOfItems)

        new Iterator[Foo] {
          var i = 0
          override def hasNext = i < numberOfItems

          override def next() = {
            val res = Foo(xs(i))
            i += 1
            res
          }
        }
      }
    }

    val javaRes = new Array[Int](ROWS_NUMBER)
    val scalaRes = new Array[Int](ROWS_NUMBER)
    val sql = "SELECT * FROM test_int32 limit " + ROWS_NUMBER
    for (_ <- 1 to 1) {
      var javaTime = 0L
      var scalaTime = 0L
      var now = System.currentTimeMillis
      var j = 0

      val rs = stmt.executeQuery(sql)
      while (rs.next) {
        javaRes(j) = rs.getInt("x")
        j += 1
      }
      rs.close()
      javaTime += System.currentTimeMillis - now
      System.out.println("jdbc = " + javaTime)

      now = System.currentTimeMillis
      val it = scalaClient.execute[Foo](sql, scalaClickhouseProperties)
      j = 0
      while (it.hasNext) {
        scalaRes(j) = it.next.x
        j += 1
      }
      scalaTime += System.currentTimeMillis - now
      System.out.println("scala = " + scalaTime)

      assert(javaRes.sameElements(scalaRes))
      System.out.println()
    }
  }

  def createInt32Table(): Unit = {
    val forCreate = "create table test_int32(x Int32) engine = Memory;"
    stmt.executeUpdate(forCreate)
  }

  def populateInt32Table(): Unit = {
    conn.setAutoCommit(false)
    val forInsert = "insert into test_int32(x) values (?)"
    val ps = conn.prepareStatement(forInsert)
    for (_ <- 1 to ROWS_NUMBER) {
      ps.setInt(1, Random.nextInt())
      ps.addBatch()
    }
    ps.executeBatch
    conn.commit()
  }
}
