package integration.copypasted

import java.sql.{Array => _, Connection => SQLConnection, _}

import chdriver.core._
import chdriver.core.columns.Column
import org.junit._
import org.testcontainers.containers.GenericContainer

import scala.collection.Iterator
import scala.util.Random

/**
 * Good old copy-paste, to be sure that very basic selects are supported.
 * Random data is inserted via JDBC, then both scala driver & JDBC do many selects and verify that result is the same.
 *
 * Since there are `ITERATION` repeats of big (rows number = `ROWS_NUMBER`) queries, this class also gives some
 * information about performance.
 */
object BasicTypesSelect {
  final val DB_URL = "jdbc:clickhouse://localhost"
  final val USER = "default"
  final val PASS = ""
  final val ROWS_NUMBER = 1000000
  final val ITERATIONS = 42
  final val chServer: GenericContainer[_] =
    new GenericContainer("yandex/clickhouse-server:latest").withExposedPorts(8123, 9000)

  var conn: SQLConnection = _
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
    conn.setAutoCommit(false)
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

class BasicTypesSelect {
  import BasicTypesSelect._

  @Test
  def testSelectInt8(): Unit = {
    case class Foo(x: Byte)

    implicit val FooDecoder = new Decoder[Foo] {
      override def validate(names: Array[String], types: Array[String]): Boolean = {
        names.sameElements(Array("x")) &&
        types.sameElements(Array("Int8"))
      }

      override def transpose(numberOfItems: Int, columns: Array[Column]): Iterator[Foo] = {
        val xs = new Array[Byte](numberOfItems)
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

    val forCreate = "create table test_int8(x Int8) engine = Memory;"
    stmt.executeUpdate(forCreate)

    val forInsert = "insert into test_int8(x) values (?)"
    val ps = conn.prepareStatement(forInsert)
    for (_ <- 1 to ROWS_NUMBER) {
      ps.setByte(1, Random.nextInt().toByte)
      ps.addBatch()
    }
    ps.executeBatch()
    conn.commit()

    val javaRes = new Array[Byte](ROWS_NUMBER)
    val scalaRes = new Array[Byte](ROWS_NUMBER)
    var javaTime = 0L
    var scalaTime = 0L
    val sql = "select * from test_int8 limit " + ROWS_NUMBER

    for (_ <- 1 to ITERATIONS) {
      var now = System.currentTimeMillis
      var j = 0

      val rs = stmt.executeQuery(sql)
      while (rs.next) {
        javaRes(j) = rs.getByte("x")
        j += 1
      }
      rs.close()
      javaTime += System.currentTimeMillis - now

      now = System.currentTimeMillis
      val it = scalaClient.execute[Foo](sql, scalaClickhouseProperties)
      j = 0
      while (it.hasNext) {
        scalaRes(j) = it.next.x
        j += 1
      }
      scalaTime += System.currentTimeMillis - now

      assert(javaRes.sameElements(scalaRes))
    }

    println(s"Total for table [$forCreate] and $ITERATIONS iterations: scala=$scalaTime, java=$javaTime")
  }

  @Test
  def testSelectInt16(): Unit = {
    case class Foo(x: Short)

    implicit val FooDecoder = new Decoder[Foo] {
      override def validate(names: Array[String], types: Array[String]): Boolean = {
        names.sameElements(Array("x")) &&
        types.sameElements(Array("Int16"))
      }

      override def transpose(numberOfItems: Int, columns: Array[Column]): Iterator[Foo] = {
        val xs = new Array[Short](numberOfItems)
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

    val forCreate = "create table test_int16(x Int16) engine = Memory;"
    stmt.executeUpdate(forCreate)

    val forInsert = "insert into test_int16(x) values (?)"
    val ps = conn.prepareStatement(forInsert)
    for (_ <- 1 to ROWS_NUMBER) {
      ps.setShort(1, Random.nextInt().toShort)
      ps.addBatch()
    }
    ps.executeBatch()
    conn.commit()

    val javaRes = new Array[Short](ROWS_NUMBER)
    val scalaRes = new Array[Short](ROWS_NUMBER)
    var javaTime = 0L
    var scalaTime = 0L
    val sql = "select * from test_int16 limit " + ROWS_NUMBER

    for (_ <- 1 to ITERATIONS) {
      var now = System.currentTimeMillis
      var j = 0

      val rs = stmt.executeQuery(sql)
      while (rs.next) {
        javaRes(j) = rs.getShort("x")
        j += 1
      }
      rs.close()
      javaTime += System.currentTimeMillis - now

      now = System.currentTimeMillis
      val it = scalaClient.execute[Foo](sql, scalaClickhouseProperties)
      j = 0
      while (it.hasNext) {
        scalaRes(j) = it.next.x
        j += 1
      }
      scalaTime += System.currentTimeMillis - now

      assert(javaRes.sameElements(scalaRes))
    }

    println(s"Total for table [$forCreate] and $ITERATIONS iterations: scala=$scalaTime, java=$javaTime")
  }

  @Test
  def testSelectInt32(): Unit = {
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

    val forCreate = "create table test_int32(x Int32) engine = Memory;"
    stmt.executeUpdate(forCreate)

    val forInsert = "insert into test_int32(x) values (?)"
    val ps = conn.prepareStatement(forInsert)
    for (_ <- 1 to ROWS_NUMBER) {
      ps.setInt(1, Random.nextInt())
      ps.addBatch()
    }
    ps.executeBatch()
    conn.commit()

    val javaRes = new Array[Int](ROWS_NUMBER)
    val scalaRes = new Array[Int](ROWS_NUMBER)
    var javaTime = 0L
    var scalaTime = 0L
    val sql = "select * from test_int32 limit " + ROWS_NUMBER

    for (_ <- 1 to ITERATIONS) {
      var now = System.currentTimeMillis
      var j = 0

      val rs = stmt.executeQuery(sql)
      while (rs.next) {
        javaRes(j) = rs.getInt("x")
        j += 1
      }
      rs.close()
      javaTime += System.currentTimeMillis - now

      now = System.currentTimeMillis
      val it = scalaClient.execute[Foo](sql, scalaClickhouseProperties)
      j = 0
      while (it.hasNext) {
        scalaRes(j) = it.next.x
        j += 1
      }
      scalaTime += System.currentTimeMillis - now

      assert(javaRes.sameElements(scalaRes))
    }

    println(s"Total for table [$forCreate] and $ITERATIONS iterations: scala=$scalaTime, java=$javaTime")
  }

  @Test
  def testSelectInt64(): Unit = {
    case class Foo(x: Long)

    implicit val FooDecoder = new Decoder[Foo] {
      override def validate(names: Array[String], types: Array[String]): Boolean = {
        names.sameElements(Array("x")) &&
        types.sameElements(Array("Int64"))
      }

      override def transpose(numberOfItems: Int, columns: Array[Column]): Iterator[Foo] = {
        val xs = new Array[Long](numberOfItems)
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

    val forCreate = "create table test_int64(x Int64) engine = Memory;"
    stmt.executeUpdate(forCreate)

    val forInsert = "insert into test_int64(x) values (?)"
    val ps = conn.prepareStatement(forInsert)
    for (_ <- 1 to ROWS_NUMBER) {
      ps.setLong(1, Random.nextLong())
      ps.addBatch()
    }
    ps.executeBatch()
    conn.commit()

    val javaRes = new Array[Long](ROWS_NUMBER)
    val scalaRes = new Array[Long](ROWS_NUMBER)
    var javaTime = 0L
    var scalaTime = 0L
    val sql = "select * from test_int64 limit " + ROWS_NUMBER

    for (_ <- 1 to ITERATIONS) {
      var now = System.currentTimeMillis
      var j = 0

      val rs = stmt.executeQuery(sql)
      while (rs.next) {
        javaRes(j) = rs.getLong("x")
        j += 1
      }
      rs.close()
      javaTime += System.currentTimeMillis - now

      now = System.currentTimeMillis
      val it = scalaClient.execute[Foo](sql, scalaClickhouseProperties)
      j = 0
      while (it.hasNext) {
        scalaRes(j) = it.next.x
        j += 1
      }
      scalaTime += System.currentTimeMillis - now

      assert(javaRes.sameElements(scalaRes))
    }

    println(s"Total for table [$forCreate] and $ITERATIONS iterations: scala=$scalaTime, java=$javaTime")
  }

  @Test
  def testSelectUInt8(): Unit = {
    case class Foo(x: Int)

    implicit val FooDecoder = new Decoder[Foo] {
      override def validate(names: Array[String], types: Array[String]): Boolean = {
        names.sameElements(Array("x")) &&
        types.sameElements(Array("UInt8"))
      }

      override def transpose(numberOfItems: Int, columns: Array[Column]) = {
        new Iterator[Foo] {
          val xs = columns(0).data.asInstanceOf[Array[Byte]]
          var i = 0

          override def hasNext = i < numberOfItems

          override def next() = {
            val res = Foo(xs(i) & 0xFF)
            i += 1
            res
          }
        }
      }
    }

    val forCreate = "create table test_uint8(x UInt8) engine = Memory;"
    stmt.executeUpdate(forCreate)

    val forInsert = "insert into test_uint8(x) values (?)"
    val ps = conn.prepareStatement(forInsert)
    for (_ <- 1 to ROWS_NUMBER) {
      ps.setInt(1, Random.nextInt().toByte & 0xFF)
      ps.addBatch()
    }
    ps.executeBatch()
    conn.commit()

    val javaRes = new Array[Int](ROWS_NUMBER)
    val scalaRes = new Array[Int](ROWS_NUMBER)
    var javaTime = 0L
    var scalaTime = 0L
    val sql = "select * from test_uint8 limit " + ROWS_NUMBER

    for (_ <- 1 to ITERATIONS) {
      var now = System.currentTimeMillis
      var j = 0

      val rs = stmt.executeQuery(sql)
      while (rs.next) {
        javaRes(j) = rs.getInt("x")
        j += 1
      }
      rs.close()
      javaTime += System.currentTimeMillis - now

      now = System.currentTimeMillis
      val it = scalaClient.execute[Foo](sql, scalaClickhouseProperties)
      j = 0
      while (it.hasNext) {
        scalaRes(j) = it.next.x
        j += 1
      }
      scalaTime += System.currentTimeMillis - now

      assert(javaRes.sameElements(scalaRes))
    }

    println(s"Total for table [$forCreate] and $ITERATIONS iterations: scala=$scalaTime, java=$javaTime")
  }

  @Test
  def testSelectUInt16(): Unit = {
    case class Foo(x: Int)

    implicit val FooDecoder = new Decoder[Foo] {
      override def validate(names: Array[String], types: Array[String]): Boolean = {
        names.sameElements(Array("x")) &&
        types.sameElements(Array("UInt16"))
      }

      override def transpose(numberOfItems: Int, columns: Array[Column]) = {
        new Iterator[Foo] {
          val xs = columns(0).data.asInstanceOf[Array[Short]]
          var i = 0

          override def hasNext = i < numberOfItems

          override def next() = {
            val res = Foo(xs(i) & 0xFFFF)
            i += 1
            res
          }
        }
      }
    }

    val forCreate = "create table test_uint16(x UInt16) engine = Memory;"
    stmt.executeUpdate(forCreate)

    val forInsert = "insert into test_uint16(x) values (?)"
    val ps = conn.prepareStatement(forInsert)
    for (_ <- 1 to ROWS_NUMBER) {
      ps.setInt(1, Random.nextInt().toShort & 0xFFFF)
      ps.addBatch()
    }
    ps.executeBatch()
    conn.commit()

    val javaRes = new Array[Int](ROWS_NUMBER)
    val scalaRes = new Array[Int](ROWS_NUMBER)
    var javaTime = 0L
    var scalaTime = 0L
    val sql = "select * from test_uint16 limit " + ROWS_NUMBER

    for (_ <- 1 to ITERATIONS) {
      var now = System.currentTimeMillis
      var j = 0

      val rs = stmt.executeQuery(sql)
      while (rs.next) {
        javaRes(j) = rs.getInt("x")
        j += 1
      }
      rs.close()
      javaTime += System.currentTimeMillis - now

      now = System.currentTimeMillis
      val it = scalaClient.execute[Foo](sql, scalaClickhouseProperties)
      j = 0
      while (it.hasNext) {
        scalaRes(j) = it.next.x
        j += 1
      }
      scalaTime += System.currentTimeMillis - now

      assert(javaRes.sameElements(scalaRes))
    }

    println(s"Total for table [$forCreate] and $ITERATIONS iterations: scala=$scalaTime, java=$javaTime")
  }

  @Test
  def testSelectUInt32(): Unit = {
    case class Foo(x: Long)

    implicit val FooDecoder = new Decoder[Foo] {
      override def validate(names: Array[String], types: Array[String]): Boolean = {
        names.sameElements(Array("x")) &&
        types.sameElements(Array("UInt32"))
      }

      override def transpose(numberOfItems: Int, columns: Array[Column]) = {
        new Iterator[Foo] {
          val xs = columns(0).data.asInstanceOf[Array[Int]]
          var i = 0

          override def hasNext = i < numberOfItems

          override def next() = {
            val res = Foo(xs(i) & 0xFFFFFFFFL)
            i += 1
            res
          }
        }
      }
    }

    val forCreate = "create table test_uint32(x UInt32) engine = Memory;"
    stmt.executeUpdate(forCreate)

    val forInsert = "insert into test_uint32(x) values (?)"
    val ps = conn.prepareStatement(forInsert)
    for (_ <- 1 to ROWS_NUMBER) {
      ps.setLong(1, Random.nextInt() & 0xFFFFFFFFL)
      ps.addBatch()
    }
    ps.executeBatch()
    conn.commit()

    val javaRes = new Array[Long](ROWS_NUMBER)
    val scalaRes = new Array[Long](ROWS_NUMBER)
    var javaTime = 0L
    var scalaTime = 0L
    val sql = "select * from test_uint32 limit " + ROWS_NUMBER

    for (_ <- 1 to ITERATIONS) {
      var now = System.currentTimeMillis
      var j = 0

      val rs = stmt.executeQuery(sql)
      while (rs.next) {
        javaRes(j) = rs.getLong("x")
        j += 1
      }
      rs.close()
      javaTime += System.currentTimeMillis - now

      now = System.currentTimeMillis
      val it = scalaClient.execute[Foo](sql, scalaClickhouseProperties)
      j = 0
      while (it.hasNext) {
        scalaRes(j) = it.next.x
        j += 1
      }
      scalaTime += System.currentTimeMillis - now

      assert(javaRes.sameElements(scalaRes))
    }

    println(s"Total for table [$forCreate] and $ITERATIONS iterations: scala=$scalaTime, java=$javaTime")
  }

  @Test
  def testSelectUInt64(): Unit = {
    case class Foo(x: BigDecimal)

    implicit val FooDecoder = new Decoder[Foo] {
      override def validate(names: Array[String], types: Array[String]): Boolean = {
        names.sameElements(Array("x")) &&
        types.sameElements(Array("UInt64"))
      }

      override def transpose(numberOfItems: Int, columns: Array[Column]) = {
        new Iterator[Foo] {
          val xs = columns(0).data.asInstanceOf[Array[Long]]
          var i = 0
          val addition = BigDecimal(Long.MaxValue) + BigDecimal(Long.MaxValue) + 2

          override def hasNext = i < numberOfItems

          override def next() = {
            val l = xs(i)
            val res = Foo(
              if (l >= 0) BigDecimal(l)
              else BigDecimal(l) + addition
            )
            i += 1
            res
          }
        }
      }
    }

    val forCreate = "create table test_uint64(x UInt64) engine = Memory;"
    stmt.executeUpdate(forCreate)

    val forInsert = "insert into test_uint64(x) values (?)"
    val ps = conn.prepareStatement(forInsert)
    val rnd = new Random()
    for (_ <- 1 to ROWS_NUMBER) {
      ps.setString(1, BigInt(Random.nextInt(64), rnd).toString)
      ps.addBatch()
    }
    ps.executeBatch()
    conn.commit()

    val javaRes = new Array[BigDecimal](ROWS_NUMBER)
    val scalaRes = new Array[BigDecimal](ROWS_NUMBER)
    var javaTime = 0L
    var scalaTime = 0L
    val sql = "select * from test_uint64 limit " + ROWS_NUMBER

    for (_ <- 1 to ITERATIONS) {
      var now = System.currentTimeMillis
      var j = 0

      val rs = stmt.executeQuery(sql)
      while (rs.next) {
        javaRes(j) = rs.getBigDecimal("x")
        j += 1
      }
      rs.close()
      javaTime += System.currentTimeMillis - now

      now = System.currentTimeMillis
      val it = scalaClient.execute[Foo](sql, scalaClickhouseProperties)
      j = 0
      while (it.hasNext) {
        scalaRes(j) = it.next.x
        j += 1
      }
      scalaTime += System.currentTimeMillis - now

      assert(javaRes.sameElements(scalaRes))
    }

    println(s"Total for table [$forCreate] and $ITERATIONS iterations: scala=$scalaTime, java=$javaTime")
  }

  @Test
  def testSelectString(): Unit = {
    case class Foo(x: String)

    implicit val FooDecoder = new Decoder[Foo] {
      override def validate(names: Array[String], types: Array[String]): Boolean = {
        names.sameElements(Array("x")) &&
        types.sameElements(Array("String"))
      }

      override def transpose(numberOfItems: Int, columns: Array[Column]): Iterator[Foo] = {
        val xs = new Array[String](numberOfItems)
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

    val forCreate = "create table test_string(x String) engine = Memory;"
    stmt.executeUpdate(forCreate)

    val forInsert = "insert into test_string(x) values (?)"
    val ps = conn.prepareStatement(forInsert)
    for (_ <- 1 to ROWS_NUMBER) {
      ps.setString(1, Random.nextString(Random.nextInt(16)))
      ps.addBatch()
    }
    ps.executeBatch()
    conn.commit()

    val javaRes = new Array[String](ROWS_NUMBER)
    val scalaRes = new Array[String](ROWS_NUMBER)
    var javaTime = 0L
    var scalaTime = 0L
    val sql = "select * from test_string limit " + ROWS_NUMBER

    for (_ <- 1 to ITERATIONS) {
      var now = System.currentTimeMillis
      var j = 0

      val rs = stmt.executeQuery(sql)
      while (rs.next) {
        javaRes(j) = rs.getString("x")
        j += 1
      }
      rs.close()
      javaTime += System.currentTimeMillis - now

      now = System.currentTimeMillis
      val it = scalaClient.execute[Foo](sql, scalaClickhouseProperties)
      j = 0
      while (it.hasNext) {
        scalaRes(j) = it.next.x
        j += 1
      }
      scalaTime += System.currentTimeMillis - now

      assert(javaRes.sameElements(scalaRes))
    }

    println(s"Total for table [$forCreate] and $ITERATIONS iterations: scala=$scalaTime, java=$javaTime")
  }

  @Test
  def testSelectFixedString42(): Unit = {
    case class Foo(x: String)

    implicit val FooDecoder = new Decoder[Foo] {
      override def validate(names: Array[String], types: Array[String]): Boolean = {
        names.sameElements(Array("x")) &&
          types.sameElements(Array("FixedString(42)"))
      }

      override def transpose(numberOfItems: Int, columns: Array[Column]): Iterator[Foo] = {
        val xs = new Array[String](numberOfItems)
        System.arraycopy(columns(0).data, 0, xs, 0, numberOfItems)

        new Iterator[Foo] {
          var i = 0
          override def hasNext = i < numberOfItems

          override def next() = {
            // jdbc does not skip '\0', so this implementation does the same
            // of course, user is free to provide own implementation
            val res = Foo(xs(i))
            i += 1
            res
          }
        }
      }
    }

    val forCreate = "create table test_fixed_string_42(x FixedString(42)) engine = Memory;"
    stmt.executeUpdate(forCreate)

    val forInsert = "insert into test_fixed_string_42(x) values (?)"
    val ps = conn.prepareStatement(forInsert)
    for (_ <- 1 to ROWS_NUMBER) {
      ps.setString(1, Random.nextString(Random.nextInt(6)))
      ps.addBatch()
    }
    ps.executeBatch()
    conn.commit()

    val javaRes = new Array[String](ROWS_NUMBER)
    val scalaRes = new Array[String](ROWS_NUMBER)
    var javaTime = 0L
    var scalaTime = 0L
    val sql = "select * from test_fixed_string_42 limit " + ROWS_NUMBER

    for (_ <- 1 to ITERATIONS) {
      var now = System.currentTimeMillis
      var j = 0

      val rs = stmt.executeQuery(sql)
      while (rs.next) {
        javaRes(j) = rs.getString("x")
        j += 1
      }
      rs.close()
      javaTime += System.currentTimeMillis - now

      now = System.currentTimeMillis
      val it = scalaClient.execute[Foo](sql, scalaClickhouseProperties)
      j = 0
      while (it.hasNext) {
        scalaRes(j) = it.next.x
        j += 1
      }
      scalaTime += System.currentTimeMillis - now

      assert(javaRes.sameElements(scalaRes))
    }

    println(s"Total for table [$forCreate] and $ITERATIONS iterations: scala=$scalaTime, java=$javaTime")
  }
}
