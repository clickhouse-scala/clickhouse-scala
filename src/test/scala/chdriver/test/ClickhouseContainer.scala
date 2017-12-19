package chdriver.test

import java.sql.{Connection => SQLConnection, Array => _, _}
import java.time.{Instant, LocalDate}
import java.util.logging.Logger

import chdriver.core.{Client, Connection, DriverProperties}
import org.testcontainers.containers.{BindMode, GenericContainer}

import scala.util.Random

/**
  * @author andr83
  */
trait ClickhouseContainer {
  private[this] val log: Logger = Logger.getLogger(this.getClass.getName)

  private lazy val clickhouseContainer: GenericContainer[_] = {
    val container = new GenericContainer("yandex/clickhouse-server:latest")
    container.withExposedPorts(8123, 9000)
    container.addFileSystemBind(getClass.getResource("/clickhouse-config.xml").getFile, "/etc/clickhouse-server/config.xml", BindMode.READ_ONLY)
    container
  }

  def startClickhouse(): Unit = clickhouseContainer.start()

  def stopClickhouse(): Unit = clickhouseContainer.stop()

  def initClickhouse(): Unit = withClickhouse {(_, conn) =>
    val stmt = conn.createStatement()
    log.info("Creating test Clickhouse table.")
    stmt.execute(
      """
        |CREATE TABLE test(
        |  int8 Int8,
        |  int16 Int16,
        |  int32 Int32,
        |  int64 Int64,
        |  uint8 UInt8,
        |  uint16 UInt16,
        |  uint32 UInt32,
        |  uint64 UInt64,
        |  float32 Float32,
        |  float64 Float64,
        |  text String,
        |  fixed_string FixedString(42),
        |  date Date,
        |  date_time DateTime,
        |  array_int32 Array(Int32)
        |) ENGINE = Memory;
      """.stripMargin)

//    stmt.execute(
//      """
//        |CREATE TABLE test(
//        |  int8 Int8,
//        |  int16 Int16,
//        |  int32 Int32,
//        |  int64 Int64,
//        |  uint8 UInt8,
//        |  uint16 UInt16,
//        |  uint32 UInt32,
//        |  uint64 UInt64,
//        |  float32 Float32,
//        |  float64 Float64,
//        |  string String,
//        |  fixed_string FixedString(42),
//        |  date Date,
//        |  date_time DateTime
//        |) ENGINE = Memory;
//      """.stripMargin)

    val rnd = new Random()
    val forInsert = "insert into test values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    val ps = conn.prepareStatement(forInsert)

    val epochStart = LocalDate.ofEpochDay(0)
    log.info("Filling test table with 1000000 records.")

    for (_ <- 1 to 1000000) {
      var i = 0
      def nextI(): Int = {
        i += 1
        i
      }
      ps.setByte(nextI(), Random.nextInt().toByte)
      ps.setShort(nextI(), Random.nextInt().toShort)
      ps.setInt(nextI(), Random.nextInt())
      ps.setLong(nextI(), Random.nextLong())
      ps.setInt(nextI(), Random.nextInt().toByte & 0xFF)
      ps.setInt(nextI(), Random.nextInt().toShort & 0xFFFF)
      ps.setLong(nextI(), Random.nextInt() & 0xFFFFFFFFL)
      ps.setString(nextI(), BigInt(Random.nextInt(64), rnd).toString)
      ps.setFloat(nextI(), Random.nextFloat())
      ps.setDouble(nextI(), Random.nextDouble())
      ps.setString(nextI(), Random.nextString(Random.nextInt(16)))
      ps.setString(nextI(), Random.nextString(Random.nextInt(10)))
      ps.setDate(nextI(), Date.valueOf(epochStart.plusDays(1 + Random.nextInt(24836)).toString)) // days for 2037-12-31
      ps.setTimestamp(nextI(), Timestamp.from(Instant.ofEpochSecond(1 + Math.abs(Random.nextLong() % 2145916799L)))) // seconds for 2037-12-31T23:59:59
      ps.setArray(nextI(), conn.createArrayOf("Int32", Array.fill[AnyRef](Random.nextInt(100))(Random.nextInt().asInstanceOf[AnyRef])))

      ps.addBatch()
    }
    ps.executeBatch()
    conn.commit()

    stmt.close()
    conn.commit()
  }

  def withClickhouse(block: (Client, SQLConnection) => Unit): Unit = {
    val httpPort = clickhouseContainer.getMappedPort(8123)
    val tcpPort = clickhouseContainer.getMappedPort(9000)
    Class.forName("ru.yandex.clickhouse.ClickHouseDriver")
    val jdbcConn = DriverManager.getConnection(s"jdbc:clickhouse://localhost:$httpPort", "default", "")
    jdbcConn.setAutoCommit(false)

    val client = new Client(
      DriverProperties.DEFAULT_INSERT_BLOCK_SIZE,
      new Connection(
        host = "localhost",
        port = tcpPort,
        clientName = "test",
        database = "default",
        user = "default",
        password = ""
      )
    )
    block(client, jdbcConn)
  }
}