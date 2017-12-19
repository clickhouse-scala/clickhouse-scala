package chdriver.test.specs

import java.time.{Instant, LocalDate, LocalDateTime}

import chdriver.core.Decoder
import chdriver.core.implicits._
import chdriver.test.implicits._
import chdriver.test.{ClickhouseContainer, ResultSetDecoder}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.reflect.ClassTag

/**
  * @author andr83
  */
class RespondTypesSpec
    extends FlatSpec
    with Matchers
    with ClickhouseContainer
    with BeforeAndAfterAll {
  val DefaultLimit = 1000

  def checkType[A: Decoder: ResultSetDecoder: ClassTag](
      colName: String): Unit = {
    withClickhouse { (client, jdbc) =>
      val sql = s"select $colName from test limit $DefaultLimit"
      val driverRes = client.execute[A](sql).toArray
      val jdbcRes = jdbc.execute[A](sql)
      driverRes should equal(jdbcRes)
    }
  }

  "Clickhouse client" should "return Int8 field" in {
    checkType[Byte]("int8")
  }

  it should "return Int16 field" in {
    checkType[Short]("int16")
  }

  it should "return Int32 field" in {
    checkType[Int]("int32")
  }

  it should "return Int64 field" in {
    checkType[Long]("int64")
  }

  it should "return Float32 field" in {
    checkType[Float]("float32")
  }

  it should "return Float64 field" in {
    checkType[Double]("float64")
  }

  it should "return String field" in {
    checkType[String]("text")
  }

  it should "return FixedString field" in {
    checkType[String]("fixed_string")
  }

  it should "return Date field" in {
    checkType[LocalDate]("date")
  }

  it should "return Instant field" in {
    checkType[Instant]("date_time")
  }

  it should "return Array[Int32] field" in {
    checkType[Array[Int]]("array_int32")
  }

  override protected def beforeAll(): Unit = {
    startClickhouse()
    initClickhouse()
  }

  override protected def afterAll(): Unit = {
    stopClickhouse()
  }
}
