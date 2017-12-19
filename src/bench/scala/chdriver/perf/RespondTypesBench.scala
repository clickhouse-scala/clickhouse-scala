package chdriver.perf


import chdriver.core.Decoder
import chdriver.core.implicits._
import chdriver.test.{ClickhouseContainer, ResultSetDecoder}
import chdriver.test.implicits._
import org.scalameter.api._
import org.scalameter.picklers.Implicits._

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
  * @author andr83
  */
object RespondTypesBench extends Bench.LocalTime with ClickhouseContainer {
  startClickhouse()
  initClickhouse()

  val sizes: Gen[Int] = Gen.enumeration("size")(1000, 100000, 1000000)

  val ranges = for {
    size <- sizes
  } yield 0 until size

  performance of "Range" in {
    measure method "map" in {
      using(ranges) in {
        r => r.map(_ + 1)
      }
    }
  }

  def bench[A : ClassTag : TypeTag : Decoder : ResultSetDecoder](field: String) = {
    withClickhouse { (client, jdbc) =>
      performance of s"Reading ${typeTag[A].tpe} data" in {
        measure method "Client" in {
          using(sizes) config (
            exec.maxWarmupRuns -> 5,
            exec.benchRuns -> 5
          ) in { size =>
            val sql = s"select $field from test limit $size"
            client.execute[A](sql).toArray
          }
        }

        measure method "Jdbc" in {
          using(sizes) config (
            exec.maxWarmupRuns -> 1,
            exec.benchRuns -> 2
          ) in { size =>
            val sql = s"select $field from test limit $size"
            jdbc.execute[A](sql)
          }
        }
      }
    }
  }

  bench[Byte]("int8")
  bench[Short]("int16")
  bench[Int]("int32")
  bench[Long]("int64")
  bench[String]("text")
  bench[Array[Int]]("array_int32")
}
