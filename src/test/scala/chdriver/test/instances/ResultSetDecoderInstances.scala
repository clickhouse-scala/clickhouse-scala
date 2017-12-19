package chdriver.test.instances

import java.time.{Instant, LocalDate, LocalDateTime}

import chdriver.test.ResultSetDecoder

/**
  * @author andr83
  */
trait ResultSetDecoderInstances {
  import ResultSetDecoder.pure

  implicit val rsByteDecoder: ResultSetDecoder[Byte] = pure(rs=> rs.getByte(1))
  implicit val rsShortDecoder: ResultSetDecoder[Short] = pure(rs=> rs.getShort(1))
  implicit val rsIntDecoder: ResultSetDecoder[Int] = pure(rs=> rs.getInt(1))
  implicit val rsLongDecoder: ResultSetDecoder[Long] = pure(rs=> rs.getLong(1))
  implicit val rsFloatDecoder: ResultSetDecoder[Float] = pure(rs=> rs.getFloat(1))
  implicit val rsDoubleDecoder: ResultSetDecoder[Double] = pure(rs=> rs.getDouble(1))
  implicit val rsStringDecoder: ResultSetDecoder[String] = pure(rs=> rs.getString(1))
  implicit val rsLocalDateDecoder: ResultSetDecoder[LocalDate] = pure(rs=> rs.getDate(1).toLocalDate)
  implicit val rsLocalDateTimeDecoder: ResultSetDecoder[LocalDateTime] = pure(rs=> rs.getTimestamp(1).toLocalDateTime)
  implicit val rsInstantDecoder: ResultSetDecoder[Instant] = pure(rs=> rs.getTimestamp(1).toInstant)
  implicit val rsInt32ArrayDecoder: ResultSetDecoder[Array[Int]] = pure(rs=> rs.getArray(1).getArray.asInstanceOf[Array[Int]])
}
