package chdriver.core.instances

import java.time._

import chdriver.core.Decoder
import chdriver.core.columns.Column

import scala.reflect.ClassTag

/**
  * @author andr83
  */
trait DecoderInstances {
  def byteDecoder(colNumber: Int): Decoder[Byte] = new Decoder[Byte] {
    override def validate(names: Array[String], types: Array[String]) = true

    override def transpose(numberOfItems: Int, columns: Array[Column]): Iterator[Byte] = {
      val xs = new Array[Byte](numberOfItems)
      System.arraycopy(columns(0).data, 0, xs, 0, numberOfItems)
      xs.iterator
    }
  }

  implicit val byteDecoder: Decoder[Byte] = byteDecoder(0)

  implicit val shortDecoder: Decoder[Short] = new Decoder[Short] {
    override def validate(names: Array[String], types: Array[String]) = true

    override def transpose(numberOfItems: Int, columns: Array[Column]): Iterator[Short] = {
      val xs = new Array[Short](numberOfItems)
      System.arraycopy(columns(0).data, 0, xs, 0, numberOfItems)
      xs.iterator
    }
  }

  implicit val intDecoder: Decoder[Int] = new Decoder[Int] {
    override def validate(names: Array[String], types: Array[String]) = true

    override def transpose(numberOfItems: Int, columns: Array[Column]): Iterator[Int] = {
      val xs = new Array[Int](numberOfItems)
      System.arraycopy(columns(0).data, 0, xs, 0, numberOfItems)
      xs.iterator
    }
  }

  implicit val longDecoder: Decoder[Long] = new Decoder[Long] {
    override def validate(names: Array[String], types: Array[String]) = true

    override def transpose(numberOfItems: Int, columns: Array[Column]): Iterator[Long] = {
      val xs = new Array[Long](numberOfItems)
      System.arraycopy(columns(0).data, 0, xs, 0, numberOfItems)
      xs.iterator
    }
  }

  implicit val floatDecoder: Decoder[Float] = new Decoder[Float] {
    override def validate(names: Array[String], types: Array[String]) = true

    override def transpose(numberOfItems: Int, columns: Array[Column]): Iterator[Float] = {
      val xs = new Array[Float](numberOfItems)
      System.arraycopy(columns(0).data, 0, xs, 0, numberOfItems)
      xs.iterator
    }
  }

  implicit val doubleDecoder: Decoder[Double] = new Decoder[Double] {
    override def validate(names: Array[String], types: Array[String]) = true

    override def transpose(numberOfItems: Int, columns: Array[Column]): Iterator[Double] = {
      val xs = new Array[Double](numberOfItems)
      System.arraycopy(columns(0).data, 0, xs, 0, numberOfItems)
      xs.iterator
    }
  }

  implicit val stringDecoder: Decoder[String] = new Decoder[String] {
    override def validate(names: Array[String], types: Array[String]) = true

    override def transpose(numberOfItems: Int, columns: Array[Column]): Iterator[String] = {
      columns(0).data.asInstanceOf[Array[String]].iterator
    }
  }

  implicit val localDateDecoder: Decoder[LocalDate] = new Decoder[LocalDate] {
    private val epochStart = LocalDate.ofEpochDay(0)

    override def validate(names: Array[String], types: Array[String]) = true

    override def transpose(numberOfItems: Int, columns: Array[Column]): Iterator[LocalDate] = {
      val dates = columns(0).data.asInstanceOf[Array[Short]]
      new Iterator[LocalDate] {
        private var i = -1
        private val upperBound = numberOfItems - 1

        override def hasNext: Boolean = i < upperBound

        override def next(): LocalDate = {
          i += 1
          epochStart.plusDays(dates(i))
        }
      }
    }
  }

  implicit val localDateTimeDecoder: Decoder[LocalDateTime] = new Decoder[LocalDateTime] {
    private val zoneOffset = LocalDateTime.now().atZone(ZoneId.systemDefault()).getOffset // todo: Take ZoneOffset from configuration

    override def validate(names: Array[String], types: Array[String]) = true

    override def transpose(numberOfItems: Int, columns: Array[Column]): Iterator[LocalDateTime] = {
      val ts = columns(0).data.asInstanceOf[Array[Int]]
      new Iterator[LocalDateTime] {
        private var i = -1
        private val upperBound = numberOfItems - 1

        override def hasNext: Boolean = i < upperBound

        override def next(): LocalDateTime = {
          i += 1
          LocalDateTime.ofEpochSecond(ts(i), 0, zoneOffset)
        }
      }
    }
  }

  implicit val instantDecoder: Decoder[Instant] = new Decoder[Instant] {
    override def validate(names: Array[String], types: Array[String]) = true

    override def transpose(numberOfItems: Int, columns: Array[Column]): Iterator[Instant] = {
      val ts = columns(0).data.asInstanceOf[Array[Int]]
      new Iterator[Instant] {
        private var i = -1
        private val upperBound = numberOfItems - 1

        override def hasNext: Boolean = i < upperBound

        override def next(): Instant = {
          i += 1
          Instant.ofEpochSecond(ts(i))
        }
      }
    }
  }

  implicit def arrayDecoder[A : ClassTag]: Decoder[Array[A]] = new Decoder[Array[A]] {
    override def validate(names: Array[String], types: Array[String]) = true

    override def transpose(numberOfItems: Int, columns: Array[Column]): Iterator[Array[A]] = {
      val arr = columns(0).data.asInstanceOf[Array[Array[A]]]
      arr.iterator
    }
  }
}
