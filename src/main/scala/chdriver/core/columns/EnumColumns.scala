package chdriver.core.columns

import java.io.DataInputStream

import chdriver.core.DriverException

class Enum8Column(_data: Array[String]) extends Column {
  override type T = String
  override val data = _data
}

object Enum8Column {
  final val p = "'(.*?)' *= *([-0-9]+)".r

  def readAllFrom(in: DataInputStream, itemsNumber: Int, enum: String): Enum8Column = {
    val mapping = p.findAllIn(enum).map { case p(v, k) => k.toByte -> v }.toMap
    val keys = Int8Column.readAllFrom(in, itemsNumber).data
    val result = new Array[String](itemsNumber)
    var i = 0
    while (i < itemsNumber) {
      result(i) = mapping.getOrElse(
        keys(i),
        throw new DriverException(s"Cannot find value for key = [${keys(i)}], whole mapping = [$mapping].")
      )
      i += 1
    }
    new Enum8Column(result)
  }
}

class Enum16Column(_data: Array[String]) extends Column {
  override type T = String
  override val data = _data
}

object Enum16Column {
  final val p = "'(.*?)' *= *([-0-9]+)".r

  def readAllFrom(in: DataInputStream, itemsNumber: Int, enum: String): Enum16Column = {
    val mapping = p.findAllIn(enum).map { case p(v, k) => k.toShort -> v }.toMap
    val keys = Int16Column.readAllFrom(in, itemsNumber).data
    val result = new Array[String](itemsNumber)
    var i = 0
    while (i < itemsNumber) {
      result(i) = mapping.getOrElse(
        keys(i),
        throw new DriverException(s"Cannot find value for key = [${keys(i)}], whole mapping = [$mapping].")
      )
      i += 1
    }
    new Enum16Column(result)
  }
}
