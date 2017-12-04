package chdriver.core.columns

import java.io.DataInputStream

class StringColumn(_data: Array[String]) extends Column {
  override type T = String
  override val data = _data
}

object StringColumn {
  import chdriver.core.Protocol.DataInputStreamOps

  def readAllFrom(in: DataInputStream, itemsNumber: Int): StringColumn = {
    val result = new Array[String](itemsNumber)
    var i = 0
    while (i < itemsNumber) {
      result(i) = in.readString()
      i += 1
    }
    new StringColumn(result)
  }
}

/**
 * jdbc does not skip padding '\0', so we don't too, but user is free to provide own implementation of Decoder.
 */
class FixedStringColumn(_data: Array[String], itemLength: Int) extends Column {
  override type T = String
  override val data = _data
}

object FixedStringColumn {
  def readAllFrom(in: DataInputStream, itemsNumber: Int, itemLength: Int): FixedStringColumn = {
    // todo advanced_functionality optimization somehow use knowledge of fixed string size; why FixedString is so much slower?
    val result = new Array[String](itemsNumber)
    var i = 0
    while (i < itemsNumber) {
      val item = new Array[Byte](itemLength)
      in.readFully(item)
      result(i) = new String(item, "UTF-8")
      i += 1
    }
    new FixedStringColumn(result, itemLength)
  }
}
