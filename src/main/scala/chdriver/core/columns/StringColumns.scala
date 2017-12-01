package chdriver.core.columns

import java.io.DataInputStream
import java.util.Arrays

class StringColumn(_data: Array[String]) extends Column {
  override type T = String
  override val data = _data
}

object StringColumn {
  import chdriver.core.Protocol.DataInputStreamOps

  def readAllFrom(in: DataInputStream, itemsNumber: Int): StringColumn = {
    val data = new Array[String](itemsNumber)
    var i = 0
    while (i < itemsNumber) {
      data(i) = in.readString()
      i += 1
    }
    new StringColumn(data)
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
    val data = new Array[Byte](itemsNumber * itemLength)
    in.readFully(data)
    val result = new Array[String](itemsNumber)
    var i = 0
    while (i < itemsNumber * itemLength) {
      result(i / itemLength) = new String(Arrays.copyOfRange(data, i, i + itemLength), "UTF-8")
      i += itemLength
    }
    new FixedStringColumn(result, itemLength)
  }
}
