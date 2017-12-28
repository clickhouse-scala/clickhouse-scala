package chdriver.core.columns

import java.io.DataInputStream

import chdriver.core.DriverProperties.DEFAULT_INSERT_BLOCK_SIZE
import chdriver.core.internal.columns.{FixedStringColumn, StringColumn}

object StringColumn {
  import chdriver.core.internal.Protocol.DataInputStreamOps

  def apply() = new StringColumn(new Array[String](DEFAULT_INSERT_BLOCK_SIZE))

  def from(in: DataInputStream, itemsNumber: Int): StringColumn = {
    val result = new Array[String](itemsNumber)
    var i = 0
    while (i < itemsNumber) {
      result(i) = in.readString()
      i += 1
    }
    new StringColumn(result)
  }
}

object FixedStringColumn {
  def apply(itemLength: Int) = new FixedStringColumn(new Array[String](DEFAULT_INSERT_BLOCK_SIZE), itemLength)

  def from(in: DataInputStream, itemsNumber: Int, itemLength: Int): FixedStringColumn = {
    val buffer = new Array[Byte](itemLength)
    val result = new Array[String](itemsNumber)
    var i = 0
    while (i < itemsNumber) {
      in.readFully(buffer)
      result(i) = new String(buffer, "UTF-8")
      i += 1
    }
    new FixedStringColumn(result, itemLength)
  }
}
