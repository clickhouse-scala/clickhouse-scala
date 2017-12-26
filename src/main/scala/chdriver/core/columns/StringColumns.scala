package chdriver.core.columns

import java.io.{DataInputStream, DataOutputStream}

import chdriver.core.DriverProperties.DEFAULT_INSERT_BLOCK_SIZE

class StringColumn private[columns] (_data: Array[String]) extends Column {
  override type T = String
  override val data = _data

  override def writeTo(out: DataOutputStream, toRow: Int): Unit = {
    import chdriver.core.Protocol.DataOutputStreamOps

    var i = 0
    while (i < toRow) {
      out.writeString(data(i))
      i += 1
    }
  }
}

object StringColumn {
  import chdriver.core.Protocol.DataInputStreamOps

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

// jdbc does not skip padding '\0', so we don't too, but user is free to provide own implementation of Decoder.
class FixedStringColumn private[columns] (_data: Array[String], itemLength: Int) extends StringColumn(_data) {
  override def writeTo(out: DataOutputStream, toRow: Int): Unit = {
    val suffix = new Array[Byte](itemLength)
    var i = 0
    while (i < toRow) {
      val row = data(i)
      require(row.length <= itemLength)
      val diff = itemLength - row.length
      out.write(data(i).getBytes("UTF-8"))
      out.write(suffix, 0, diff)
      i += 1
    }
  }

  override def chType: String = super.chType + s"($itemLength)"
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
