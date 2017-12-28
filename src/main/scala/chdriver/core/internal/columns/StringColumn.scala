package chdriver.core.internal.columns

import java.io.DataOutputStream

import chdriver.core.columns.Column
import chdriver.core.internal.Protocol

class StringColumn private[chdriver] (_data: Array[String]) extends Column {
  override type T = String
  override val data = _data

  override def writeTo(out: DataOutputStream, toRow: Int): Unit = {
    import Protocol.DataOutputStreamOps

    var i = 0
    while (i < toRow) {
      out.writeString(data(i))
      i += 1
    }
  }
}

// jdbc does not skip padding '\0', so we don't too, but user is free to provide own implementation of Decoder.
class FixedStringColumn private[chdriver] (_data: Array[String], itemLength: Int) extends StringColumn(_data) {
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
