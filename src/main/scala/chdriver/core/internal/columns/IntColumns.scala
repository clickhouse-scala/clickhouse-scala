package chdriver.core.internal.columns

import java.io.DataOutputStream

import chdriver.core.columns.Column
import chdriver.core.internal.Protocol.DataOutputStreamOps

class Int8Column private[chdriver] (_data: Array[Byte]) extends Column {
  override type T = Byte
  override val data = _data

  override def writeTo(out: DataOutputStream, toRow: Int): Unit = {
    var i = 0
    while (i < toRow) {
      out.writeByte(data(i))
      i += 1
    }
  }
}

class Int16Column private[chdriver] (_data: Array[Short]) extends Column {
  override type T = Short
  override val data = _data

  override def writeTo(out: DataOutputStream, toRow: Int): Unit = {
    var i = 0
    while (i < toRow) {
      out.writeInt16(data(i))
      i += 1
    }
  }
}

class Int32Column private[chdriver] (_data: Array[Int]) extends Column {
  override type T = Int
  override val data = _data

  override def writeTo(out: DataOutputStream, toRow: Int): Unit = {
    var i = 0
    while (i < toRow.min(data.length)) {
      out.writeInt32(data(i))
      i += 1
    }
  }
}

class Int64Column private[chdriver] (_data: Array[Long]) extends Column {
  override type T = Long
  override val data = _data

  override def writeTo(out: DataOutputStream, toRow: Int): Unit = {
    var i = 0
    while (i < toRow) {
      out.writeInt64(data(i))
      i += 1
    }
  }
}
