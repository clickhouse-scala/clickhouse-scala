package chdriver.core.internal.columns

import java.io.DataOutputStream

import chdriver.core.columns.Column
import chdriver.core.internal.Protocol.DataOutputStreamOps

class Float32Column private[chdriver] (_data: Array[Float]) extends Column {
  override type T = Float
  override val data = _data

  override def writeTo(out: DataOutputStream, toRow: Int): Unit = {
    var i = 0
    while (i < toRow) {
      val int = java.lang.Float.floatToIntBits(data(i))
      out.writeInt32(int)
      i += 1
    }
  }
}

class Float64Column private[chdriver] (_data: Array[Double]) extends Column {
  override type T = Double
  override val data = _data

  override def writeTo(out: DataOutputStream, toRow: Int): Unit = {
    var i = 0
    while (i < toRow) {
      val long = java.lang.Double.doubleToLongBits(data(i))
      out.writeInt64(long)
      i += 1
    }
  }
}
