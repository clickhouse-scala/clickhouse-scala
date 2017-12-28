package chdriver.core.internal.columns

import java.io.DataOutputStream

import chdriver.core.columns.Column

class NullableColumn private[chdriver] (val nulls: Array[Byte], val inner: Column) extends Column {
  override type T = inner.T
  override val data: Array[inner.T] = inner.data

  override def writeTo(out: DataOutputStream, toRow: Int): Unit = {
    var i = 0
    while (i < toRow) {
      out.writeBoolean(nulls(i) == 0)
      i += 1
    }

    inner.writeTo(out, toRow)
  }

  override def chType: String = super.chType + "(" + inner.chType + ")"
}
