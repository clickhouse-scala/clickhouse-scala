package chdriver.core.columns

import java.io.{DataInputStream, DataOutputStream}

class NullableColumn private[columns] (val nulls: Array[Byte], val inner: Column) extends Column {
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

object NullableColumn {
  def apply(inner: Column) = new NullableColumn(Array.fill[Byte](inner.data.length)(1), inner)

  def from(in: DataInputStream, itemsNumber: Int, innerType: String): NullableColumn = {
    val nulls = new Array[Byte](itemsNumber)
    in.readFully(nulls)
    val inner = Column.from(in, itemsNumber, innerType)
    val result = new Array[Any](itemsNumber)
    var i = 0
    while (i < itemsNumber) {
      result(i) =
        if (nulls(i) == 0) inner.data(i)
        else null
      i += 1
    }
    new NullableColumn(nulls, inner)
  }
}
