package chdriver.core.internal.columns

import java.io.DataOutputStream

import chdriver.core.columns.Column

/**
 * Beware of negative bytes, use `data(i) & 0xFF`.
 */
class UInt8Column private[chdriver] (_data: Array[Byte]) extends Column {
  override type T = Byte
  override val data: Array[Byte] = _data

  override def writeTo(out: DataOutputStream, toRow: Int): Unit = {
    new Int8Column(data).writeTo(out, toRow)
  }
}

/**
 * Beware of negative shorts, use `data(i) & 0xFFFF`.
 */
class UInt16Column private[chdriver] (_data: Array[Short]) extends Column {
  override type T = Short
  override val data: Array[Short] = _data

  override def writeTo(out: DataOutputStream, toRow: Int): Unit = {
    new Int16Column(data).writeTo(out, toRow)
  }
}

/**
 * Beware of negative ints, use `data(i) & 0xFFFFFFFFL`.
 */
class UInt32Column private[chdriver] (_data: Array[Int]) extends Column {
  override type T = Int
  override val data: Array[Int] = _data

  override def writeTo(out: DataOutputStream, toRow: Int): Unit = {
    new Int32Column(data).writeTo(out, toRow)
  }
}

/**
 * Beware of negative longs.
 */
class UInt64Column private[chdriver] (_data: Array[Long]) extends Column {
  override type T = Long
  override val data: Array[Long] = _data

  override def writeTo(out: DataOutputStream, toRow: Int): Unit = {
    new Int64Column(data).writeTo(out, toRow)
  }
}
