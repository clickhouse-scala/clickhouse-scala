package chdriver.core.columns

import java.io.{DataInputStream, DataOutputStream}

import chdriver.core.DriverProperties.DEFAULT_INSERT_BLOCK_SIZE

/**
 * Beware of negative bytes, use `data(i) & 0xFF`.
 */
class UInt8Column private[columns] (_data: Array[Byte]) extends Column {
  override type T = Byte
  override val data: Array[Byte] = _data

  override def writeTo(out: DataOutputStream, toRow: Int): Unit = {
    new Int8Column(data).writeTo(out, toRow)
  }
}

object UInt8Column {
  def apply() = new UInt8Column(new Array[Byte](DEFAULT_INSERT_BLOCK_SIZE))

  def from(in: DataInputStream, itemsNumber: Int): UInt8Column = {
    new UInt8Column(Int8Column.from(in, itemsNumber).data)
  }
}

/**
 * Beware of negative shorts, use `data(i) & 0xFFFF`.
 */
class UInt16Column private[columns] (_data: Array[Short]) extends Column {
  override type T = Short
  override val data: Array[Short] = _data

  override def writeTo(out: DataOutputStream, toRow: Int): Unit = {
    new Int16Column(data).writeTo(out, toRow)
  }
}

object UInt16Column {
  def apply() = new UInt16Column(new Array[Short](DEFAULT_INSERT_BLOCK_SIZE))

  def from(in: DataInputStream, itemsNumber: Int): UInt16Column = {
    new UInt16Column(Int16Column.from(in, itemsNumber).data)
  }
}

/**
 * Beware of negative ints, use `data(i) & 0xFFFFFFFFL`.
 */
class UInt32Column private[columns] (_data: Array[Int]) extends Column {
  override type T = Int
  override val data: Array[Int] = _data

  override def writeTo(out: DataOutputStream, toRow: Int): Unit = {
    new Int32Column(data).writeTo(out, toRow)
  }
}

object UInt32Column {
  def apply() = new UInt32Column(new Array[Int](DEFAULT_INSERT_BLOCK_SIZE))

  def from(in: DataInputStream, itemsNumber: Int): UInt32Column = {
    new UInt32Column(Int32Column.from(in, itemsNumber).data)
  }
}

/**
 * Beware of negative longs.
 */
class UInt64Column private[columns] (_data: Array[Long]) extends Column {
  override type T = Long
  override val data: Array[Long] = _data

  override def writeTo(out: DataOutputStream, toRow: Int): Unit = {
    new Int64Column(data).writeTo(out, toRow)
  }
}

object UInt64Column {
  def apply() = new UInt64Column(new Array[Long](DEFAULT_INSERT_BLOCK_SIZE))

  def from(in: DataInputStream, itemsNumber: Int): UInt64Column = {
    new UInt64Column(Int64Column.from(in, itemsNumber).data)
  }
}
