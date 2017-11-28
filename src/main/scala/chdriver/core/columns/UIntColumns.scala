package chdriver.core.columns

import java.io.DataInputStream

/**
  * Beware of negative bytes, use `data(i) & 0xFF`.
  */
class UInt8Column(_data: Array[Byte]) extends Column {
  override type T = Byte
  override val data: Array[Byte] = _data
}

object UInt8Column {
  def readAllFrom(in: DataInputStream, itemsNumber: Int): UInt8Column = {
    new UInt8Column(Int8Column.readAllFrom(in, itemsNumber).data)
  }
}

/**
  * Beware of negative shorts, use `data(i) & 0xFFFF`.
  */
class UInt16Column(_data: Array[Short]) extends Column {
  override type T = Short
  override val data: Array[Short] = _data
}

object UInt16Column {
  def readAllFrom(in: DataInputStream, itemsNumber: Int): UInt16Column = {
    new UInt16Column(Int16Column.readAllFrom(in, itemsNumber).data)
  }
}


/**
  * Beware of negative ints, use `data(i) & 0xFFFFFFFFL`.
  */
class UInt32Column(_data: Array[Int]) extends Column {
  override type T = Int
  override val data: Array[Int] = _data
}

object UInt32Column {
  def readAllFrom(in: DataInputStream, itemsNumber: Int): UInt32Column = {
    new UInt32Column(Int32Column.readAllFrom(in, itemsNumber).data)
  }
}

/**
  * Beware of negative longs.
  */
class UInt64Column(_data: Array[Long]) extends Column {
  override type T = Long
  override val data: Array[Long] = _data
}

object UInt64Column {
  def readAllFrom(in: DataInputStream, itemsNumber: Int): UInt64Column = {
    new UInt64Column(Int64Column.readAllFrom(in, itemsNumber).data)
  }
}
