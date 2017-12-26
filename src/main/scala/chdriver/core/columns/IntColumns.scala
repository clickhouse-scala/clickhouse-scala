package chdriver.core.columns

import java.io.{DataInputStream, DataOutputStream}

import chdriver.core.DriverProperties.DEFAULT_INSERT_BLOCK_SIZE
import chdriver.core.Protocol.DataInputStreamOps
import chdriver.core.Protocol.DataOutputStreamOps

class Int8Column private[columns] (_data: Array[Byte]) extends Column {
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

object Int8Column {
  def apply() = new Int8Column(new Array[Byte](DEFAULT_INSERT_BLOCK_SIZE))

  def from(in: DataInputStream, itemsNumber: Int): Int8Column = {
    val result = new Array[Byte](itemsNumber)
    in.readFully(result)
    new Int8Column(result)
  }
}

class Int16Column private[columns] (_data: Array[Short]) extends Column {
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

object Int16Column {
  def apply() = new Int16Column(new Array[Short](DEFAULT_INSERT_BLOCK_SIZE))

  def from(in: DataInputStream, itemsNumber: Int): Int16Column = {
    val data = new Array[Byte](itemsNumber * 2)
    in.readFully(data)
    val result = new Array[Short](itemsNumber)
    var i = 0
    while (i < 2 * itemsNumber) {
      result(i / 2) = DataInputStreamOps.fromBytes(data(i + 1), data(i))
      i += 2
    }
    new Int16Column(result)
  }
}

class Int32Column private[columns] (_data: Array[Int]) extends Column {
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

object Int32Column {
  def apply() = new Int32Column(new Array[Int](DEFAULT_INSERT_BLOCK_SIZE))

  def from(in: DataInputStream, itemsNumber: Int): Int32Column = {
    val data = new Array[Byte](itemsNumber * 4)
    in.readFully(data)
    val result = new Array[Int](itemsNumber)
    var i = 0
    while (i < 4 * itemsNumber) {
      result(i / 4) = DataInputStreamOps.fromBytes(data(i + 3), data(i + 2), data(i + 1), data(i))
      i += 4
    }
    new Int32Column(result)
  }
}

class Int64Column private[columns] (_data: Array[Long]) extends Column {
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

object Int64Column {
  def apply() = new Int64Column(new Array[Long](DEFAULT_INSERT_BLOCK_SIZE))

  def from(in: DataInputStream, itemsNumber: Int): Int64Column = {
    val data = new Array[Byte](itemsNumber * 8)
    in.readFully(data)
    val result = new Array[Long](itemsNumber)
    var i = 0
    while (i < 8 * itemsNumber) {
      result(i / 8) = DataInputStreamOps.fromBytes(
        data(i + 7),
        data(i + 6),
        data(i + 5),
        data(i + 4),
        data(i + 3),
        data(i + 2),
        data(i + 1),
        data(i)
      )
      i += 8
    }
    new Int64Column(result)
  }
}
