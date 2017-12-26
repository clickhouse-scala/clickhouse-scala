package chdriver.core.columns

import java.io.{DataInputStream, DataOutputStream}

import chdriver.core.DriverProperties.DEFAULT_INSERT_BLOCK_SIZE
import chdriver.core.Protocol.DataInputStreamOps
import chdriver.core.Protocol.DataOutputStreamOps

class Float32Column private[columns] (_data: Array[Float]) extends Column {
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

object Float32Column {
  def apply() = new Float32Column(new Array[Float](DEFAULT_INSERT_BLOCK_SIZE))

  def from(in: DataInputStream, itemsNumber: Int): Float32Column = {
    val data = new Array[Byte](itemsNumber * 4)
    in.readFully(data)
    val result = new Array[Float](itemsNumber)
    var i = 0
    while (i < itemsNumber * 4) {
      val lebInt = DataInputStreamOps.fromBytes(data(i + 3), data(i + 2), data(i + 1), data(i))
      result(i / 4) = java.lang.Float.intBitsToFloat(lebInt)
      i += 4
    }
    new Float32Column(result)
  }
}

class Float64Column private[columns] (_data: Array[Double]) extends Column {
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

object Float64Column {
  def apply() = new Float64Column(new Array[Double](DEFAULT_INSERT_BLOCK_SIZE))

  def from(in: DataInputStream, itemsNumber: Int): Float64Column = {
    val data = new Array[Byte](itemsNumber * 8)
    in.readFully(data)
    val result = new Array[Double](itemsNumber)
    var i = 0
    while (i < itemsNumber * 8) {
      val lebLong = DataInputStreamOps.fromBytes(
        data(i + 7),
        data(i + 6),
        data(i + 5),
        data(i + 4),
        data(i + 3),
        data(i + 2),
        data(i + 1),
        data(i)
      )
      result(i / 8) = java.lang.Double.longBitsToDouble(lebLong)
      i += 8
    }
    new Float64Column(result)
  }
}
