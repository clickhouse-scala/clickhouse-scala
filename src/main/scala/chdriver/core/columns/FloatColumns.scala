package chdriver.core.columns

import java.io.DataInputStream

import chdriver.core.DriverProperties.DEFAULT_INSERT_BLOCK_SIZE
import chdriver.core.internal.Protocol.DataInputStreamOps
import chdriver.core.internal.columns.{Float32Column, Float64Column}

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

object Float64Column {
  def apply() = new Float64Column(new Array[Double](DEFAULT_INSERT_BLOCK_SIZE))

  def from(in: DataInputStream, itemsNumber: Int): Float64Column = {
    val data = new Array[Byte](itemsNumber * 8)
    in.readFully(data)
    val result = new Array[Double](itemsNumber)
    var i = 0
    while (i < itemsNumber * 8) {
      val lebLong = DataInputStreamOps.fromBytes(data(i + 7), data(i + 6), data(i + 5), data(i + 4), data(i + 3), data(i + 2), data(i + 1), data(i))
      result(i / 8) = java.lang.Double.longBitsToDouble(lebLong)
      i += 8
    }
    new Float64Column(result)
  }
}
