package chdriver.core.columns

import java.io.DataInputStream

import chdriver.core.DriverProperties.DEFAULT_INSERT_BLOCK_SIZE
import chdriver.core.internal.Protocol.DataInputStreamOps
import chdriver.core.internal.columns.{Int16Column, Int32Column, Int64Column, Int8Column}

object Int8Column {
  def apply() = new Int8Column(new Array[Byte](DEFAULT_INSERT_BLOCK_SIZE))

  def from(in: DataInputStream, itemsNumber: Int): Int8Column = {
    val result = new Array[Byte](itemsNumber)
    in.readFully(result)
    new Int8Column(result)
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
