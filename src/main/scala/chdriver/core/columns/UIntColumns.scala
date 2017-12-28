package chdriver.core.columns

import java.io.DataInputStream

import chdriver.core.DriverProperties.DEFAULT_INSERT_BLOCK_SIZE
import chdriver.core.internal.columns.{UInt16Column, UInt32Column, UInt64Column, UInt8Column}

object UInt8Column {
  def apply() = new UInt8Column(new Array[Byte](DEFAULT_INSERT_BLOCK_SIZE))

  def from(in: DataInputStream, itemsNumber: Int): UInt8Column = {
    new UInt8Column(Int8Column.from(in, itemsNumber).data)
  }
}

object UInt16Column {
  def apply() = new UInt16Column(new Array[Short](DEFAULT_INSERT_BLOCK_SIZE))

  def from(in: DataInputStream, itemsNumber: Int): UInt16Column = {
    new UInt16Column(Int16Column.from(in, itemsNumber).data)
  }
}

object UInt32Column {
  def apply() = new UInt32Column(new Array[Int](DEFAULT_INSERT_BLOCK_SIZE))

  def from(in: DataInputStream, itemsNumber: Int): UInt32Column = {
    new UInt32Column(Int32Column.from(in, itemsNumber).data)
  }
}

object UInt64Column {
  def apply() = new UInt64Column(new Array[Long](DEFAULT_INSERT_BLOCK_SIZE))

  def from(in: DataInputStream, itemsNumber: Int): UInt64Column = {
    new UInt64Column(Int64Column.from(in, itemsNumber).data)
  }
}
