package chdriver.core.columns

import java.io.DataInputStream

import chdriver.core.DriverProperties.DEFAULT_INSERT_BLOCK_SIZE
import chdriver.core.internal.columns.DateColumn

object DateColumn {
  def apply() = new DateColumn(new Array[Short](DEFAULT_INSERT_BLOCK_SIZE))

  def from(in: DataInputStream, itemsNumber: Int): DateColumn = {
    new DateColumn(Int16Column.from(in, itemsNumber).data)
  }
}
