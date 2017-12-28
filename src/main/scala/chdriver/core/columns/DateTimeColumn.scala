package chdriver.core.columns

import java.io.DataInputStream

import chdriver.core.DriverProperties.DEFAULT_INSERT_BLOCK_SIZE
import chdriver.core.internal.columns.DateTimeColumn

object DateTimeColumn {
  def apply() = new DateTimeColumn(new Array[Int](DEFAULT_INSERT_BLOCK_SIZE))

  def from(in: DataInputStream, itemsNumber: Int): DateTimeColumn = {
    new DateTimeColumn(Int32Column.from(in, itemsNumber).data)
  }
}
