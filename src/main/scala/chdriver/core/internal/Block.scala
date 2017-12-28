package chdriver.core.internal

import java.io.{DataInputStream, DataOutputStream}

import chdriver.core.columns.Column
import chdriver.core.internal.columns.Column
import chdriver.core.{Decoder, DriverException}

case class Block(numberOfRows: Int,
                 data: Array[Column],
                 info: BlockInfo,
                 columnNames: Array[String],
                 columnTypes: Array[String]) {

  def iterator[T](implicit decoder: Decoder[T]): Iterator[T] = {
    if (numberOfRows > 0) {
      if (!decoder.validate(columnNames, columnTypes)) {
        throw new DriverException(
          s"Incompatible runtime data, names=[${columnNames.mkString(",")}], types=[${columnTypes.mkString(",")}]"
        )
      }
      decoder.transpose(numberOfRows, data)
    } else Iterator.empty
  }

  def writeTo(out: DataOutputStream, toRow: Int): Unit = {
    import Protocol.DataOutputStreamOps

    info.writeTo(out)
    out.writeAsUInt128(columnTypes.length)
    out.writeAsUInt128(numberOfRows.min(toRow))

    for (i <- data.indices) {
      out.writeString(columnNames(i))
      out.writeString(columnTypes(i))
      data(i).writeTo(out, toRow)
    }

    out.flush()
  }
}

object Block {
  def from(in: DataInputStream): Block = {
    import Protocol.DataInputStreamOps

    val info = BlockInfo.readItselfFrom(in)
    val numberOfColumns = in.readAsUInt128()
    val numberOfRows = in.readAsUInt128()

    // todo constant_memory_optimization preallocate arrays and reuse them
    val names = new Array[String](numberOfColumns)
    val types = new Array[String](numberOfColumns)
    val data = new Array[Column](numberOfColumns)

    for (i <- 0 until numberOfColumns) {
      val columnName = in.readString()
      val columnType = in.readString()

      names(i) = columnName
      types(i) = columnType

      if (numberOfRows > 0) {
        data(i) = Column.from(in, numberOfRows, columnType)
      }
    }

    Block(numberOfRows, data, info, names, types)
  }

  val empty = new Block(0, Array(), new BlockInfo(), Array(), Array())
}
