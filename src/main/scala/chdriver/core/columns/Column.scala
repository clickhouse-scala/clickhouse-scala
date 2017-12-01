package chdriver.core.columns

import java.io.DataInputStream

abstract class Column {
  type T
  val data: Array[T]

  def writeItselfTo(): Unit = ??? // todo basic_functionality for inserts
  override def toString: String = s"Column(${data.mkString(" ")})"
}

object Column {
  val ArrayRegex = "Array\\(([0-9A-Za-z]+)\\)".r
  val NullableRegex = "Nullable\\(([0-9A-Za-z]+)\\)".r
  val FixedStringRegex = "FixedString\\(([0-9]+)\\)".r

  def from(in: DataInputStream, itemsNumber: Int, chtype: String): Column = chtype match {
    case "Int8" => Int8Column.readAllFrom(in, itemsNumber)
    case "Int16" => Int16Column.readAllFrom(in, itemsNumber)
    case "Int32" => Int32Column.readAllFrom(in, itemsNumber)
    case "Int64" => Int64Column.readAllFrom(in, itemsNumber)
    case "UInt8" => UInt8Column.readAllFrom(in, itemsNumber)
    case "UInt16" => UInt16Column.readAllFrom(in, itemsNumber)
    case "UInt32" => UInt32Column.readAllFrom(in, itemsNumber)
    case "UInt64" => UInt64Column.readAllFrom(in, itemsNumber)
    case "String" => StringColumn.readAllFrom(in, itemsNumber)
    case FixedStringRegex(itemLength) => FixedStringColumn.readAllFrom(in, itemsNumber, itemLength.toInt)
    case ArrayRegex(innerType) => ArrayColumn.readAllFrom(in, itemsNumber, innerType)
    case NullableRegex(innerType) => NullableColumn.readAllFrom(in, itemsNumber, innerType)
  }
}
