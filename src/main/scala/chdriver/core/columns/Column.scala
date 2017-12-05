package chdriver.core.columns

import java.io.DataInputStream

abstract class Column {
  type T
  val data: Array[T]

  def writeItselfTo(): Unit = ??? // todo basic_functionality for inserts
  override def toString: String = s"Column(${data.mkString(" ")})"
}

object Column {
  final val ENUM8_R = "Enum8\\((.+)\\)".r
  final val ENUM16_R = "Enum16\\((.+)\\)".r
  final val FIXED_STRING_R = "FixedString\\(([0-9]+)\\)".r
  final val ARRAY_R = "Array\\(([0-9A-Za-z]+)\\)".r
  final val NULLABLE_R = "Nullable\\(([0-9A-Za-z]+)\\)".r

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
    case "Float32" => Float32Column.readAllFrom(in, itemsNumber)
    case "Float64" => Float64Column.readAllFrom(in, itemsNumber)
    case ENUM8_R(enum) => Enum8Column.readAllFrom(in, itemsNumber, enum)
    case ENUM16_R(enum) => Enum16Column.readAllFrom(in, itemsNumber, enum)
    case FIXED_STRING_R(itemLength) => FixedStringColumn.readAllFrom(in, itemsNumber, itemLength.toInt)
    case "Date" => DateColumn.readAllFrom(in, itemsNumber)
    case "DateTime" => DateTimeColumn.readAllFrom(in, itemsNumber)
    case ARRAY_R(innerType) => ArrayColumn.readAllFrom(in, itemsNumber, innerType)
    case NULLABLE_R(innerType) => NullableColumn.readAllFrom(in, itemsNumber, innerType)
  }
}
