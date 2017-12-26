package chdriver.core.columns

import java.io.{DataInputStream, DataOutputStream}

abstract class Column {
  type T
  val data: Array[T]

  def writeTo(out: DataOutputStream, toRow: Int): Unit = ???

  def chType: String = {
    val name = this.getClass.getSimpleName
    require(name.contains("Column"))
    val columnSuffixStart = name.indexOf("Column")
    name.substring(0, columnSuffixStart)
  }

  def conformsTo(runtimeChType: String): Boolean = chType == runtimeChType

  override def toString: String = chType
}

object Column {
  final val ENUM8_R = "Enum8\\((.+)\\)".r
  final val ENUM16_R = "Enum16\\((.+)\\)".r
  final val FIXED_STRING_R = "FixedString\\(([0-9]+)\\)".r
  final val ARRAY_R = "Array\\(([0-9A-Za-z()]+)\\)".r
  final val NULLABLE_R = "Nullable\\(([0-9A-Za-z()]+)\\)".r

  def from(in: DataInputStream, itemsNumber: Int, chType: String): Column =
    chType match {
      case "Int8" => Int8Column.from(in, itemsNumber)
      case "Int16" => Int16Column.from(in, itemsNumber)
      case "Int32" => Int32Column.from(in, itemsNumber)
      case "Int64" => Int64Column.from(in, itemsNumber)
      case "UInt8" => UInt8Column.from(in, itemsNumber)
      case "UInt16" => UInt16Column.from(in, itemsNumber)
      case "UInt32" => UInt32Column.from(in, itemsNumber)
      case "UInt64" => UInt64Column.from(in, itemsNumber)
      case "Float32" => Float32Column.from(in, itemsNumber)
      case "Float64" => Float64Column.from(in, itemsNumber)
      case "String" => StringColumn.from(in, itemsNumber)
      case FIXED_STRING_R(itemLength) => FixedStringColumn.from(in, itemsNumber, itemLength.toInt)
      case ENUM8_R(enum) => Enum8Column.from(in, itemsNumber, enum)
      case ENUM16_R(enum) => Enum16Column.from(in, itemsNumber, enum)
      case "Date" => DateColumn.from(in, itemsNumber)
      case "DateTime" => DateTimeColumn.from(in, itemsNumber)
      case ARRAY_R(innerType) => ArrayColumn.from(in, itemsNumber, innerType)
      case NULLABLE_R(innerType) => NullableColumn.from(in, itemsNumber, innerType)
    }

//  def byCHType(chType: String): Column = chType match {
//    case "Int32" => Int32Column()
//    case ARRAY_R(innerType) => ArrayColumn(byCHType(innerType))
//  }
}
