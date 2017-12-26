package chdriver.core.columns

import java.io.{DataInputStream, DataOutputStream}
import java.util.{Map => JMap}

import chdriver.core.DriverProperties.DEFAULT_INSERT_BLOCK_SIZE
import scala.collection.JavaConverters._

sealed abstract class EnumColumn[TT] private[columns] (_data: Array[TT], val mapping: Map[TT, String]) extends Column {
  override type T = TT
  override val data = _data

  override def conformsTo(runtimeChType: String): Boolean =
    mapping.forall { case (v, k) => runtimeChType.contains(s"'$k' = $v") }

  override def writeTo(out: DataOutputStream, toRow: Int): Unit = this.getClass.getSimpleName match {
    case "Enum8Column" => new Int8Column(data.asInstanceOf[Array[Byte]]).writeTo(out, toRow)
    case "Enum16Column" => new Int16Column(data.asInstanceOf[Array[Short]]).writeTo(out, toRow)
  }
}

class Enum8Column private[columns] (_data: Array[Byte], mapping: Map[Byte, String]) extends EnumColumn(_data, mapping)

object Enum8Column {
  final val p = "'(.*?)' *= *([-0-9]+)".r

  def apply(mapping: Map[Byte, String]): Enum8Column = new Enum8Column(new Array[Byte](DEFAULT_INSERT_BLOCK_SIZE), mapping)

  def apply(mapping: JMap[Byte, String]): Enum8Column = apply(mapping.asScala.toMap)

  def from(in: DataInputStream, itemsNumber: Int, enum: String): Enum8Column = {
    val mapping = p.findAllIn(enum).map { case p(v, k) => k.toByte -> v }.toMap
    val keys = Int8Column.from(in, itemsNumber).data
    new Enum8Column(keys, mapping)
  }
}

class Enum16Column private[columns] (_data: Array[Short], mapping: Map[Short, String]) extends EnumColumn(_data, mapping)

object Enum16Column {
  final val p = "'(.*?)' *= *([-0-9]+)".r

  def apply(mapping: Map[Short, String]): Enum16Column = new Enum16Column(new Array[Short](DEFAULT_INSERT_BLOCK_SIZE), mapping)

  def apply(mapping: JMap[Short, String]): Enum16Column = apply(mapping.asScala.toMap)

  def from(in: DataInputStream, itemsNumber: Int, enum: String): Enum16Column = {
    val mapping = p.findAllIn(enum).map { case p(v, k) => k.toShort -> v }.toMap
    val keys = Int16Column.from(in, itemsNumber).data
    new Enum16Column(keys, mapping)
  }
}
