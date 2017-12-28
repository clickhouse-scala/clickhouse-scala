package chdriver.core.internal.columns

import java.io.DataOutputStream

import chdriver.core.columns.Column

sealed abstract class EnumColumn[TT] private[chdriver] (_data: Array[TT], val mapping: Map[TT, String]) extends Column {
  override type T = TT
  override val data = _data

  override def conformsTo(runtimeChType: String): Boolean =
    mapping.forall { case (v, k) => runtimeChType.contains(s"'$k' = $v") }

  override def writeTo(out: DataOutputStream, toRow: Int): Unit = this.getClass.getSimpleName match {
    case "Enum8Column" => new Int8Column(data.asInstanceOf[Array[Byte]]).writeTo(out, toRow)
    case "Enum16Column" => new Int16Column(data.asInstanceOf[Array[Short]]).writeTo(out, toRow)
  }
}

class Enum8Column private[chdriver] (_data: Array[Byte], mapping: Map[Byte, String]) extends EnumColumn(_data, mapping)

class Enum16Column private[chdriver] (_data: Array[Short], mapping: Map[Short, String]) extends EnumColumn(_data, mapping)
