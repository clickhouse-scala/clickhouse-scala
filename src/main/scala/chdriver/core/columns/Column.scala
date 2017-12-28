package chdriver.core.columns

import java.io.DataOutputStream

abstract class Column {
  type T
  val data: Array[T]

  def writeTo(out: DataOutputStream, toRow: Int): Unit

  def chType: String = {
    val name = this.getClass.getSimpleName
    require(name.contains("Column"))
    val columnSuffixStart = name.indexOf("Column")
    name.substring(0, columnSuffixStart)
  }

  def conformsTo(runtimeChType: String): Boolean = chType == runtimeChType

  override def toString: String = chType
}
