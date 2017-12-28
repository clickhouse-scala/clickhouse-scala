package chdriver.core.internal.columns

import java.io.DataOutputStream
import java.util.ArrayDeque

import chdriver.core.columns.Column
import chdriver.core.internal.Protocol.DataOutputStreamOps

class ArrayColumn private[chdriver] (_data: Array[Any], val inner: Column) extends Column {
  override type T = Any
  override val data = _data

  override def writeTo(out: DataOutputStream, toRow: Int): Unit = {
    require(data.nonEmpty)

    // `inner` may be an ArrayColumn, and we need innermost Column (that is not an ArrayColumn) to send data
    val innermost = {
      var innermost = inner
      while (innermost.isInstanceOf[ArrayColumn]) innermost = innermost.asInstanceOf[ArrayColumn].inner
      innermost
    }

    val q = new ArrayDeque[(Array[_], Int)]()
    q.addFirst(data -> toRow)
    var sum = 0
    var leftOnCurrentLevel = toRow
    while (!q.isEmpty) {
      val (data, onThisLevel) = q.removeFirst()

      if (data.isInstanceOf[Array[Array[_]]] || data.nonEmpty && data.head.isInstanceOf[Array[_]]) { // data contains nodes
        data.take(onThisLevel).foreach { v =>
          val innerArray = v.asInstanceOf[Array[_]]
          val size = innerArray.length
          sum += size
          out.writeInt64(sum)
          leftOnCurrentLevel -= 1
          q.addLast(innerArray -> size)
        }
      } else { // data contains leaves
        System.arraycopy(data, 0, innermost.data, 0, data.length)
        innermost.writeTo(out, data.length)
      }

      if (leftOnCurrentLevel == 0) {
        leftOnCurrentLevel = sum
        sum = 0
      }
    }
  }

  override def chType: String = super.chType + "(" + inner.chType + ")"
}
