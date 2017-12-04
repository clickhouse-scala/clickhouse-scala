package chdriver.core.columns

import java.io.DataInputStream

class Float32Column(_data: Array[Float]) extends Column {
  override type T = Float
  override val data = _data
}

object Float32Column {
  def readAllFrom(in: DataInputStream, itemsNumber: Int): Float32Column = {
    import chdriver.core.Protocol.DataInputStreamOps

    val data = new Array[Byte](itemsNumber * 4)
    in.readFully(data)
    val result = new Array[Float](itemsNumber)
    var i = 0
    while (i < itemsNumber * 4) {
      val lebInt = DataInputStreamOps.fromBytes(
        data(i + 3),
        data(i + 2),
        data(i + 1),
        data(i)
      )
      result(i / 4) = java.lang.Float.intBitsToFloat(lebInt)
      i += 4
    }
    new Float32Column(result)
  }
}

class Float64Column(_data: Array[Double]) extends Column {
  override type T = Double
  override val data = _data
}

object Float64Column {
  def readAllFrom(in: DataInputStream, itemsNumber: Int): Float64Column = {
    import chdriver.core.Protocol.DataInputStreamOps

    val data = new Array[Byte](itemsNumber * 8)
    in.readFully(data)
    val result = new Array[Double](itemsNumber)
    var i = 0
    while (i < itemsNumber * 8) {
      val lebLong = DataInputStreamOps.fromBytes(
        data(i + 7),
        data(i + 6),
        data(i + 5),
        data(i + 4),
        data(i + 3),
        data(i + 2),
        data(i + 1),
        data(i)
      )
      result(i / 8) = java.lang.Double.longBitsToDouble(lebLong)
      i += 8
    }
    new Float64Column(result)
  }
}
