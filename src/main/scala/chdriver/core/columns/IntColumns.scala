package chdriver.core.columns

import java.io.DataInputStream

class Int8Column(_data: Array[Byte]) extends Column {
  override type T = Byte
  override val data: Array[Byte] = _data
}

object Int8Column {
  def readAllFrom(in: DataInputStream, itemsNumber: Int): Int8Column = {
    val result = new Array[Byte](itemsNumber)
    in.readFully(result)
    new Int8Column(result)
  }
}

class Int16Column(_data: Array[Short]) extends Column {
  override type T = Short
  override val data: Array[Short] = _data
}

object Int16Column {
  def readAllFrom(in: DataInputStream, itemsNumber: Int): Int16Column = {
    import chdriver.core.Protocol.DataInputStreamOps

    val data = new Array[Byte](itemsNumber * 2)
    in.readFully(data)
    val result = new Array[Short](itemsNumber)
    var i = 0
    while (i < 2 * itemsNumber) {
      result(i / 2) = DataInputStreamOps.fromBytes(0, 0, data(i + 1), data(i)).asInstanceOf[Short]
      i += 2
    }
    new Int16Column(result)
  }
}

class Int32Column(_data: Array[Int]) extends Column {
  override type T = Int
  override val data: Array[Int] = _data
}

object Int32Column {
  def readAllFrom(in: DataInputStream, itemsNumber: Int): Int32Column = {
    import chdriver.core.Protocol.DataInputStreamOps

    val data = new Array[Byte](itemsNumber * 4)
    in.readFully(data)
    val result = new Array[Int](itemsNumber)
    var i = 0
    while (i < 4 * itemsNumber) {
      result(i / 4) = DataInputStreamOps.fromBytes(data(i + 3), data(i + 2), data(i + 1), data(i))
      i += 4
    }
    new Int32Column(result)
  }
}

class Int64Column(_data: Array[Long]) extends Column {
  override type T = Long
  override val data: Array[Long] = _data
}

object Int64Column {
  def readAllFrom(in: DataInputStream, itemsNumber: Int): Int64Column = {
    import chdriver.core.Protocol.DataInputStreamOps

    val data = new Array[Byte](itemsNumber * 8)
    in.readFully(data)
    val result = new Array[Long](itemsNumber)
    var i = 0
    while (i < 8 * itemsNumber) {
      result(i / 8) = DataInputStreamOps.fromBytes(
        data(i + 7),
        data(i + 6),
        data(i + 5),
        data(i + 4),
        data(i + 3),
        data(i + 2),
        data(i + 1),
        data(i)
      )
      i += 8
    }
    new Int64Column(result)
  }
}
