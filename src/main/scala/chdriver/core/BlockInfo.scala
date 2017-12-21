package chdriver.core

import java.io.{DataInputStream, DataOutputStream}

class BlockInfo(val isOverflows: Boolean = false, val bucketNum: Int = 0) { // todo C++ what info is encoded in this BlockInfo?
  import chdriver.core.Protocol.DataOutputStreamOps

  def writeTo(out: DataOutputStream): Unit = {
    out.writeAsUInt128(1)
    out.writeUInt8(isOverflows)
    out.writeAsUInt128(2)
    out.writeInt32(bucketNum)
    out.writeAsUInt128(0)
  }
}

object BlockInfo {
  def readItselfFrom(in: DataInputStream): BlockInfo = {
    import Protocol.DataInputStreamOps

    @annotation.tailrec
    def fillInfoFields(isOverflows: Boolean, bucketNum: Int): (Boolean, Int) =
      in.readAsUInt128() match {
        case 0 => (isOverflows, bucketNum)
        case 1 => fillInfoFields(in.readUInt8() > 0, bucketNum)
        case 2 => fillInfoFields(isOverflows, in.readInt32())
        case _ => fillInfoFields(isOverflows, bucketNum)
      }

    val finished = fillInfoFields(isOverflows = false, bucketNum = 0)

    new BlockInfo(finished._1, finished._2)
  }
}
