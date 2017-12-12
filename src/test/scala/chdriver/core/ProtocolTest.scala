package chdriver.core

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import org.junit._

class ProtocolTest {
  import Protocol.DataOutputStreamOps
  import Protocol.DataInputStreamOps

  @Test
  def testWriteUInt8(): Unit = {
    checkOut(_.writeUInt8(true), Array(1))
    checkOut(_.writeUInt8(false), Array(0))
    checkOut(_.writeUInt8(1), Array(1))
    checkOut(_.writeUInt8(0), Array(0))
    checkOut(_.writeUInt8(255), Array(-1))
    checkOut(_.writeUInt8(128), Array(-128))
  }

  @Test
  def testWriteInt32(): Unit = {
    checkOut(_.writeInt32(666666), Array(42, 44, 10, 0))
    checkOut(_.writeInt32(0), Array(0, 0, 0, 0))
    checkOut(_.writeInt32(-7777777), Array(15, 82, -119, -1))
    checkOut(_.writeInt32(Integer.MAX_VALUE), Array(-1, -1, -1, 127))
    checkOut(_.writeInt32(Integer.MIN_VALUE), Array(0, 0, 0, -128))
  }

  @Test
  def testWriteUInt128(): Unit = {
    checkOut(_.writeAsUInt128(0), Array(0))
    checkOut(_.writeAsUInt128(624485), Array(-27, -114, 38))
    checkOut(_.writeAsUInt128(100000000), Array(-128, -62, -41, 47))
  }

  @Test
  def testWriteString(): Unit = {
    checkOut(_.writeString("a.b.c"), Array(5, 97, 46, 98, 46, 99))
    checkOut(_.writeString(""), Array(0))
    checkOut(_.writeString("foo뉡옓ꗃ붺༱쯇涀愜Ṽ"), Array(30, 102, 111, 111, -21, -119, -95, -20, -104, -109, -22, -105, -125, -21, -74, -70, -32, -68, -79, -20, -81, -121, -26, -74, -128, -26, -124, -100, -31, -71, -68))
  }

  @Test
  def testReadUInt8(): Unit = {
    implicit val action = (x: DataInputStream) => x.readUInt8()
    checkIn(Array(0), 0)
    checkIn(Array(1), 1)
    checkIn(Array(127), 127)
    checkIn(Array(-128), 128)
    checkIn(Array(-1), 255)
  }

  @Test
  def testReadInt32(): Unit = {
    implicit val action = (x: DataInputStream) => x.readInt32()
    checkIn(Array(0, 0, 0, 0), 0)
    checkIn(Array(42, 44, 10, 0), 666666)
    checkIn(Array(15, 82, -119, -1), -7777777)
    checkIn(Array(-1, -1, -1, 127), Integer.MAX_VALUE)
    checkIn(Array(0, 0, 0, -128), Integer.MIN_VALUE)
  }

  @Test
  def testReadInt64(): Unit = {
    implicit val action = (x: DataInputStream) => x.readInt64()
    checkIn(Array(0, 0, 0, 0, 0, 0, 0, 0), 0L)
    checkIn(Array(116, 29, -103, -66, 28, 0, 0, 0), 123456789876L)
    checkIn(Array(-57, 116, 38, 112, 4, -9, -1, -1), -9876543212345L)
    checkIn(Array(-1, -1, -1, -1, -1, -1, -1, 127), Long.MaxValue)
    checkIn(Array(0, 0, 0, 0, 0, 0, 0, -128), Long.MinValue)
  }

  @Test
  def testReadUInt128(): Unit = {
    implicit val action = (x: DataInputStream) => x.readAsUInt128()
    checkIn(Array(0), 0)
    checkIn(Array(-27, -114, 38), 624485)
    checkIn(Array(-128, -62, -41, 47), 100000000)
  }

  @Test
  def testReadString(): Unit = {
    implicit val action = (x: DataInputStream) => x.readString()
    checkIn(Array(5, 97, 46, 98, 46, 99), "a.b.c")
    checkIn(Array(0), "")
    checkIn(Array(30, 102, 111, 111, -21, -119, -95, -20, -104, -109, -22, -105, -125, -21, -74, -70, -32, -68, -79, -20, -81, -121, -26, -74, -128, -26, -124, -100, -31, -71, -68), "foo뉡옓ꗃ붺༱쯇涀愜Ṽ")
  }

  private def checkOut(action: DataOutputStream => Unit, expected: Array[Byte]): Unit = {
    import scala.language.reflectiveCalls
    val out = new DataOutputStream(new ByteArrayOutputStream()) {
      val baos = out.asInstanceOf[ByteArrayOutputStream]
    }
    action(out)
    val got = out.baos.toByteArray
    assert(
      got.sameElements(expected),
      s"${got.mkString("Array(", ", ", ")")} didn't equal ${expected.mkString("Array(", ", ", ")")}"
    )
  }

  private def checkIn[T](origin: Array[Byte], expected: T)(implicit action: DataInputStream => T): Unit = {
    val in = new DataInputStream(new ByteArrayInputStream(origin))
    val got = action(in)
    assert(got == expected, s"Got = $got, expected = $expected")
  }
}
