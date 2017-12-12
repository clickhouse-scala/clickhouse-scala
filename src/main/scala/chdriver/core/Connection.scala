package chdriver.core

import java.io.{BufferedInputStream, BufferedOutputStream, DataInputStream, DataOutputStream}
import java.net.Socket

import ClickhouseVersionSpecific._
import DriverProperties._

class Connection(val host: String = "localhost",
                 val port: Int = 9000,
                 val clientName: String = CLIENT_NAME,
                 val database: String = "default",
                 val user: String = "default",
                 val password: String = "",
) {
  import Protocol._
  import chdriver.core.blocks.Native.{BlockOutputStream, BlockInputStream}

  var serverRevision: Int = _
  private var _serverTZ: String = _

  def getServerTimeZoneName: Option[String] = Option(_serverTZ)

  private var connected = false
  private var socket: Socket = _
  private var out: DataOutputStream = _
  private var in: DataInputStream = _

  def connect(): Unit = {
    socket = new Socket(host, port)
    // todo basic_functionality timeout
    connected = true

    // todo advances_functionality https
    // todo advanced_functionality investigate possibility for parallel reads from net and block decoding
    // todo advanced_functionality investigate possibility to iterate on several columns simultaneously
    // todo advanced_functionality investigate nio / netty
    // todo advanced_functionality investigate https://github.com/real-logic/agrona/tree/master/agrona/src/main/java/org/agrona/io
    out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream, BUFFER_SIZE))
    in = new DataInputStream(new BufferedInputStream(socket.getInputStream, BUFFER_SIZE))

    sendHello()
    receiveHello()
  }

  def disconnect(): Unit = {
    out.close()
    in.close()
    socket.close()
  }

  def forceConnect(): Unit = {
    if (!connected) connect()
    // todo basic_functionality ping
  }

  private[chdriver] def sendQuery(query: String, settings: ClickhouseProperties): Unit = {
    if (!connected) ??? // todo basic_functionality reconnect
    out.writeAsUInt128(ClientPacketTypes.QUERY)
    val queryId = "" // todo C++ check what is this for
    out.writeString(queryId)
    if (serverRevision > DBMS_MIN_REVISION_WITH_CLIENT_INFO) {
      val clientInfo = new ClientInfo(name = CLIENT_NAME, queryKind = ClientInfo.QueryKind.INITIAL_QUERY)
      clientInfo.writeItselfTo(out, serverRevision)
    }
    settings.writeItselfTo(out)
    out.writeAsUInt128(QueryProcessingStage.COMPLETE)
    out.writeAsUInt128(Compression.DISABLED) // todo advanced_functionality compressions
    out.writeString(query)
    out.flush()
  }

  private[chdriver] def sendExternalTables(): Unit = { // todo basic_functionality
    val fake = null
    sendData(new Block[Any](0, fake, columnsWithTypes = None)(null))
  }

  private def sendHello(): Unit = {
    out.writeAsUInt128(ClientPacketTypes.HELLO)
    out.writeString(CLIENT_NAME)
    out.writeAsUInt128(DBMS_VERSION_MAJOR)
    out.writeAsUInt128(DBMS_VERSION_MINOR)
    out.writeAsUInt128(CLIENT_VERSION)
    out.writeString(database)
    out.writeString(user)
    out.writeString(password)
    out.flush()
  }

  private def receiveHello(): Unit = {
    in.readAsUInt128() match {
      case ServerPacketTypes.HELLO =>
        val serverName = in.readString()
        val serverVersionMajor = in.readAsUInt128()
        val serverVersionMinor = in.readAsUInt128()
        serverRevision = in.readAsUInt128()

        println(s"Connected to $serverName $serverVersionMajor:$serverVersionMinor, revision = $serverRevision")

        if (serverRevision >= DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE) {
          _serverTZ = in.readString()
          println(s"Server timezone = ${_serverTZ}")
        }

      case ServerPacketTypes.EXCEPTION =>
        throw ExceptionPacket.from(in)

      case e =>
        disconnect()
        throw new DriverException(s"Unexpected packet type $e in response to 'hello' message")
    }
  }

  private def sendData(block: Block[_], tableName: String = ""): Unit = {
    out.writeAsUInt128(ClientPacketTypes.DATA)
    if (serverRevision >= DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES) {
      out.writeString(tableName)
    }
    out.writeBlock(block, serverRevision)
  }

  private def receiveData[T: Decoder](): Block[T] = {
    if (serverRevision > DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES) {
      val _ = in.readString()
    }
    in.readBlock(serverRevision)
  }

  def receivePacket[T: Decoder](): Packet = {
    val packetType = in.readAsUInt128()

    packetType match {
      case ServerPacketTypes.DATA =>
        val data = receiveData()
        DataPacket(data)

      case ServerPacketTypes.EXCEPTION =>
        ExceptionPacket.from(in)

      case ServerPacketTypes.PROGRESS =>
        ProgressPacket.from(in, serverRevision)

      case ServerPacketTypes.PROFILE_INFO =>
        ProfileInfoPacket.from(in)

      case ServerPacketTypes.TOTALS | ServerPacketTypes.EXTREMES => // todo advanced_functionality what are these for?
        val data = receiveData()
        DataPacket(data)

      case ServerPacketTypes.END_OF_STREAM =>
        EndOfStreamPacket
    }
  }
}
