package chdriver.core

import java.io.{BufferedInputStream, BufferedOutputStream, DataInputStream, DataOutputStream}
import java.net.Socket

import ClickhouseVersionSpecific._
import DriverProperties._
import chdriver.core.internal._

final class Connection(val host: String = "localhost",
                       val port: Int = DEFAULT_PORT,
                       val clientName: String = CLIENT_NAME,
                       val database: String = "default",
                       val user: String = "default",
                       val password: String = "",
) {
  import chdriver.core.internal.Protocol._
  var serverRevision: Int = _
  private var _serverTZ: String = _

  def getServerTimeZoneName: Option[String] = Option(_serverTZ)

  private var connected = false
  private var socket: Socket = _
  private[chdriver] var out: DataOutputStream = _
  private[chdriver] var in: DataInputStream = _

  // todo do this in constructor ?
  def connect(): Unit = {
    socket = new Socket(host, port)
    // todo basic_functionality timeout
    connected = true

    // todo advances_functionality https
    // todo advanced_functionality investigate possibility for parallel reads from net and block decoding
    // todo advanced_functionality investigate possibility to iterate on several columns simultaneously
    // todo advanced_functionality investigate nio / netty
    // todo advanced_functionality investigate https://github.com/real-logic/agrona/tree/master/agrona/src/main/java/org/agrona/io
    out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream, INPUT_BUFFER_SIZE))
    in = new DataInputStream(new BufferedInputStream(socket.getInputStream, INPUT_BUFFER_SIZE))

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
      clientInfo.writeTo(out, serverRevision)
    }
    settings.writeTo(out)
    out.writeAsUInt128(QueryProcessingStage.COMPLETE)
    out.writeAsUInt128(Compression.DISABLED) // todo advanced_functionality compressions
    out.writeString(query)
    out.flush()
  }

  private[chdriver] def sendExternalTables(): Unit = { // todo advanced_functionality mocked
    sendData(Block.empty, toRow = 1)
  }

  private[chdriver] def sendHello(): Unit = {
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

  private[chdriver] def receiveHello(): Unit = {
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

  private[chdriver] def sendData(block: Block, tableName: String = "", toRow: Int): Unit = {
    out.writeAsUInt128(ClientPacketTypes.DATA)
    if (serverRevision >= DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES) {
      out.writeString(tableName)
    }
    block.writeTo(out, toRow)
  }

  private def receiveData(): Block = {
    if (serverRevision > DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES) {
      val _ = in.readString()
    }
    Block.from(in)
  }

  private[chdriver] def receivePacket(): Packet = {
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

      case ServerPacketTypes.TOTALS => // todo advanced_functionality what are these for?
        val data = receiveData()
        DataPacket(data)

      case ServerPacketTypes.EXTREMES =>
        val data = receiveData()
        DataPacket(data)

      case ServerPacketTypes.END_OF_STREAM =>
        EndOfStreamPacket
    }
  }

  @annotation.tailrec
  private[chdriver] def receiveSampleEmptyBlock(): Block =
    receivePacket() match {
      case DataPacket(b) => b
      case EndOfStreamPacket => receiveSampleEmptyBlock()
      case p => throw new DriverException(s"Unexpected packet $p. Expected DATA.")
    }
}
