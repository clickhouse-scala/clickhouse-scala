package chdriver.core

class Client(val insertBlockSize: Int = DriverProperties.DEFAULT_INSERT_BLOCK_SIZE,
             val connection: Connection = new Connection()) {
  def disconnect(): Unit = connection.disconnect()

  def execute[T](query: String, settings: ClickhouseProperties)(implicit decoder: Decoder[T]): Iterator[T] = {
    // todo basic_functionality insert vs select distinction
    connection.forceConnect()

    connection.sendQuery(query, settings)
    connection.sendExternalTables()

    // todo advanced_functionality progress vs no_progress
    receiveResult()
  }

  def receiveResult[T: Decoder](): Iterator[T] = {
    receiveResultNoProgress(Iterator[T]())
  }

  @annotation.tailrec
  final def receiveResultNoProgress[T: Decoder](result: Iterator[T]): Iterator[T] = {
    connection.receivePacket() match {
      case data: DataPacket[T] @unchecked =>
        receiveResultNoProgress(result ++ data.block.iterator)

      case e: ExceptionPacket =>
        throw e

      case profileInto: ProfileInfoPacket =>
        // todo do smth
        receiveResultNoProgress(result)

      case progress: ProgressPacket =>
        // todo do smth, backpressure?
        receiveResultNoProgress(result)

      case EndOfStreamPacket =>
        result

      case UnrecognizedPacket =>
        receiveResultNoProgress(result)
    }
  }
}
