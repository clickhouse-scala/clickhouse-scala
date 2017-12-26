package chdriver.core

import org.reactivestreams.Publisher

class Client(val insertBlockSize: Int = DriverProperties.DEFAULT_INSERT_BLOCK_SIZE,
             val connection: Connection = new Connection()) {

  def insert[T: Encoder](query: String,
                         settings: ClickhouseProperties,
                         publisher: Publisher[T]): ClickHouseBlockingSubscriber[T] = {
    connection.forceConnect()

    connection.sendQuery(query, settings)
    connection.sendExternalTables()

    val sample = connection.receiveSampleEmptyBlock() // todo Either for exception?
    val subscriber = new ClickHouseBlockingSubscriber[T](connection, sample)
    publisher.subscribe(subscriber)
    subscriber
  }

  //todo: Replace result Iterator[T] with Publisher[T]?
  def execute[T](query: String, settings: ClickhouseProperties = new ClickhouseProperties)(implicit decoder: Decoder[T]): Iterator[T] = {
    connection.forceConnect()

    connection.sendQuery(query, settings)
    connection.sendExternalTables()

    // todo advanced_functionality progress vs no_progress
    receiveResult()
  }

  def receiveResult[T: Decoder](): Iterator[T] = {
    receiveResultNoProgress(Iterator[T]())
  }

  def disconnect(): Unit = connection.disconnect()

  @annotation.tailrec
  final def receiveResultNoProgress[T: Decoder](result: Iterator[T]): Iterator[T] = {
    connection.receivePacket() match {
      case data: DataPacket =>
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
