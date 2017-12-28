package chdriver.core

import chdriver.core.DriverProperties.DEFAULT_INSERT_BLOCK_SIZE
import chdriver.core.columns.Column
import chdriver.core.internal.Block
import chdriver.core.internal.columns.{EnumColumn, NullableColumn}
import org.reactivestreams.{Subscriber, Subscription}

class ClickHouseBlockingSubscriber[T](connection: Connection, sample: Block)(implicit encoder: Encoder[T])
    extends Subscriber[T] {
  private var subscription: Subscription = _
  private var state: Array[Column] = encoder.initialState
  // todo j.u.c.Executor for async & non-blocking
  private var currentRow = 0 // atomic?
  private var isFinished = false

  for (i <- state.indices) {
    val provided = state(i)
    val actual = sample.columnTypes(i)
    require(provided.conformsTo(actual), s"Incompatible runtime data, provided=[$provided], actual=[$actual]")
  }

  override def onError(t: Throwable): Unit = {
    if (t == null) throw null // 2.13

    println(t.getStackTrace.mkString("\n"))

    flush()
    isFinished = true
    connection.sendData(Block.empty, toRow = 1)
  }

  override def onComplete(): Unit = {
    flush()
    isFinished = true
    connection.sendData(Block.empty, toRow = 1)
  }

  override def onNext(t: T): Unit = if (!isFinished) {
    if (t == null) throw null // 2.13

    if (currentRow == DEFAULT_INSERT_BLOCK_SIZE) {
      flush()
    }

    for (j <- state.indices) {
      val field = encoder.fieldByIndex(t, j) // possible boxing here
      state(j) match {
        case nc: NullableColumn if field == null =>
          nc.nulls(currentRow) = 0

        case ec: EnumColumn[_] =>
          val value = field.asInstanceOf[ec.T]
          if (!ec.mapping.contains(value)) { // CH allows data corrupting
            isFinished = true
            subscription.cancel() // 2.13
          } else {
            ec.data(currentRow) = value
          }

        case c =>
          c.data(currentRow) = field.asInstanceOf[c.T] // possible unboxing here
      }
    }

    subscription.request(DEFAULT_INSERT_BLOCK_SIZE)

    currentRow += 1
  }

  override def onSubscribe(s: Subscription): Unit = {
    if (subscription != null) subscription.cancel() // 2.5
    subscription = s
    s.request(DEFAULT_INSERT_BLOCK_SIZE)
  }

  private def flush(): Unit = {
    import chdriver.core.internal.Protocol.DataOutputStreamOps

    if (!isFinished // 2.8
        && currentRow > 0) { // not empty state
      connection.out.writeAsUInt128(ClientPacketTypes.DATA)
      connection.out.writeString("") // todo smth to do with temporary tables here

      Block(currentRow, state, sample.info, sample.columnNames, sample.columnTypes).writeTo(connection.out, currentRow)

      currentRow = 0
      state = encoder.initialState
    }
  }
}
