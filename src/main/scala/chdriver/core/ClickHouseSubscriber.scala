package chdriver.core

import chdriver.core.DriverProperties.DEFAULT_INSERT_BLOCK_SIZE
import chdriver.core.columns.Column
import org.reactivestreams.{Subscriber, Subscription}

class ClickHouseSubscriber[T](connection: Connection, sample: Block)(implicit encoder: Encoder[T])
  extends Subscriber[T] {
  private var subscription: Subscription = _
  private var state: Array[Column] = encoder.initialState
  // todo j.u.c.Executor for async & non-blocking
  private var currentRow: Int = 0 // atomic?

  override def onError(t: Throwable): Unit = {
    if (t == null) throw null // 2.13

    println(t.getStackTrace.mkString("\n"))

    // todo 2.8 don't flush immediately (do: flush all now, and flush rest of new data once in N seconds)
    flush()
    connection.sendData(Block.empty, until = 1)
  }

  override def onComplete(): Unit = {
    // todo 2.8 don't flush immediately (do: flush all now, and flush rest of new data once in N seconds)
    flush()
    connection.sendData(Block.empty, until = 1)
  }

  override def onNext(t: T): Unit = {
    if (t == null) throw null // 2.13

    if (currentRow == DEFAULT_INSERT_BLOCK_SIZE) {
      flush()
    }

    for (j <- state.indices) {
      val c = state(j)
      val field = encoder.fieldByIndex(t, j) // boxing here
      c.data(currentRow) = field.asInstanceOf[c.T] // unboxing here
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
    import Protocol.DataOutputStreamOps

    if (currentRow > 0) { // not empty state
      connection.out.writeAsUInt128(ClientPacketTypes.DATA)
      connection.out.writeString("") // todo smth to do with temporary tables here

      Block(currentRow, state, sample.info, sample.columnNames, sample.columnTypes).writeTo(connection.out, currentRow)

      currentRow = 0
      state = encoder.initialState
    }
  }
}