package chdriver.core

import chdriver.core.columns.Column

trait Encoder[T] { // todo seems possible to "generate" with java's reflection
  def initialState: Array[Column]

  def fieldByIndex(t: T, n: Int): Any // !!! must match with 'initialState'
  // def encode(t: T, state: Array[Column], position: Int): Unit // todo check if this can give measurable profit (avoids boxing)
}
