package chdriver.core

import chdriver.core.columns.Column

/**
 * Typeclass for all "encodable" things.
 * If you want to insert some domain objects, then you must have `Encoder` for that type.
 */
trait Encoder[T] {

  /**
   * Creates `Column`s with empty array of relevant data.
   * Can be used several time, to re-initialize state, thus `def`.
   *
   * ! Important -- must match with `fieldByIndex`.
   */
  def initialState: Array[Column]

  /**
   * Shows how to split domain object `T` in several scala/java types.
   * For NullableColumn, to indicate when field is null, should return null.
   *
   * ! Important -- must match with `initialState`.
   */
  def fieldByIndex(t: T, n: Int): Any
}
