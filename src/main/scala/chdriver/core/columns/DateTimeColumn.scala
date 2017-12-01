package chdriver.core.columns

import java.io.DataInputStream

/**
 * Quoting CH docs:
 * "Stored in four bytes as a Unix timestamp (unsigned). Allows storing values in the same range as for the Date type.
 * The minimal value is output as 0000-00-00 00:00:00.
 * The time is stored with accuracy up to one second (without leap seconds)."
 *
 * We decided not to choose which class will represent that, and return decoded "number of seconds since epoch" as Int.
 * You can find sample implementation of Decoder for java.time.LocalDateTime in 'BasicTypesSelect.testSelectDateTime'
 * It's possible to get server's time zone, see example in the mentioned above test.
 */
class DateTimeColumn(_data: Array[Int]) extends Column {
  override type T = Int
  override val data: Array[Int] = _data
}

object DateTimeColumn {
  def readAllFrom(in: DataInputStream, itemsNumber: Int): DateTimeColumn = {
    new DateTimeColumn(Int32Column.readAllFrom(in, itemsNumber).data)
  }
}
