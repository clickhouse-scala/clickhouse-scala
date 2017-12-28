package chdriver.core.internal.columns

import java.io.DataOutputStream

import chdriver.core.columns.Column

/**
 * Quoting CH docs:
 * "Stored in two bytes as the number of days since 1970-01-01 (unsigned). Allows storing values from
 * just after the beginning of the Unix Epoch to the upper threshold defined by a constant at the compilation stage
 * (currently, this is until the year 2038, but it may be expanded to 2106).
 * The minimum value is output as 0000-00-00."
 *
 * We decided not to choose which class will represent that, and return decoded "number of days since epoch" as Short.
 * You can find sample implementation of Decoder for java.time.LocalDate in 'BasicTypesSelect.testSelectDate'
 */
class DateColumn private[chdriver] (_data: Array[Short]) extends Column {
  override type T = Short
  override val data = _data

  override def writeTo(out: DataOutputStream, toRow: Int): Unit = {
    new Int16Column(data).writeTo(out, toRow)
  }
}
