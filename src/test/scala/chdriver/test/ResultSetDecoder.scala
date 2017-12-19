package chdriver.test

import java.sql.ResultSet

/**
  * @author andr83
  */
trait ResultSetDecoder[A] {
  def decode(rs: ResultSet): A
}

object ResultSetDecoder {
  def pure[A](f: ResultSet => A): ResultSetDecoder[A] = new ResultSetDecoder[A] {
    override def decode(rs: ResultSet): A = f(rs)
  }
}
