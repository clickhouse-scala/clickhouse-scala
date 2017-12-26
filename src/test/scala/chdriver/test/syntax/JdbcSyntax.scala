package chdriver.test.syntax

import java.sql.Connection

import chdriver.test.ResultSetDecoder

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
 * @author andr83
 */
trait JdbcSyntax {
  implicit class JdbcHelper(conn: Connection) {
    def execute[A: ClassTag](sql: String)(implicit decoder: ResultSetDecoder[A]): Array[A] = {
      val stmt = conn.createStatement()
      val rs = stmt.executeQuery(sql)
      val res = new ArrayBuffer[A]
      var i = 0
      while (rs.next) {
        res += decoder.decode(rs)
        i += 1
      }
      rs.close()
      res.toArray[A]
    }
  }
}
