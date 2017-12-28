package chdriver.core.columns

import java.io.DataInputStream
import java.util.{Map => JMap}

import chdriver.core.DriverProperties.DEFAULT_INSERT_BLOCK_SIZE

import scala.collection.JavaConverters._
import chdriver.core.internal.columns.{Enum16Column, Enum8Column}

object Enum8Column {
  final val p = "'(.*?)' *= *([-0-9]+)".r

  def apply(mapping: Map[Byte, String]): Enum8Column = new Enum8Column(new Array[Byte](DEFAULT_INSERT_BLOCK_SIZE), mapping)

  def apply(mapping: JMap[Byte, String]): Enum8Column = apply(mapping.asScala.toMap)

  def from(in: DataInputStream, itemsNumber: Int, enum: String): Enum8Column = {
    val mapping = p.findAllIn(enum).map { case p(v, k) => k.toByte -> v }.toMap
    val keys = Int8Column.from(in, itemsNumber).data
    new Enum8Column(keys, mapping)
  }
}

object Enum16Column {
  final val p = "'(.*?)' *= *([-0-9]+)".r

  def apply(mapping: Map[Short, String]): Enum16Column = new Enum16Column(new Array[Short](DEFAULT_INSERT_BLOCK_SIZE), mapping)

  def apply(mapping: JMap[Short, String]): Enum16Column = apply(mapping.asScala.toMap)

  def from(in: DataInputStream, itemsNumber: Int, enum: String): Enum16Column = {
    val mapping = p.findAllIn(enum).map { case p(v, k) => k.toShort -> v }.toMap
    val keys = Int16Column.from(in, itemsNumber).data
    new Enum16Column(keys, mapping)
  }
}
