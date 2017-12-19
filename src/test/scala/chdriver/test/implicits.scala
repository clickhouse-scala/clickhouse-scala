package chdriver.test

import chdriver.test.instances.ResultSetDecoderInstances
import chdriver.test.syntax.JdbcSyntax

/**
  * @author andr83
  */
object implicits extends ResultSetDecoderInstances
  with JdbcSyntax
