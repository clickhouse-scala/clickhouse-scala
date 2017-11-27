package chdriver.core

import java.io.DataOutputStream

class ClickhouseProperties { // todo basic_functionality other fields
  import Protocol.DataOutputStreamOps

  def writeItselfTo(out: DataOutputStream): Unit = {
    out.writeString("") // end of settings
  }
}
