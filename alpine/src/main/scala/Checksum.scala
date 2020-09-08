package main.scala

import main.scala.metering.DeltaLogging

case class VersionChecksum(
    tableSizeBytes: Long,
    numFiles: Long,
    numMetadata: Long,
    numProtocol: Long,
    numTransactions: Long)

trait ReadChecksum extends DeltaLogging {
  self: DeltaLog =>

}
