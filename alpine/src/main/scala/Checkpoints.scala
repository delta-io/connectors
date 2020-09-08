package main.scala

import main.scala.metering.DeltaLogging

case class CheckpointMetaData(
  version: Long,
  size: Long,
  parts: Option[Int])

trait Checkpoints extends DeltaLogging {
  self: DeltaLog =>
}