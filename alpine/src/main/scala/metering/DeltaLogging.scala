package main.scala.metering

import main.scala.DeltaLog

trait DeltaLogging {
  protected def recordDeltaEvent(
    deltaLog: DeltaLog,
    opType: String,
    tags: Map[TagDefinition, String] = Map.empty,
    data: AnyRef = null): Unit = {
  }

  protected def recordDeltaOperation[A](
    deltaLog: DeltaLog,
    opType: String,
    tags: Map[TagDefinition, String] = Map.empty)(
    thunk: => A): A = {

  }
}
