package io.delta.standalone.internal

import scala.jdk.CollectionConverters.seqAsJavaListConverter

import io.delta.standalone.actions.{Action => ActionJ}
import io.delta.standalone.data.CloseableIterator

import io.delta.standalone.internal.actions.Action
import io.delta.standalone.internal.util.ConversionUtils
import io.delta.standalone.internal.util.Implicits.CloseableIteratorOps

private[internal] class MemoryOptimizedVersionLog(
    version: Long,
    supplier: () => CloseableIterator[String])
  extends io.delta.standalone.VersionLog(version, java.util.List[ActionJ]) {

  private def getNewActionIterator(stringIterator: CloseableIterator[String]) =
    new CloseableIterator[ActionJ]() {

      override def next(): ActionJ = {
        ConversionUtils.convertAction(
          io.delta.standalone.internal.actions.Action.fromJson(stringIterator.next))
      }

      @throws[java.io.IOException]
      override def close(): Unit = {
        stringIterator.close()
      }

      override def hasNext: Boolean = {
        stringIterator.hasNext
      }
    }

  override def getActionsIterator: CloseableIterator[ActionJ] = {
    getNewActionIterator(supplier())
  }

  override def getActions: java.util.List[ActionJ] = {
    supplier()
      .toArray
      .map(x => ConversionUtils.convertAction(Action.fromJson(x)))
      .toList
      .asJava
  }
}
