package io.delta.standalone.internal

import scala.jdk.CollectionConverters.seqAsJavaListConverter

import io.delta.standalone.actions.{Action => ActionJ}
import io.delta.standalone.data.CloseableIterator

import io.delta.standalone.internal.actions.Action
import io.delta.standalone.internal.util.ConversionUtils
import io.delta.standalone.internal.util.Implicits.CloseableIteratorOps

private[internal] class VersionLog(
    version: Long,
    supplier: () => CloseableIterator[String],
    actions: java.util.List[ActionJ])
  extends io.delta.standalone.VersionLog(version, actions) {

  // Simpler constructor
  def this(version: Long, supplier: () => CloseableIterator[String]) = {
    this(version, supplier, List().asJava)
  }

  private def getNewActionIterator(stringIterator: CloseableIterator[String]) =
    new CloseableIterator[ActionJ]() {

      override def next(): ActionJ = {
        ConversionUtils.convertAction(
          io.delta.standalone.internal.actions.Action.fromJson(stringIterator.next))
      }

      @throws[java.io.IOException] // Not sure how to handle Java exceptions in Scala
      override def close(): Unit = {
        stringIterator.close()
      }

      override def hasNext: Boolean = {
        stringIterator.hasNext
      }
    }

  override def getActionIterator: CloseableIterator[ActionJ] = {
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
