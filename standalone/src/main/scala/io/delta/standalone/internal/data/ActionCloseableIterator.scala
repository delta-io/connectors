package io.delta.standalone.internal.data

import io.delta.standalone.actions.Action
import io.delta.standalone.data.CloseableIterator

import io.delta.standalone.internal.util.ConversionUtils

case class ActionCloseableIterator(stringIterator: CloseableIterator[String])
  extends CloseableIterator[Action]{

  override def hasNext: Boolean = {
    stringIterator.hasNext
  }

  override def next(): Action = {
    ConversionUtils.convertAction(
      io.delta.standalone.internal.actions.Action.fromJson(stringIterator.next))
  }

  override def close(): Unit = {
    stringIterator.close()
  }
}
