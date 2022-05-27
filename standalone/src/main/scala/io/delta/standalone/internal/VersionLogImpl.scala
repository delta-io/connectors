package io.delta.standalone.internal

import io.delta.standalone.VersionLog
import io.delta.standalone.actions.Action
import io.delta.standalone.data.{ActionCloseableIterator, CloseableIterator}

import io.delta.standalone.internal.data.ActionCloseableIterator

final class VersionLogImpl(
    version: Long,
    supplier: () => CloseableIterator[String],
    actions: java.util.List[Action])
  extends VersionLog(version, actions) {
  // use empty list initialize this object

  override def getVersion: Long = {
    version
  }

  def getActionIterator: ActionCloseableIterator = {
    data.ActionCloseableIterator(supplier())
  }
}