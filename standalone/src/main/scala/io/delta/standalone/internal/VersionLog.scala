package io.delta.standalone.internal

import io.delta.standalone.VersionLogInterface
import io.delta.standalone.actions.Action
import io.delta.standalone.data.CloseableIterator

import io.delta.standalone.internal.data.ActionCloseableIterator

final class VersionLog(
    version: Long,
    supplier: () => CloseableIterator[String],
    actions: java.util.List[Action])
  extends VersionLogInterface(version, actions) {
  // use empty list initialize this object

  override def getVersion: Long = {
    version
  }

  def getActionIterator: ActionCloseableIterator = {
    data.ActionCloseableIterator(supplier())
  }

  override def getActions: java.util.List[Action] = {
    actions // TODO: load all elements in the list
  }
}