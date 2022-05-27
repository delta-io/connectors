package io.delta.standalone.internal

import java.util.List

import io.delta.standalone.VersionLog
import io.delta.standalone.actions.Action
import io.delta.standalone.data.CloseableIterator

final class VersionLogImpl(
    version: Long,
    supplier: () => CloseableIterator[Action],
    actions: List[Action])
  extends VersionLog(version, actions) {
  // use empty list initialize this object

  override def getVersion(): Long = {
    version
  }

  def getActionIterator(): CloseableIterator[Action] = {
    supplier()
  }
}