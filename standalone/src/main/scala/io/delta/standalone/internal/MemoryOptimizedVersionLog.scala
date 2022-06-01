/*
 * Copyright (2020-present) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.standalone.internal

import scala.jdk.CollectionConverters.seqAsJavaListConverter

import io.delta.standalone.actions.{Action => ActionJ}
import io.delta.standalone.data.CloseableIterator
import io.delta.standalone.VersionLog

import io.delta.standalone.internal.actions.Action
import io.delta.standalone.internal.util.ConversionUtils
import io.delta.standalone.internal.util.Implicits.CloseableIteratorOps

/**
 * Scala implementation of Java class [[VersionLog]].
 * To save memory, full action list is loaded only when calling [[getActions]]
 */
private[internal] class MemoryOptimizedVersionLog(
    version: Long,
    supplier: () => CloseableIterator[String])
  extends io.delta.standalone.VersionLog(version, new java.util.ArrayList[ActionJ]()) {

  /** @return an [[CloseableIterator]] of [[Action]] for this table version */
  override def getActionsIterator: CloseableIterator[ActionJ] = new CloseableIterator[ActionJ]() {
    /**
     * A wrapper class transforming [[CloseableIterator]] of [[String]]
     * to [[CloseableIterator]] of [[Action]]
     */

    private val stringIterator: CloseableIterator[String] = supplier()

    override def next(): ActionJ = {
      ConversionUtils.convertAction(Action.fromJson(stringIterator.next))
    }

    @throws[java.io.IOException]
    override def close(): Unit = {
      stringIterator.close()
    }

    override def hasNext: Boolean = {
      stringIterator.hasNext
    }
  }

  /** @return an [[List]] of [[Action]] for this table version */
  override def getActions: java.util.List[ActionJ] = supplier()
    .toArray
    .map(x => ConversionUtils.convertAction(Action.fromJson(x)))
    .toList
    .asJava
}
