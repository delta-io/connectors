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

import java.io.UncheckedIOException
import java.util.Collections

import scala.collection.JavaConverters._
import scala.util.Try

import io.delta.storage.CloseableIterator

import io.delta.standalone.VersionLog
import io.delta.standalone.actions.{Action => ActionJ}

import io.delta.standalone.internal.actions.Action
import io.delta.standalone.internal.util.ConversionUtils


/**
 * Scala implementation of Java class [[VersionLog]].
 * To save memory, full action list is loaded only when calling [[getActions]].
 * [[CloseableIterator]] of actions, instead of [[List]], is passed into the class and the full
 * action list is only instantiated when calling [[getActions]]. When action list is long, this
 * helps saving the memory wasted by action list.
 *
 * @param version the table version at which these actions occurred
 * @param supplier provide [[CloseableIterator]] of actions for fetching information inside all
 *                 [[Action]] stored in this table version
 */
private[internal] class MemoryOptimizedVersionLog(
    version: Long,
    supplier: () => CloseableIterator[String])
  extends VersionLog(version, new java.util.ArrayList[ActionJ]()) {
  import io.delta.standalone.internal.util.Implicits._

  private lazy val cachedActions: java.util.List[ActionJ] = {
    supplier()
      .toArray
      .map(x => ConversionUtils.convertAction(Action.fromJson(x)))
      .toList
      .asJava
  }

  override def getActionsIterator: CloseableIterator[ActionJ] = {
    new CloseableIterator[ActionJ]() {
      // A wrapper class transforming CloseableIterator[String] to CloseableIterator[Action]

      private val stringIterator = supplier()

      override def next(): ActionJ = {
        ConversionUtils.convertAction(Action.fromJson(stringIterator.next))
      }

      override def close(): Unit = {
        stringIterator.close()
      }

      override def hasNext: Boolean = {
        stringIterator.hasNext
      }
    }
  }

  override def getActions: java.util.List[ActionJ] = {
    Collections.unmodifiableList(cachedActions)
  }
}
