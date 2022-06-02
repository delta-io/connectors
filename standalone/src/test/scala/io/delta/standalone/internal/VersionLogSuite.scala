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

import io.delta.storage.CloseableIterator
import org.scalatest.FunSuite
import scala.collection.JavaConverters._

import io.delta.standalone.VersionLog
import io.delta.standalone.actions.{Action => ActionJ}

import io.delta.standalone.internal.actions.{Action, AddFile}
import io.delta.standalone.internal.util.ConversionUtils

class VersionLogSuite extends FunSuite {

  private val defaultVersionNumber = 33
  private val listLength = 300
  private val stringList: List[String] = List.fill(listLength)(
    AddFile(1.toString, Map.empty, 1, 1, dataChange = true).json)
  private val actionList: java.util.List[ActionJ] = stringList
    .toArray
    .map(x => ConversionUtils.convertAction(Action.fromJson(x)))
    .toList
    .asJava

  private var stringIterator = stringList.iterator
  private val actionCloseableIterator: CloseableIterator[String] = new CloseableIterator[String]() {

    override def next(): String = {
      stringIterator.next
    }

    @throws[java.io.IOException]
    override def close(): Unit = {}

    override def hasNext: Boolean = {
      stringIterator.hasNext
    }
  }

  private def checkVersionLog(newVersionLog: VersionLog): Unit = {

    val newActionList = newVersionLog.getActions

    assert(newVersionLog.getVersion == defaultVersionNumber,
      s"versionLog.getVersion() should be $defaultVersionNumber other than " +
        s"${newVersionLog.getVersion}")
    assert(newActionList.size() == actionList.size())
    assert(newActionList
      .toArray()
      .zip(actionList.toArray())
      .count(x => x._1 == x._2) == newActionList.size())
  }

  test("basic operation for VersionLog.java") {

    val versionLogWithList = new VersionLog(
      defaultVersionNumber,
      actionList
    )

    checkVersionLog(versionLogWithList)
  }

  test("basic operation for MemoryOptimizedVersionLog.scala") {

    val versionLogWithIterator = new MemoryOptimizedVersionLog(
      defaultVersionNumber,
      () => actionCloseableIterator
    )
    checkVersionLog(versionLogWithIterator)
  }

  test("CloseableIterator should not be instantiated when supplier is not used ") {
    stringIterator = stringList.iterator
    var applyCounter: Int = 0
    val supplierWithCounter: () => CloseableIterator[String] =
      () => {
        applyCounter += 1
        actionCloseableIterator
      }

    val versionLogWithIterator = new MemoryOptimizedVersionLog(
      defaultVersionNumber,
      supplierWithCounter
    )

    assert(versionLogWithIterator.getVersion == defaultVersionNumber,
      s"versionLog.getVersion() should be $defaultVersionNumber other than " +
        s"${versionLogWithIterator.getVersion}")

    assert(applyCounter == 0)
    versionLogWithIterator.getActions
    assert(applyCounter == 1)
    versionLogWithIterator.getActionsIterator
    assert(applyCounter == 2)
  }
}
