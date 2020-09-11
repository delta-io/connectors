/*
 * Copyright (2020) The Delta Lake Project Authors.
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

package main.scala

import main.scala.storage.LogStoreProvider
import main.scala.util.Clock
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

/**
 * Used to query the current state of the log as well as modify it by adding
 * new atomic collections of actions.
 *
 * Internally, this class implements an optimistic concurrency control
 * algorithm to handle multiple readers or writers. Any single read
 * is guaranteed to see a consistent snapshot of the table.
 */
class DeltaLog private(
    val logPath: Path,
    val dataPath: Path,
    val clock: Clock)
  extends Checkpoints
  with LogStoreProvider
  with SnapshotManagement {

  import DeltaLog._

  private def tombstoneRetentionMillis: Long = 100000000000000000L // TODO TOMBSTONE_RETENTION

  def minFileRetentionTimestamp: Long = clock.getTimeMillis() - tombstoneRetentionMillis

  lazy val store = createLogStore(hadoopConf)
}

object DeltaLog {
  val hadoopConf = new Configuration()
}
