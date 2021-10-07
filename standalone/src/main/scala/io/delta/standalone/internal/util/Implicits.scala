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


package io.delta.standalone.internal.util

import io.delta.standalone.data.CloseableIterator

object Implicits {
  implicit class CloseableIteratorOps[T](private val iter: CloseableIterator[T]) {
    import scala.collection.JavaConverters._

    /**
     * Convert the [[CloseableIterator]] (Java) to an in-memory [[List]] (Scala).
     *
     * [[List]] is used over [[Seq]] so that the full list is actually generated instead of loaded
     * lazily, in which case `iter.close()` would be called before the Seq was actually generated.
     */
    def toAutoClosedList: List[T] = {
      try {
        iter.asScala.toList
      } finally {
        iter.close()
      }
    }
  }
}
