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

package io.delta.standalone.internal.sources

/**
 * [[org.apache.hadoop.conf.Configuration]] entries for Delta Standalone Reader features.
 */
private[internal] object StandaloneHadoopConf {

  /** time zone as which time-based parquet values will be encoded and decoded */
  val PARQUET_DATA_TIME_ZONE_ID = "io.delta.standalone.PARQUET_DATA_TIME_ZONE_ID"

  /** TODO */
  val LOG_STORE_CLASS_KEY = "io.delta.standalone.LOG_STORE_CLASS_KEY"
}
