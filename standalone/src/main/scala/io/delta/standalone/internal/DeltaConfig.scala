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

package io.delta.standalone.internal

import java.util.{HashMap, Locale}

import io.delta.standalone.util.DateTimeConstants
import io.delta.standalone.internal.actions.{Action, Metadata, Protocol}
import io.delta.standalone.internal.exception.DeltaErrors
import io.delta.standalone.internal.util.{CalendarInterval, IntervalUtils}

import org.apache.hadoop.conf.Configuration

// todo: do we want to be able to validate a metadata as a whole?

case class DeltaConfig[T](
                           key: String,
                           defaultValue: String,
                           fromString: String => T,
                           validationFunction: T => Boolean,
                           helpMessage: String,
                           minimumProtocolVersion: Option[Protocol] = None,
                           editable: Boolean = true) {
  /**
   * Recover the saved value of this configuration from `Metadata`.
   * If undefined, return defaultValue.
   */
  // todo: should this have the delta prefix? Delta protocol doesn't have it but its implemented in
  //  runtime and delta oss with the prefix for this check
  def fromMetaData(metadata: Metadata): T = {
    fromString(metadata.configuration.getOrElse(key, defaultValue))
  }

  /** Validate the setting for this configuration */
  private def validate(value: String): Unit = {
    if (!editable) {
      throw DeltaErrors.cannotModifyTableProperty(key)
    }
    val onErrorMessage = s"$key $helpMessage"
    try {
      require(validationFunction(fromString(value)), onErrorMessage)
    } catch {
      case e: NumberFormatException =>
        throw new IllegalArgumentException(onErrorMessage, e)
    }
  }

  /**
   * Validate this configuration and return the key - value pair to save into the metadata.
   */
  def apply(value: String): (String, String) = {
    validate(value)
    key -> value
  }
}

// TODO: do we want to extend and include DeltaLogging?
/**
 * Contains list of reservoir configs and validation checks.
 */
trait DeltaConfigsBase {

  // todo: update the docs when done
  /**
   * Convert a string to [[CalendarInterval]]. This method is case-insensitive and will throw
   * [[IllegalArgumentException]] when the input string is not a valid interval.
   *
   * to/do Remove this method and use `CalendarInterval.fromCaseInsensitiveString` instead when
   * upgrading Spark. This is a fork version of `CalendarInterval.fromCaseInsensitiveString` which
   * will be available in the next Spark release (See SPARK-27735).C
   *
   * @throws IllegalArgumentException if the string is not a valid internal.
   */
  def parseCalendarInterval(s: String): CalendarInterval = {
    if (s == null || s.trim.isEmpty) {
      throw new IllegalArgumentException("Interval cannot be null or blank.")
    }
    val sInLowerCase = s.trim.toLowerCase(Locale.ROOT)
    val interval =
      if (sInLowerCase.startsWith("interval ")) sInLowerCase else "interval " + sInLowerCase
    val cal = IntervalUtils.safeStringToInterval(interval)
    if (cal == null) {
      throw new IllegalArgumentException("Invalid interval: " + s)
    }
    cal
  }

  private val entries = new HashMap[String, DeltaConfig[_]]

  protected def buildConfig[T](
                                key: String,
                                defaultValue: String,
                                fromString: String => T,
                                validationFunction: T => Boolean,
                                helpMessage: String,
                                minimumProtocolVersion: Option[Protocol] = None,
                                userConfigurable: Boolean = true): DeltaConfig[T] = {

    // todo: is this the prefix we want or should it be io.delta.standalone or something?
    val deltaConfig = DeltaConfig(s"delta.$key",
      defaultValue,
      fromString,
      validationFunction,
      helpMessage,
      minimumProtocolVersion,
      userConfigurable)

    entries.put(key.toLowerCase(Locale.ROOT), deltaConfig)
    deltaConfig
  }

  /**
   * Validates specified configurations and returns the normalized key -> value map.
   */
  def validateConfigurations(configurations: Map[String, String]): Map[String, String] = {
    configurations.map {
      case kv @ (key, value) if key.toLowerCase(Locale.ROOT).startsWith("delta.constraints.") =>
        // This is a CHECK constraint, we should allow it.
        kv
      case (key, value) if key.toLowerCase(Locale.ROOT).startsWith("delta.") =>
        Option(entries.get(key.toLowerCase(Locale.ROOT).stripPrefix("delta.")))
          .map(_(value))
          .getOrElse {
            throw DeltaErrors.unknownConfigurationKeyException(key)
          }
      case keyvalue @ (key, _) =>
        if (entries.containsKey(key.toLowerCase(Locale.ROOT))) {
          // TODO: how do we log in connectors?
//          logConsole(
//            s"""
//               |You are trying to set a property the key of which is the same as Delta config: $key.
//               |If you are trying to set a Delta config, prefix it with "delta.", e.g. 'delta.$key'.
//            """.stripMargin)
        }
        keyvalue
    }
  }

  /**
   * TODO docs
   *
   * User provided configs take precedence.
   */
  def mergeGlobalConfigs(hadoopConf: Configuration, tableConf: Map[String, String]):
  Map[String, String] = {
    import collection.JavaConverters._

    // todo: is there a hadoop conf prefix? this is based off of combo in OptimisticTransactionImpl
    // todo: do the metadata configurations have a prefix (see todo above at fromMetadata)

    val globalConfs = entries.asScala.flatMap { case (key, config) =>
      Option(hadoopConf.get(config.key.stripPrefix("delta."), null)) match {
        case Some(default) => Some(config(default))
        case _ => None
      }
    }

    globalConfs.toMap ++ tableConf
  }

  def getMilliSeconds(i: CalendarInterval): Long = {
    getMicroSeconds(i) / 1000L
  }

  private def getMicroSeconds(i: CalendarInterval): Long = {
    assert(i.months == 0)
    i.days * DateTimeConstants.MICROS_PER_DAY + i.microseconds
  }

  /**
   * For configs accepting an interval, we require the user specified string must obey:
   *
   * - Doesn't use months or years, since an internal like this is not deterministic.
   * - The microseconds parsed from the string value must be a non-negative value.
   *
   * The method returns whether a [[CalendarInterval]] satisfies the requirements.
   */
  def isValidIntervalConfigValue(i: CalendarInterval): Boolean = {
    i.months == 0 && getMicroSeconds(i) >= 0
  }

  /**
   * The protocol reader version modelled as a table property. This property is *not* stored as
   * a table property in the `Metadata` action. It is stored as its own action. Having it modelled
   * as a table property makes it easier to upgrade, and view the version.
   */
  val MIN_READER_VERSION = buildConfig[Int](
    "minReaderVersion",
    Action.readerVersion.toString,
    _.toInt,
    v => v > 0 && v <= Action.readerVersion,
    s"needs to be an integer between [1, ${Action.readerVersion}].")

  /**
   * The protocol reader version modelled as a table property. This property is *not* stored as
   * a table property in the `Metadata` action. It is stored as its own action. Having it modelled
   * as a table property makes it easier to upgrade, and view the version.
   */
  val MIN_WRITER_VERSION = buildConfig[Int](
    "minWriterVersion",
    Action.writerVersion.toString,
    _.toInt,
    v => v > 0 && v <= Action.writerVersion,
    s"needs to be an integer between [1, ${Action.writerVersion}].")

  /**
   * The shortest duration we have to keep delta files around before deleting them. We can only
   * delete delta files that are before a compaction. We may keep files beyond this duration until
   * the next calendar day.
   */
  val LOG_RETENTION = buildConfig[CalendarInterval](
    "logRetentionDuration",
    "interval 30 days",
    parseCalendarInterval,
    isValidIntervalConfigValue,
    "needs to be provided as a calendar interval such as '2 weeks'. Months " +
      "and years are not accepted. You may specify '365 days' for a year instead.")

  /**
   * The shortest duration we have to keep logically deleted data files around before deleting them
   * physically. This is to prevent failures in stale readers after compactions or partition
   * overwrites.
   *
   * Note: this value should be large enough:
   * - It should be larger than the longest possible duration of a job if you decide to run "VACUUM"
   *   when there are concurrent readers or writers accessing the table.
   * - If you are running a streaming query reading from the table, you should make sure the query
   *   doesn't stop longer than this value. Otherwise, the query may not be able to restart as it
   *   still needs to read old files.
   */
  val TOMBSTONE_RETENTION = buildConfig[CalendarInterval](
    "deletedFileRetentionDuration",
    "interval 1 week",
    parseCalendarInterval,
    isValidIntervalConfigValue,
    "needs to be provided as a calendar interval such as '2 weeks'. Months " +
      "and years are not accepted. You may specify '365 days' for a year instead.")

  /** How often to checkpoint the delta log. */
  val CHECKPOINT_INTERVAL = buildConfig[Int](
    "checkpointInterval",
    "10",
    _.toInt,
    _ > 0,
    "needs to be a positive integer.")

  /** Whether to clean up expired checkpoints and delta logs. */
  val ENABLE_EXPIRED_LOG_CLEANUP = buildConfig[Boolean](
    "enableExpiredLogCleanup",
    "true",
    _.toBoolean,
    _ => true,
    "needs to be a boolean.")

  /**
   * Whether this Delta table is append-only. Files can't be deleted, or values can't be updated.
   */
  val IS_APPEND_ONLY = buildConfig[Boolean](
    "appendOnly",
    "false",
    _.toBoolean,
    _ => true,
    "needs to be a boolean.",
    Some(new Protocol(0, 2)))
}

object DeltaConfigs extends DeltaConfigsBase

