/*
 * DATABRICKS CONFIDENTIAL & PROPRIETARY
 * __________________
 *
 * Copyright 2020-present Databricks, Inc.
 * All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains the property of Databricks, Inc.
 * and its suppliers, if any.  The intellectual and technical concepts contained herein are
 * proprietary to Databricks, Inc. and its suppliers and may be covered by U.S. and foreign Patents,
 * patents in process, and are protected by trade secret and/or copyright law. Dissemination, use,
 * or reproduction of this information is strictly forbidden unless prior written permission is
 * obtained from Databricks, Inc.
 *
 * If you view or obtain a copy of this information and believe Databricks, Inc. may not have
 * intended it to be made available, please promptly report it to Databricks Legal Department
 * @ legal@databricks.com.
 */

package io.delta.standalone.internal

import java.util.{HashMap, Locale}
import java.time.{Duration, Period}

import io.delta.standalone.internal.actions.{Action, Metadata, Protocol}
import io.delta.standalone.internal.exception.DeltaErrors

import org.apache.hadoop.conf.Configuration

case class DeltaConfig[T](
                           key: String,
                           defaultValue: String,
                           fromString: String => T,
                           validationFunction: T => Boolean,
                           helpMessage: String,
                           minimumProtocolVersion: Option[Protocol] = None,
                           editable: Boolean = true) {
  /**
   * Recover the saved value of this configuration from `Metadata`. If undefined, fall back to
   * alternate keys, returning defaultValue if none match.
   */
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

  // TODO: rewrite this without spark & update docs
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
  def parseCalendarInterval(s: String): (Period, Duration) = {
    if (s == null || s.trim.isEmpty) {
      throw new IllegalArgumentException("Interval cannot be null or blank.")
    }
    val sInLowerCase = s.trim.toLowerCase(Locale.ROOT)
    val interval =
      if (sInLowerCase.startsWith("interval ")) sInLowerCase else "interval " + sInLowerCase
    val cal = IntervalUtils.safeStringToInterval(UTF8String.fromString(interval))
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
          // todo: how to log?
          logConsole(
            s"""
               |You are trying to set a property the key of which is the same as Delta config: $key.
               |If you are trying to set a Delta config, prefix it with "delta.", e.g. 'delta.$key'.
            """.stripMargin)
        }
        keyvalue
    }
  }

  /** TODO docs
   */
  def mergeGlobalConfigs(hadoopConf: Configuration, tableConf: Map[String, String]):
  Map[String, String] = {
    import collection.JavaConverters._

    val globalConfs = entries.asScala.flatMap { case (key, config) =>
      Option(hadoopConf.get(key, null)) match {
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

