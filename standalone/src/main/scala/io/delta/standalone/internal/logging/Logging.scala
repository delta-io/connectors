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

package io.delta.standalone.internal.logging

import org.slf4j.{Logger, LoggerFactory}

/**
 * Utility trait for classes that want to log data. Creates a SLF4J logger for the class and allows
 * logging messages at different levels using methods that only evaluate parameters lazily if the
 * log level is enabled.
 */
private[internal] trait Logging {

  // Make the log field transient so that objects with Logging can
  // be serialized and used on another machine
  @transient private var log_ : Logger = null

  // Method to get the logger name for this object
  private def logName = {
    // Ignore trailing $'s in the class names for Scala objects
    this.getClass.getName.stripSuffix("$")
  }

  // Method to get or create the logger for this object
  private def log: Logger = {
    if (log_ == null) {
      log_ = LoggerFactory.getLogger(logName)
    }
    log_
  }

  /**
   * Optional prefix that will be prepended to each log message.
   */
  protected val logPrefix: Option[String] = None

  // Log methods that take only a String
  protected def logInfo(msg: => String): Unit = {
    if (log.isInfoEnabled) logPrefix match {
      case Some(prefix) => log.info(prefix + msg)
      case _ => log.info(msg)
    }
  }

  protected def logWarning(msg: => String): Unit = {
    if (log.isWarnEnabled) logPrefix match {
      case Some(prefix) => log.warn(prefix + msg)
      case _ => log.warn(msg)
    }
  }

  protected def logError(msg: => String): Unit = {
    if (log.isErrorEnabled) logPrefix match {
      case Some(prefix) => log.error(prefix + msg)
      case _ => log.error(msg)
    }
  }

  // Log methods that take Throwables (Exceptions/Errors) too
  protected def logInfo(msg: => String, throwable: Throwable): Unit = {
    if (log.isInfoEnabled) logPrefix match {
      case Some(prefix) => log.info(prefix + msg, throwable)
      case _ => log.info(msg, throwable)
    }
  }

  protected def logWarning(msg: => String, throwable: Throwable): Unit = {
    if (log.isWarnEnabled) logPrefix match {
      case Some(prefix) => log.warn(prefix + msg, throwable)
      case _ => log.warn(msg, throwable)
    }
  }

  protected def logError(msg: => String, throwable: Throwable): Unit = {
    if (log.isErrorEnabled) logPrefix match {
      case Some(prefix) => log.error(prefix + msg, throwable)
      case _ => log.error(msg, throwable)
    }
  }
}
