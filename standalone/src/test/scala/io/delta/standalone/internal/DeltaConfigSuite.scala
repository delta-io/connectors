package io.delta.standalone.internal

// scalastyle:off funsuite
import org.scalatest.FunSuite

class DeltaConfigSuite extends FunSuite {

  // todo: test mergeGlobalConfigs

  // todo: test parsing (WIP)

  // todo: test defaults?
  // todo: test validate, build & from metadata (errors too)?

//  test("parseCalendarInterval") {
//    for (input <- Seq("5 MINUTES", "5 minutes", "5 Minutes", "inTERval 5 minutes")) {
//      assert(parseCalendarInterval(input) ===
//        new CalendarInterval(0, 0, TimeUnit.MINUTES.toMicros(5)))
//    }
//
//    for (input <- Seq(null, "", " ")) {
//      val e = intercept[IllegalArgumentException] {
//        parseCalendarInterval(input)
//      }
//      assert(e.getMessage.contains("cannot be null or blank"))
//    }
//
//    for (input <- Seq("interval", "interval1 day", "foo", "foo 1 day")) {
//      val e = intercept[IllegalArgumentException] {
//        parseCalendarInterval(input)
//      }
//      assert(e.getMessage.contains("Invalid interval"))
//    }
//  }
//
//  test("isValidIntervalConfigValue") {
//    for (input <- Seq(
//      // Allow 0 microsecond because we always convert microseconds to milliseconds so 0
//      // microsecond is the same as 100 microseconds.
//      "0 microsecond",
//      "1 microsecond",
//      "1 millisecond",
//      "1 day",
//      "-1 day 86400001 milliseconds", // This is 1 millisecond
//      "1 day -1 microseconds")) {
//      assert(isValidIntervalConfigValue(parseCalendarInterval(input)))
//    }
//    for (input <- Seq(
//      "-1 microseconds",
//      "-1 millisecond",
//      "-1 day",
//      "1 day -86400001 milliseconds", // This is -1 millisecond
//      "1 month",
//      "1 year")) {
//      assert(!isValidIntervalConfigValue(parseCalendarInterval(input)), s"$input")
//    }
//  }
}
