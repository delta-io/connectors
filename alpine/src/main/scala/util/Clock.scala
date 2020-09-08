package main.scala.util

trait Clock {
  def getTimeMillis(): Long
  def nanoTime(): Long
  def waitTillTime(targetTime: Long): Long
}
