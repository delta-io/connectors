package io.delta.standalone.internal.data

import java.math.{BigDecimal => BigDecimalJ}
import java.sql.{Date, Timestamp}

import io.delta.standalone.data.{RowRecord => RowRecordJ}
import io.delta.standalone.types._

/**
 * A RowRecord representing a Delta Lake partition of Map(partitionKey -> partitionValue)
 */
private[internal] class PartitionRowRecord(
    partitionSchema: StructType,
    partitionValues: Map[String, String]) extends RowRecordJ {

  private val partitionFieldToType =
   partitionSchema.getFields.map { f => f.getName -> f.getDataType }.toMap

  require(partitionFieldToType.keySet == partitionValues.keySet,
    s"""
      |Column mismatch between partitionSchema and partitionValues.
      |partitionSchema: ${partitionFieldToType.keySet.mkString(", ")}
      |partitionValues: ${partitionValues.keySet.mkString(", ")}
      |""".stripMargin)

  private def requireFieldExists(fieldName: String): Unit = {
    // this is equivalent to checking both partitionValues and partitionFieldToType maps
    // due to `require` statement above
    require(partitionValues.contains(fieldName))
  }

  override def getSchema: StructType = partitionSchema

  override def getLength: Int = partitionSchema.getFieldNames.length

  override def isNullAt(fieldName: String): Boolean = {
    requireFieldExists(fieldName)
    null == partitionValues(fieldName)
  }

  override def getInt(fieldName: String): Int = {
    requireFieldExists(fieldName)
    require(partitionFieldToType(fieldName).isInstanceOf[IntegerType])
    partitionValues(fieldName).toInt
  }

  override def getLong(fieldName: String): Long = {
    requireFieldExists(fieldName)
    require(partitionFieldToType(fieldName).isInstanceOf[LongType])
    partitionValues(fieldName).toLong
  }

  override def getByte(fieldName: String): Byte = {
    requireFieldExists(fieldName)
    require(partitionFieldToType(fieldName).isInstanceOf[ByteType])
    partitionValues(fieldName).toByte
  }

  override def getShort(fieldName: String): Short = {
    requireFieldExists(fieldName)
    require(partitionFieldToType(fieldName).isInstanceOf[ShortType])
    partitionValues(fieldName).toShort
  }

  override def getBoolean(fieldName: String): Boolean = {
    requireFieldExists(fieldName)
    require(partitionFieldToType(fieldName).isInstanceOf[BooleanType])
    partitionValues(fieldName).toBoolean
  }

  override def getFloat(fieldName: String): Float = {
    requireFieldExists(fieldName)
    require(partitionFieldToType(fieldName).isInstanceOf[FloatType])
    partitionValues(fieldName).toFloat
  }

  override def getDouble(fieldName: String): Double = {
    requireFieldExists(fieldName)
    require(partitionFieldToType(fieldName).isInstanceOf[DoubleType])
    partitionValues(fieldName).toDouble
  }

  override def getString(fieldName: String): String = {
    requireFieldExists(fieldName)
    require(partitionFieldToType(fieldName).isInstanceOf[StringType])
    partitionValues(fieldName)
  }

  override def getBinary(fieldName: String): Array[Byte] = {
    requireFieldExists(fieldName)
    require(partitionFieldToType(fieldName).isInstanceOf[DoubleType])
    partitionValues(fieldName).split("/").map(_.toByte)
  }

  override def getBigDecimal(fieldName: String): BigDecimalJ = {
    requireFieldExists(fieldName)
    require(partitionFieldToType(fieldName).isInstanceOf[DoubleType])
    new BigDecimalJ(partitionValues(fieldName))
  }

  override def getTimestamp(fieldName: String): Timestamp = {
    requireFieldExists(fieldName)
    require(partitionFieldToType(fieldName).isInstanceOf[DoubleType])
    Timestamp.valueOf(partitionValues(fieldName))
  }

  override def getDate(fieldName: String): Date = {
    requireFieldExists(fieldName)
    require(partitionFieldToType(fieldName).isInstanceOf[DoubleType])
    Date.valueOf(partitionValues(fieldName))
  }

  override def getRecord(fieldName: String): RowRecordJ = {
    throw new UnsupportedOperationException(
      "Struct is not a supported partition type.")
  }

  override def getList[T](fieldName: String): java.util.List[T] = {
    throw new UnsupportedOperationException(
      "Array is not a supported partition type.")
  }

  override def getMap[K, V](fieldName: String): java.util.Map[K, V] = {
    throw new UnsupportedOperationException(
      "Map is not a supported partition type.")
  }
}
