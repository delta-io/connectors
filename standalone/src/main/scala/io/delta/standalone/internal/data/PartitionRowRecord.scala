package io.delta.standalone.internal.data

import java.math.{BigDecimal => BigDecimalJ}
import java.sql.{Date, Timestamp}

import scala.reflect.ClassTag

import io.delta.standalone.data.{RowRecord => RowRecordJ}
import io.delta.standalone.internal.exception.DeltaErrors
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

  // todo: if partition values CAN be null...
//  private def getAs[T, D](fieldName: String, f: String => T): Option[T] = {
//    requireFieldExists(fieldName)
//    if (isNullAt(fieldName)) {
//      if (!partitionSchema.get(fieldName).isNullable()) {
//        throw DeltaErrors.nullValueFoundForNonNullSchemaField(fieldName, partitionSchema)
//      }
//      None
//    } else {
//      require(partitionFieldToType(fieldName).isInstanceOf[D])
//      Some(f(partitionValues(fieldName)))
//    }
//  }
  // FOLLOWING ROWRECORD INTERFACE...
  private def getPrimitive(fieldName: String): String = {
    if (isNullAt(fieldName)) {
      throw new NullPointerException(
        s"null value found for primitive DataType: " +
          s"${partitionFieldToType(fieldName).getSimpleString()} " +
          s"Field: $fieldName in RowRecord: $partitionValues")
    }
    partitionValues(fieldName)
  }

  private def getNonPrimitive(fieldName: String): Option[String] = {
    if (isNullAt(fieldName)) {
      if (!partitionSchema.get(fieldName).isNullable()) {
        throw new NullPointerException(
          s"null value found for non-nullable Field: $fieldName in RowRecord: $partitionValues")
      }
      None
    } else {
      Some(partitionValues(fieldName))
    }
  }

  override def getSchema: StructType = partitionSchema

  override def getLength: Int = partitionSchema.getFieldNames.length


  // per Delta Protocol an empty string for any type is a null value
  // TODO: should this throw an error if field is not nullable?
  override def isNullAt(fieldName: String): Boolean = {
    requireFieldExists(fieldName)
    partitionValues(fieldName).isEmpty()
  }

  override def getInt(fieldName: String): Int = {
    requireFieldExists(fieldName)
    if (!partitionFieldToType(fieldName).isInstanceOf[IntegerType]) {
      throw new ClassCastException(s"Mismatched DataType for Field: " +
        s"$fieldName in RowRecord: $partitionValues")
    }
    getPrimitive(fieldName).toInt
  }

  override def getLong(fieldName: String): Long = {
    requireFieldExists(fieldName)
    if (!partitionFieldToType(fieldName).isInstanceOf[LongType]) {
      throw new ClassCastException(s"Mismatched DataType for Field: " +
        s"$fieldName in RowRecord: $partitionValues")
    }
    getPrimitive(fieldName).toLong
  }

  override def getByte(fieldName: String): Byte = {
    requireFieldExists(fieldName)
    if (!partitionFieldToType(fieldName).isInstanceOf[ByteType]) {
      throw new ClassCastException(s"Mismatched DataType for Field: " +
        s"$fieldName in RowRecord: $partitionValues")
    }
    getPrimitive(fieldName).toByte
  }

  override def getShort(fieldName: String): Short = {
    requireFieldExists(fieldName)
    if (!partitionFieldToType(fieldName).isInstanceOf[ShortType]) {
      throw new ClassCastException(s"Mismatched DataType for Field: " +
        s"$fieldName in RowRecord: $partitionValues")
    }
    getPrimitive(fieldName).toShort
  }

  override def getBoolean(fieldName: String): Boolean = {
    requireFieldExists(fieldName)
    if (!partitionFieldToType(fieldName).isInstanceOf[BooleanType]) {
      throw new ClassCastException(s"Mismatched DataType for Field: " +
        s"$fieldName in RowRecord: $partitionValues")
    }
    getPrimitive(fieldName).toBoolean
  }

  override def getFloat(fieldName: String): Float = {
    requireFieldExists(fieldName)
    if (!partitionFieldToType(fieldName).isInstanceOf[FloatType]) {
      throw new ClassCastException(s"Mismatched DataType for Field: " +
        s"$fieldName in RowRecord: $partitionValues")
    }
    getPrimitive(fieldName).toFloat
  }

  override def getDouble(fieldName: String): Double = {
    requireFieldExists(fieldName)
    if (!partitionFieldToType(fieldName).isInstanceOf[DoubleType]) {
      throw new ClassCastException(s"Mismatched DataType for Field: " +
        s"$fieldName in RowRecord: $partitionValues")
    }
    getPrimitive(fieldName).toDouble
  }

  override def getString(fieldName: String): String = {
    requireFieldExists(fieldName)
    if (!partitionFieldToType(fieldName).isInstanceOf[StringType]) {
      throw new ClassCastException(s"Mismatched DataType for Field: " +
        s"$fieldName in RowRecord: $partitionValues")
    }
    getNonPrimitive(fieldName).getOrElse(null)
  }

  override def getBinary(fieldName: String): Array[Byte] = {
    requireFieldExists(fieldName)
    if (!partitionFieldToType(fieldName).isInstanceOf[BinaryType]) {
      throw new ClassCastException(s"Mismatched DataType for Field: " +
        s"$fieldName in RowRecord: $partitionValues")
    }
    getNonPrimitive(fieldName).map{_.map{_.toByte}.toArray}.getOrElse(null)
  }

  override def getBigDecimal(fieldName: String): BigDecimalJ = {
    requireFieldExists(fieldName)
    if (!partitionFieldToType(fieldName).isInstanceOf[DecimalType]) {
      throw new ClassCastException(s"Mismatched DataType for Field: " +
        s"$fieldName in RowRecord: $partitionValues")
    }
    getNonPrimitive(fieldName).map{new BigDecimalJ(_)}.getOrElse(null)
  }

  override def getTimestamp(fieldName: String): Timestamp = {
    requireFieldExists(fieldName)
    if (!partitionFieldToType(fieldName).isInstanceOf[TimestampType]) {
      throw new ClassCastException(s"Mismatched DataType for Field: " +
        s"$fieldName in RowRecord: $partitionValues")
    }
    getNonPrimitive(fieldName).map{Timestamp.valueOf(_)}.getOrElse(null)
  }

  override def getDate(fieldName: String): Date = {
    requireFieldExists(fieldName)
    if (!partitionFieldToType(fieldName).isInstanceOf[DateType]) {
      throw new ClassCastException(s"Mismatched DataType for Field: " +
        s"$fieldName in RowRecord: $partitionValues")
    }
    getNonPrimitive(fieldName).map{Date.valueOf(_)}.getOrElse(null)
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
