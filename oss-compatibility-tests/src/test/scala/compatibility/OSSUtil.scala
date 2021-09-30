package compatibility

import org.apache.spark.sql.delta.actions.{Format, Metadata}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

trait OSSUtil {
  val ossSchema = StructType(Array(
    StructField("col1_part", IntegerType, nullable = true),
  ))

  val ossMetadata = Metadata(
    id = "id",
    name = "name",
    description = "description",
    format = Format(provider = "parquet", options = Map("format_key" -> "format_value")),
    partitionColumns = Seq("col1_part"),
    schemaString = ossSchema.json
  )
}
