package compatibility

import java.util
import java.util.Collections

import io.delta.standalone.actions.{Format, Metadata}
import io.delta.standalone.types.{IntegerType, StructField, StructType}

trait StandaloneUtil {
  val standaloneSchema: StructType = new StructType(Array(
    new StructField("col1_part", new IntegerType(), true),
  ))

  val standaloneMetadata: Metadata = Metadata.builder()
    .id("id")
    .name("name")
    .description("description")
    .format(new Format("parquet", Collections.singletonMap("format_key", "format_value")))
    .partitionColumns(util.Arrays.asList("col1_part"))
    .schema(standaloneSchema)
    .build()
}
