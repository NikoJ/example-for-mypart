package model

import org.apache.spark.sql.types.{ArrayType, LongType, StringType, StructType}

object schemas {
  val schemaWeblogs: StructType = new StructType()
    .add("uid", StringType)
    .add("visits", ArrayType(new StructType().add("timestamp", LongType).add("url", StringType)))
}
