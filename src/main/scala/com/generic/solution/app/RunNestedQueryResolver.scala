package com.generic.solution.app

import com.generic.solution.utility.NestedQueryResolver
import org.apache.spark.sql.SparkSession

object RunNestedQueryResolver {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Nested Query Resolver")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read.option("multiline", "true").json("src/data/data.json")

    val query =
      """
        |SELECT certAuthority,status,zoneId,country
        |FROM mdm
        |NAMESPACE header.auditTrail.systemInfo.ipTrace.device.os.securityPatch.certAuthority.location.country,
        |header.auditTrail.systemInfo.ipTrace.device.os.securityPatch.certAuthority,
        |payload.policyDetails.holder.employment.status,
        |header.auditTrail.systemInfo.ipTrace.device.os.securityPatch.certAuthority.location.region.zone.zoneId
        |ORDER BY zoneId DESC
        |""".stripMargin

    import com.generic.solution.utility.NestedQueryResolver._


    validateQuerySyntax(query) match {
      case Right(parsed) =>
        println("Query is syntactically valid.")
        val result = resolveQuery(df, query)
        result.show()

      case Left(error) =>
        println(s"Query is invalid: $error")
    }


//    val doc = SchemaPathHelper.generateColumnDocumentation(df.schema)
//    println("===== COLUMN DOCUMENTATION =====")
//    println(doc)
  }
}

