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

    val result = NestedQueryResolver.resolveQuery(df, query)
    result.show(false)

//    val doc = SchemaPathHelper.generateColumnDocumentation(df.schema)
//    println("===== COLUMN DOCUMENTATION =====")
//    println(doc)
    /*
    Exception in thread "main" java.lang.IllegalArgumentException: Column 'certAuthority' not found under namespaces: payload.policyDetails.agent.branch.region.zone, payload.policyDetails.agent.certAuthority.certAuthority, payload.policyDetails.agent.certAuthority, payload.policyDetails.policyStatus. Tried paths: payload.policyDetails.agent.branch.region.zone.certAuthority, payload.policyDetails.agent.certAuthority.certAuthority, payload.policyDetails.agent.certAuthority, payload.policyDetails.policyStatus.certAuthority, certAuthority
	at com.kafkastreaming.main.utlities.SchemaPathHelper$.$anonfun$resolveLeaf$3(SchemaPathHelper.scala:43)
	at scala.Option.getOrElse(Option.scala:189)
     */
  }
}

