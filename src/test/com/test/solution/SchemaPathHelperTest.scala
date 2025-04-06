package com.test.solution

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

class SchemaPathHelperTest extends AnyFunSuite {

  val spark: SparkSession = SparkSession.builder()
    .appName("SchemaPathHelperTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val sampleData = Seq(
    (
      "user1",
      Map("email" -> "user1@example.com"),
      Seq(Map("itemId" -> "A1", "qty" -> 2)),
      Map("meta" -> Map("ip" -> "127.0.0.1"))
    )
  ).toDF("username", "contact", "orders", "session")

  val schema = sampleData.schema

  test("findQualifiedPaths should find all leaf paths") {
    val paths = SchemaPathHelper.findQualifiedPaths(schema)
    val expected = Seq(
      ("username", false),
      ("contact.email", false),
      ("orders[].itemId", true),
      ("orders[].qty", true),
      ("session.meta.ip", false)
    )
    expected.foreach { case (path, isArray) =>
      assert(paths.contains((path, isArray)))
    }
  }

  test("resolveLeafColumn should resolve simple column") {
    val resolved = SchemaPathHelper.resolveLeafColumn(sampleData, "username")
    assert(resolved.isDefined)
    assert(!resolved.get.isArray)
    assert(resolved.get.column.expr.sql.contains("username"))
  }

  test("resolveLeafColumn should resolve nested field") {
    val resolved = SchemaPathHelper.resolveLeafColumn(sampleData, "email")
    assert(resolved.isDefined)
    assert(resolved.get.column.expr.sql.contains("contact.email"))
  }

  test("resolveLeafColumn should resolve array nested field") {
    val resolved = SchemaPathHelper.resolveLeafColumn(sampleData, "itemId")
    assert(resolved.isDefined)
    assert(resolved.get.isArray)
    assert(resolved.get.column.expr.sql.contains("itemId"))
  }

  test("selectResolved should select and alias resolved column") {
    val df = SchemaPathHelper.selectResolved(sampleData, "ip")
    assert(df.columns.contains("ip"))
    assert(df.count() == 1)
  }

  test("generateColumnDocumentation should return readable doc") {
    val doc = SchemaPathHelper.generateColumnDocumentation(schema)
    assert(doc.contains("username: string"))
    assert(doc.contains("orders[].itemId: string"))
    assert(doc.contains("session.meta.ip: string"))
  }

  test("extractLeafPaths should return all fully qualified paths") {
    val leafPaths = SchemaPathHelper.extractLeafPaths(schema)
    assert(leafPaths.contains("username"))
    assert(leafPaths.contains("orders[].itemId"))
    assert(leafPaths.contains("session.meta.ip"))
  }

  test("extractColumnMap should map leaf names to all qualified paths") {
    val map = SchemaPathHelper.extractColumnMap(schema)
    assert(map("ip").contains("session.meta.ip"))
    assert(map("qty").contains("orders[].qty"))
  }

  test("columnExists should validate existing paths") {
    val path = "session.meta.ip"
    val exists = SchemaPathHelper.resolveLeaf("ip", schema, Seq("session.meta"))
    assert(exists == path)
  }

  test("resolveLeaf should throw error for missing column") {
    val ex = intercept[IllegalArgumentException] {
      SchemaPathHelper.resolveLeaf("nonexistent", schema, Seq("session"))
    }
    assert(ex.getMessage.contains("Column 'nonexistent' not found"))
  }
}
