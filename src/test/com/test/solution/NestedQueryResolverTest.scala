package com.test.solution

import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession

class NestedQueryResolverTest extends AnyFunSuite {

  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("NestedQueryResolverTest")
    .getOrCreate()

  import spark.implicits._

  val schema = StructType(Seq(
    StructField("payload", StructType(Seq(
      StructField("customer", StructType(Seq(
        StructField("id", StringType),
        StructField("name", StringType)
      ))),
      StructField("transactions", ArrayType(StructType(Seq(
        StructField("amount", IntegerType),
        StructField("type", StringType)
      ))))
    )))
  ))

  val data = Seq(
    Row(Row(Row("C001", "Alice"), Seq(Row(100, "credit"), Row(200, "debit")))),
    Row(Row(Row("C002", "Bob"), Seq(Row(150, "credit"))))
  )

  val df: DataFrame = spark.createDataFrame(
    spark.sparkContext.parallelize(data),
    schema
  )

  test("parseQuery should parse SELECT, WHERE, ORDER BY, and NAMESPACE clauses") {
    val query =
      """SELECT id, name
        |NAMESPACE payload.customer
        |WHERE id = 'C001'
        |ORDER BY name""".stripMargin

    val parsed = NestedQueryResolver.parseQuery(query)
    assert(parsed.select == Seq("id", "name"))
    assert(parsed.where.contains("id = 'C001'"))
    assert(parsed.orderBy.contains("name"))
    assert(parsed.namespaces == Seq("payload.customer"))
  }

  test("resolveQuery should select correct nested columns using namespace") {
    val query =
      """SELECT id, name
        |NAMESPACE payload.customer""".stripMargin

    val result = NestedQueryResolver.resolveQuery(df, query)
    val resultRows = result.collect()

    assert(result.columns.contains("id"))
    assert(result.columns.contains("name"))
    assert(resultRows.length == 2)
    assert(resultRows.exists(_.getAs[String]("id") == "C001"))
  }

  test("resolveQuery should filter based on WHERE clause") {
    val query =
      """SELECT name
        |NAMESPACE payload.customer
        |WHERE id = 'C002'""".stripMargin

    val result = NestedQueryResolver.resolveQuery(df, query)
    val rows = result.collect()

    assert(rows.length == 1)
    assert(rows.head.getAs[String]("name") == "Bob")
  }

  test("resolveQuery should order by column") {
    val query =
      """SELECT name
        |NAMESPACE payload.customer
        |ORDER BY name DESC""".stripMargin

    val result = NestedQueryResolver.resolveQuery(df, query)
    val rows = result.collect()

    assert(rows.head.getAs[String]("name") == "Bob")
    assert(rows.last.getAs[String]("name") == "Alice")
  }

  test("resolveQuery should throw if field not found") {
    val query =
      """SELECT nonExistentField
        |NAMESPACE payload.customer""".stripMargin

    val thrown = intercept[IllegalArgumentException] {
      NestedQueryResolver.resolveQuery(df, query)
    }

    assert(thrown.getMessage.contains
