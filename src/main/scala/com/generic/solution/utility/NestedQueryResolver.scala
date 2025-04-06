package com.generic.solution.utility

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{Column, DataFrame}

object NestedQueryResolver {
  case class QueryParts(
                         select: Seq[String],
                         where: Option[String],
                         orderBy: Option[String],
                         namespaces: Seq[String]
                       )
  def validateQuerySyntax(query: String): Either[String, QueryParts] = {
    try {
      val parsed = parseQuery(query)

      // Validate SELECT clause
      if (parsed.select.isEmpty)
        return Left("SELECT clause is missing or invalid.")

      // Validate FROM clause
      if (!query.toUpperCase.contains("FROM"))
        return Left("Missing FROM clause.")

      // Validate WHERE clause
      parsed.where.foreach { clause =>
        val conditions = clause.split("AND").map(_.trim)
        for (c <- conditions) {
          if (!c.contains("=")) return Left(s"Invalid WHERE condition: '$c'")
        }
      }

      // Validate ORDER BY clause
      parsed.orderBy.foreach { clause =>
        val orderTokens = clause.split(",").map(_.trim)
        for (o <- orderTokens) {
          val parts = o.split(" ")
          if (parts.length > 2 || parts.isEmpty)
            return Left(s"Invalid ORDER BY expression: '$o'")
        }
      }

      Right(parsed)
    } catch {
      case ex: Throwable =>
        Left(s"Query parsing failed: ${ex.getMessage}")
    }
  }

  def parseQuery(raw: String): QueryParts = {
    val cleaned = raw.replaceAll("\\n", " ").replaceAll(" +", " ").trim

    val select =
      """SELECT (.+?) FROM""".r
        .findFirstMatchIn(cleaned)
        .map(_.group(1).split(",").map(_.trim).toSeq)
        .getOrElse(Seq("*"))

    val where =
      """WHERE (.+?)(ORDER BY|$)""".r
        .findFirstMatchIn(cleaned)
        .map(_.group(1).trim)

    val orderBy =
      """ORDER BY (.+)$""".r
        .findFirstMatchIn(cleaned)
        .map(_.group(1).trim)

    val namespaceRegex = """NAMESPACE (.+?)(WHERE|ORDER BY|$)""".r
    val namespaces = namespaceRegex
      .findFirstMatchIn(cleaned)
      .map(_.group(1).trim.split(",").map(_.trim).toSeq)
      .getOrElse(Seq.empty)

    QueryParts(select, where, orderBy, namespaces)
  }

  def resolveQuery(df: DataFrame, query: String): DataFrame = {
    val spark = df.sparkSession

    val schema = df.schema
    val parsed = parseQuery(query)

    val resolvedSelect: Seq[Column] = {
      if (parsed.select.contains("*")) {
        df.columns.toSeq.map(col)
      } else {
        parsed.select.map { colName =>
          val fullPath = SchemaPathHelper.resolveLeaf(colName, schema, parsed.namespaces)
          col(fullPath).alias(colName)
        }
      }
    }

    var resultDF = df.select(resolvedSelect: _*)

    parsed.where.foreach { clause =>
      val conditions = clause.split("AND").map(_.trim).map { expr =>
        val Array(left, right) = expr.split("=").map(_.trim)
        val colPath = SchemaPathHelper.resolveLeaf(left, schema, parsed.namespaces)
        col(colPath) === lit(right.replaceAll("'", ""))
      }
      resultDF = resultDF.filter(conditions.reduce(_ && _))
    }

    parsed.orderBy.foreach { clause =>
      val orders = clause.split(",").map(_.trim).map { o =>
        val parts = o.split(" ")
        val path = SchemaPathHelper.resolveLeaf(parts(0), schema, parsed.namespaces)
        if (parts.length > 1 && parts(1).equalsIgnoreCase("desc")) col(path).desc else col(path)
      }
      resultDF = resultDF.orderBy(orders: _*)
    }

    resultDF
  }
}
