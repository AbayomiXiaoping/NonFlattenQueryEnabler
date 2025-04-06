package com.generic.solution.app

import org.apache.spark.sql.functions.{col, transform}
import org.apache.spark.sql.types.{ArrayType, DataType, StructType}
import org.apache.spark.sql.{Column, DataFrame}

object SchemaPathHelper {

  /**
   * Represents a resolved column path
   *
   * @param column  actual Spark Column
   * @param isArray true if resolution used transform on an array
   */
  case class ResolvedColumn(column: Column, isArray: Boolean)

  /**
   * Recursively search for leaf field name in schema and return fully qualified paths
   */
  def findQualifiedPaths(schema: DataType, path: String = ""): Seq[(String, Boolean)] = {
    schema match {
      case StructType(fields) =>
        fields.flatMap { field =>
          val newPath = if (path.isEmpty) field.name else s"$path.${field.name}"
          findQualifiedPaths(field.dataType, newPath)
        }
      case ArrayType(elementType, _) =>
        findQualifiedPaths(elementType, path + "[]").map(p => (p._1, true))
      case _ => Seq((path, false))
    }
  }

  /**
   * Resolve user query for fieldName and return Spark Column using transform if needed
   */
  def resolveLeafColumn(df: DataFrame, fieldName: String): Option[ResolvedColumn] = {
    val candidates = findQualifiedPaths(df.schema)
    val matching = candidates.filter {
      case (fullPath, _) =>
        fullPath.split("\\.").lastOption.contains(fieldName) || fullPath == fieldName
    }

    matching.headOption.map {
      case (fullPath, _) =>
        val pathParts = fullPath.split("(?<=\\[\\])|\\.").toList // split on dot or after []

        def buildColumn(parts: List[String], currentCol: Column): Column = {
          parts match {
            case Nil => currentCol
            case head :: tail if head.endsWith("[]") =>
              val fieldName = head.stripSuffix("[]")
              val arrayCol = if (currentCol == null) col(fieldName) else currentCol.getField(fieldName)
              transform(arrayCol, x => buildColumn(tail, x))
            case head :: tail =>
              val nextCol = if (currentCol == null) col(head) else currentCol.getField(head)
              buildColumn(tail, nextCol)
          }
        }

        val resolved = buildColumn(pathParts, null)
        ResolvedColumn(resolved, fullPath.contains("[]"))
    }
  }

  /**
   * Select resolved column for SQL compatibility
   */
  def selectResolved(df: DataFrame, fieldName: String): DataFrame = {
    resolveLeafColumn(df, fieldName) match {
      case Some(resolved) => df.select(resolved.column.alias(fieldName))
      case None => throw new IllegalArgumentException(s"Field '$fieldName' not found in schema")
    }
  }

  /**
   * Generate documentation for all leaf fields in schema
   */
  def generateColumnDocumentation(schema: StructType): String = {
    val sb = new StringBuilder

    def recurse(st: StructType, prefix: String): Unit = {
      st.fields.foreach { field =>
        val path = if (prefix.isEmpty) field.name else s"$prefix.${field.name}"
        field.dataType match {
          case struct: StructType => recurse(struct, path)
          case ArrayType(struct: StructType, _) => recurse(struct, s"$path[]")
          case ArrayType(dt, _) => sb.append(s"$path[]: ${dt.simpleString}\n")
          case _ => sb.append(s"$path: ${field.dataType.simpleString}\n")
        }
      }
    }

    recurse(schema, "")
    sb.toString()
  }

  /**
   * Extract all leaf paths
   */
  def extractLeafPaths(schema: StructType, prefix: String = ""): Seq[String] = {
    schema.fields.flatMap { field =>
      val fullPath = if (prefix.isEmpty) field.name else s"$prefix.${field.name}"
      field.dataType match {
        case st: StructType => extractLeafPaths(st, fullPath)
        case ArrayType(st: StructType, _) => extractLeafPaths(st, s"$fullPath[]")
        case _ => Seq(fullPath)
      }
    }
  }

  /**
   * Extract a map of leaf column name to all qualified paths
   */
  def extractColumnMap(schema: StructType): Map[String, Seq[String]] = {
    extractLeafPaths(schema).groupBy(_.split("\\.").last)
  }

  /**
   * Check if fully qualified column path exists
   */
  private def columnExists(schema: StructType, path: String): Boolean = {
    try {
      val parts = path.split("(?<=\\[\\])|\\.")
      parts.foldLeft(schema: DataType) {
        case (struct: StructType, field) if field.endsWith("[]") =>
          val name = field.stripSuffix("[]")
          struct(name).dataType match {
            case ArrayType(innerType, _) => innerType
            case _ => return false
          }

        case (struct: StructType, field) =>
          struct(field).dataType

        case _ => return false
      }
      true
    } catch {
      case _: Exception => false
    }
  }


  /**
   * Try resolving a field name under given namespaces
   */
  def resolveLeaf(leaf: String, schema: StructType, namespaces: Seq[String]): String = {
    val attempts = namespaces.map { ns =>
      if (ns.endsWith(leaf)) ns else s"$ns.$leaf"
    }.map(_.replace("[]", "") /* optional if you only use this in columnExists */) :+ leaf
    // try root-level leaf too

    val validPath = attempts.find(path => columnExists(schema, path))

    validPath.getOrElse {
      throw new IllegalArgumentException(
        s"Column '$leaf' not found under namespaces: ${namespaces.mkString(", ")}. " +
          s"Tried paths: ${attempts.mkString(", ")}"
      )
    }
  }
}
