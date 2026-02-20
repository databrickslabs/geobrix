package com.databricks.labs.gbx.expressions

import com.fasterxml.jackson.databind.ObjectMapper

/**
 * Loads function metadata (e.g. examples for DESCRIBE FUNCTION EXTENDED) from
 * a JSON resource generated from doc examples (single-copy pattern).
 * See docs/scripts/generate-function-info.py.
 */
object FunctionInfoLoader {

  private val ResourcePath = "/com/databricks/labs/gbx/function-info.json"

  private lazy val map: Map[String, FunctionInfo] = load()

  /** Loads function metadata from the bundled JSON resource. */
  private def load(): Map[String, FunctionInfo] = {
    val stream = Option(getClass.getResourceAsStream(ResourcePath))
    stream match {
      case None => Map.empty
      case Some(s) =>
        try {
          val mapper = new ObjectMapper
          val root = mapper.readTree(s)
          val functions = root.get("functions")
          if (functions == null || !functions.isObject) return Map.empty
          val it = functions.fields
          val b = Map.newBuilder[String, FunctionInfo]
          while (it.hasNext) {
            val e = it.next
            val name = e.getKey
            if (name.startsWith("_")) () else {
              val node = e.getValue
              val examples = if (node.has("examples")) Option(node.get("examples").asText()) else None
              if (examples.nonEmpty) b += name -> FunctionInfo(examples = examples)
            }
          }
          b.result()
        } finally s.close()
    }
  }

  /** Returns metadata for the given function name (e.g. examples for DESCRIBE FUNCTION EXTENDED), if present. */
  def get(name: String): Option[FunctionInfo] = map.get(name)
}

/** Case class holding optional examples string for a function. Used by FunctionInfoLoader for DESCRIBE FUNCTION EXTENDED. */
case class FunctionInfo(examples: Option[String] = None)
