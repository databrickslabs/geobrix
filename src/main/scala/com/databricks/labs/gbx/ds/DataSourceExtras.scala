package com.databricks.labs.gbx.ds

import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters._

/**
  * Trait for data sources that need to inject extra options (e.g. from Spark config) into
  * TableProvider/Table options when resolving tables.
  */
trait DataSourceExtras {

    /** Extra options to merge with the provided options (e.g. from dsExtraMap(checkMap)). */
    def dsExtraMap(checkMap: Map[String, String] = Map.empty): Map[String, String]

    /** Merges dsExtraMap into the given Java properties and returns as java.util.Map. */
    def extraJavaUtilMap(properties: java.util.Map[String, String]): java.util.Map[String, String] = {
        val cMap = properties.asScala.toMap
        val newMap = cMap ++ dsExtraMap(checkMap = cMap)
        newMap.asJava
    }

    /** Merges dsExtraMap into the given options and returns a new CaseInsensitiveStringMap. */
    def extraCaseInsensitiveStringMap(options: CaseInsensitiveStringMap): CaseInsensitiveStringMap = {
        val cMap = options.asCaseSensitiveMap().asScala.toMap
        val newMap = cMap ++ dsExtraMap(checkMap = cMap)
        new CaseInsensitiveStringMap(newMap.asJava)
    }

}
