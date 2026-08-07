package com.databricks.labs.gbx.rasterx.expressions.spectral

import com.databricks.labs.gbx.expressions.{ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.expressions.RST_MapAlgebra
import com.databricks.labs.gbx.rasterx.util.RST_ExpressionUtil
import com.databricks.labs.gbx.util.SerializationUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.MapData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/**
  * Generic spectral-index dispatcher.
  *
  * Takes a named formula (e.g. ``"ndvi"``, ``"gndvi"``, ``"msavi"``) plus a
  * ``MAP<STRING, INT>`` band map that wires the formula's named bands to
  * 1-based band indices in the input tile. Returns a single-band Float32 GTiff
  * tile, same shape as the rest of the spectral-index family.
  *
  * Built-in formulae (case-insensitive name):
  *   - ``ndvi``  -> ``(NIR - Red) / (NIR + Red)``                          bands: ``red``, ``nir``
  *   - ``gndvi`` -> ``(NIR - Green) / (NIR + Green)``                      bands: ``green``, ``nir``
  *   - ``msavi`` -> ``(2*NIR + 1 - sqrt((2*NIR+1)^2 - 8*(NIR-Red))) / 2``  bands: ``red``, ``nir``
  *   - ``ndvi_re`` -> ``(NIR - RedEdge) / (NIR + RedEdge)``                bands: ``red_edge``, ``nir``
  *   - ``ndmi`` -> ``(NIR - SWIR) / (NIR + SWIR)``                         bands: ``nir``, ``swir`` (also covers NBR)
  *   - ``ndsi`` -> ``(Green - SWIR) / (Green + SWIR)``                     bands: ``green``, ``swir``
  *
  * Built-ins are intentionally a small curated set; users with custom
  * formulae can drop down to ``gbx_rst_mapalgebra`` directly. All built-ins
  * delegate to ``RST_MapAlgebra`` for per-pixel evaluation.
  */
case class RST_Index(
    tile: Expression,
    formulaNameExpr: Expression,
    bandMapExpr: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(
        tile, formulaNameExpr, bandMapExpr, ExpressionConfigExpr()
    )
    override def inputTypes: Seq[DataType] = Seq(
        tile.dataType, StringType, MapType(StringType, IntegerType), StringType
    )
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tile)
    override def nullable: Boolean = true
    override def prettyName: String = RST_Index.name
    override def replacement: Expression = invoke(RST_Index)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2))

}

object RST_Index extends WithExpressionInfo {

    /**
      * Built-in index registry.
      *
      *   - ``calc`` is the per-pixel formula with placeholders like ``{red}``,
      *     ``{nir}`` etc.; each placeholder gets substituted with the alias
      *     letter (A, B, ...) that the corresponding band index is wired to.
      *   - ``bands`` is the ordered list of band names the formula expects;
      *     the band map must supply each one (matching is case-insensitive).
      */
    private case class IndexDef(calc: String, bands: Seq[String])

    private val Registry: Map[String, IndexDef] = Map(
        "ndvi"    -> IndexDef("({nir}-{red})/({nir}+{red})",
                              Seq("red", "nir")),
        "gndvi"   -> IndexDef("({nir}-{green})/({nir}+{green})",
                              Seq("green", "nir")),
        "msavi"   -> IndexDef("(2*{nir}+1-sqrt((2*{nir}+1)**2-8*({nir}-{red})))/2",
                              Seq("red", "nir")),
        "ndvi_re" -> IndexDef("({nir}-{red_edge})/({nir}+{red_edge})",
                              Seq("red_edge", "nir")),
        "ndmi"    -> IndexDef("({nir}-{swir})/({nir}+{swir})",
                              Seq("nir", "swir")),
        "ndsi"    -> IndexDef("({green}-{swir})/({green}+{swir})",
                              Seq("green", "swir"))
    )

    def eval(row: InternalRow, formulaName: UTF8String, bandMap: MapData, conf: UTF8String): InternalRow =
        runDispatch(row, formulaName, bandMap, conf, BinaryType)

    private def runDispatch(
        row: InternalRow, formulaName: UTF8String, bandMap: MapData, conf: UTF8String, dt: DataType
    ): InternalRow = {
        val nameStr = if (formulaName == null) null else formulaName.toString
        val bandMapScala = if (bandMap == null) Map.empty[String, Int]
                           else SerializationUtil.createMap[String, Int](bandMap)
        SpectralIndexSpec.runRasterCalc(row, conf, dt) { calcDs =>
            execute(calcDs, nameStr, bandMapScala)
        }
    }

    /** Pure compute path - extracted for direct unit-testing without Spark. */
    def execute(ds: Dataset, formulaName: String, bandMap: Map[String, Int]): (Dataset, Map[String, String]) = {
        require(ds != null, "RST_Index.execute: source Dataset is null")
        require(formulaName != null && formulaName.nonEmpty,
            "RST_Index.execute: formula_name required")
        require(bandMap != null && bandMap.nonEmpty,
            "RST_Index.execute: band_map required (e.g. map('red', 1, 'nir', 2))")
        // scalastyle:off caselocale
        val key = formulaName.toLowerCase
        // Normalize band-map keys to lowercase so MAP('Red', 1) matches the registry.
        val bandMapLc = bandMap.map { case (k, v) => k.toLowerCase -> v }
        // scalastyle:on caselocale
        val ix = Registry.getOrElse(key, throw new IllegalArgumentException(
            s"gbx_rst_index: unknown formula '$formulaName'. Known: ${Registry.keys.toSeq.sorted.mkString(", ")}"
        ))
        ix.bands.foreach { b =>
            require(bandMapLc.contains(b),
                s"gbx_rst_index: formula '$formulaName' requires band '$b' in band_map; got keys ${bandMapLc.keys.toSeq.sorted.mkString(", ")}")
        }
        // Assign A, B, C... to bands in declared order.
        val aliasFor: Map[String, String] = ix.bands.zipWithIndex.map {
            case (band, i) => band -> ('A' + i).toChar.toString
        }.toMap
        val calc = ix.bands.foldLeft(ix.calc) { (acc, b) =>
            acc.replace("{" + b + "}", aliasFor(b))
        }
        val aliases: Seq[(String, Int)] = ix.bands.map(b => aliasFor(b) -> bandMapLc(b))
        val spec = SpectralIndexSpec.singleSourceSpec(calc, aliases)
        RST_MapAlgebra.execute(Seq(ds), Map.empty, spec)
    }

    /** Names of all built-in formulae (for docs / errors). */
    def builtinFormulae: Seq[String] = Registry.keys.toSeq.sorted

    override def name: String = "gbx_rst_index"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 3 => RST_Index(c(0), c(1), c(2))
        case n => throw new IllegalArgumentException(
            s"gbx_rst_index takes 3 arguments (tile, formula_name, band_map); got $n"
        )
    }

}
