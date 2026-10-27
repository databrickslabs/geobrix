package com.databricks.labs.gbx.vectorx.ds.ogr

import org.gdal.ogr.{DataSource, Layer}

/** The layer of an OGR dataset a read uses: the layer called `name` when `name` is not empty (it overrides
  * the index), otherwise the layer at index `number` (0-based). Schema inference, planning (the feature
  * count) and the reader all pick their layer here, so the three always agree on which layer is read. */
case class OGR_LayerSelection(number: Int, name: String) {

    /** The selected layer of `dataset`, or null when it has no such layer. The layer belongs to `dataset`:
      * it is valid until the dataset is deleted. */
    def open(dataset: DataSource): Layer = if (name.isEmpty) dataset.GetLayer(number) else dataset.GetLayerByName(name)

    /** What the read asked for, for an error message: the layer name, or the index when no name was given. */
    def requested: String = if (name.isEmpty) number.toString else name

    /** "layer '<requested>' not found; layers: <names of the dataset's layers, in index order>". */
    def notFound(dataset: DataSource): String =
        s"layer '$requested' not found; layers: ${OGR_LayerSelection.layerNames(dataset).mkString(", ")}"

}

object OGR_LayerSelection {

    /** The selection the reader options ask for: `layerName` (default none) and `layerNumber` (default 0). */
    def fromOptions(options: Map[String, String]): OGR_LayerSelection =
        OGR_LayerSelection(options.getOrElse("layerNumber", "0").toInt, options.getOrElse("layerName", ""))

    /** The names of the layers of `dataset`, in index order. */
    def layerNames(dataset: DataSource): Seq[String] = (0 until dataset.GetLayerCount()).map(i => dataset.GetLayer(i).GetName())

}
