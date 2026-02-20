package com.databricks.labs.gbx.expressions

import org.apache.spark.sql.catalyst.expressions.{ImplicitCastInputTypes, Literal, RuntimeReplaceable}
import org.apache.spark.sql.types.{BinaryType, DataType, ObjectType, StringType}

/**
  * Base trait for GeoBrix expressions that are evaluated by calling a method on a companion object.
  *
  * The catalyst expression is replaced at analysis time with a [[PrettyInvoke]] that invokes
  * the companion's `eval` (or `evalPath`/`evalBinary` for raster tiles) with the child expressions.
  * This allows each expression to be implemented as a regular Scala method on the companion
  * while still participating in Spark's optimizer and codegen.
  */
trait InvokedExpression extends RuntimeReplaceable with ImplicitCastInputTypes {

    /** Overrides ImplicitCastInputTypes.inputTypes: one-to-one with children data types. */
    override def inputTypes: Seq[DataType] = children.map(_.dataType)

    /** Builds the runtime invocation: call `methodName` on `companion` with `children` as arguments. */
    def invoke(companion: Object, methodName: String = "eval"): PrettyInvoke = {
        val moduleLiteral = Literal.create(
          companion,
          ObjectType(companion.getClass)
        )

        // Invoke the companion's method at runtime; Spark passes serialized child values.
        new PrettyInvoke(
          exprName = companion.asInstanceOf[WithExpressionInfo].name,
          targetObject = moduleLiteral,
          functionName = methodName,
          dataType = dataType,
          arguments = children,
          methodInputTypes = inputTypes,
          propagateNull = true,
          returnNullable = true,
          isDeterministic = true
        )
    }

    /** Raster tile dispatch: use evalPath (path-based tile) or evalBinary (binary tile) by tile type. */
    def rstInvoke(companion: Object, rdt: DataType): PrettyInvoke = {
        rdt match {
            case StringType => invoke(companion, "evalPath")
            case BinaryType => invoke(companion, "evalBinary")
        }
    }

}
