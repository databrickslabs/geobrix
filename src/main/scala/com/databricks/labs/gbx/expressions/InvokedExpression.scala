package com.databricks.labs.gbx.expressions

import org.apache.spark.sql.catalyst.expressions.{ImplicitCastInputTypes, Literal, RuntimeReplaceable}
import org.apache.spark.sql.types.{DataType, ObjectType}

/**
  * Base trait for GeoBrix expressions that are evaluated by calling a method on a companion object.
  *
  * The catalyst expression is replaced at analysis time with a [[PrettyInvoke]] that invokes
  * the companion's `eval` method with the child expressions.
  * This allows each expression to be implemented as a regular Scala method on the companion
  * while still participating in Spark's optimizer and codegen.
  */
trait InvokedExpression extends RuntimeReplaceable with ImplicitCastInputTypes {

    /** Overrides ImplicitCastInputTypes.inputTypes: one-to-one with children data types. */
    override def inputTypes: Seq[DataType] = children.map(_.dataType)

    /** Builds the runtime invocation: call `methodName` on `companion` with `children` as arguments.
      * Set `nonFoldable=true` for I/O expressions (e.g. rst_fromfile) so Catalyst ConstantFolding
      * never evaluates them on the driver at plan time — they must run at runtime on executors.
      *
      * `propagateNull` defaults to true (Spark's `Invoke` short-circuits to null without calling the
      * method when ANY argument is null). Set it to false for expressions whose `builder()` injects a
      * `Literal(null, ...)` default for an OPTIONAL trailing arg (e.g. rst_clip's clipCrs): with
      * propagateNull=true that legitimately-null optional arg would null the whole result without ever
      * running `eval`. Such expressions MUST instead guard a null primary (tile) arg inside `eval`. */
    def invoke(
        companion: Object,
        methodName: String = "eval",
        nonFoldable: Boolean = false,
        propagateNull: Boolean = true
    ): PrettyInvoke = {
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
          propagateNull = propagateNull,
          returnNullable = true,
          isDeterministic = true,
          nonFoldable = nonFoldable
        )
    }

}
