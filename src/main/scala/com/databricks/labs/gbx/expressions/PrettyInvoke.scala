package com.databricks.labs.gbx.expressions

import org.apache.spark.sql.catalyst.expressions.objects.Invoke
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types.DataType

/**
  * Invoke a method on a target object with the given arguments; used as the runtime replacement
  * for [[InvokedExpression]]. Extends Spark's [[Invoke]] with a readable toString (function name
  * and args) for plans and debugging, and for redacting long literals (e.g. config).
  */
class PrettyInvoke(
    exprName: String,
    targetObject: Expression,
    functionName: String,
    dataType: DataType,
    arguments: Seq[Expression] = Nil,
    methodInputTypes: Seq[DataType] = Nil,
    propagateNull: Boolean = true,
    returnNullable: Boolean = true,
    isDeterministic: Boolean = true,
    nonFoldable: Boolean = false
) extends Invoke(
      targetObject,
      functionName,
      dataType,
      arguments,
      methodInputTypes,
      propagateNull,
      returnNullable,
      isDeterministic
    ) {

    /** When nonFoldable, never let Catalyst ConstantFolding evaluate this at plan time on the
      * DRIVER (Invoke.foldable is otherwise true for all-literal, deterministic args). Required for
      * I/O expressions like rst_fromfile: a literal path must be read at runtime on the EXECUTOR
      * (which has the UC Volume FUSE mount), not folded on the driver. */
    override def foldable: Boolean = if (nonFoldable) false else super.foldable

    /** Overrides toString: readable function name and args; long literals redacted as "literal(...)". */
    override def toString(): String = {
        val args = arguments.map {
            // Guard literal.value != null: an optional-arg default is injected as Literal(null, T)
            // (e.g. rst_clip's clipCrs), and .toString on a null value NPEs. Spark's Literal.toString
            // renders a null value as "null", so the fallthrough `arg.toString()` handles it safely.
            case literal: Literal if literal.value != null && literal.value.toString.length > 20 => s"literal(...)"
            case arg => arg.toString()
        }.mkString(", ")
        val targetClass = targetObject.toString().split("\\$").headOption.getOrElse("Unknown")
        s"$exprName($args)@$targetClass"
    }

    /** Overrides Expression.withNewChildrenInternal: rebuilds PrettyInvoke with new target and arguments. */
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): PrettyInvoke =
        new PrettyInvoke(
          exprName = exprName,
          targetObject = nc.head,
          functionName = functionName,
          dataType = dataType,
          arguments = nc.tail,
          methodInputTypes = methodInputTypes,
          propagateNull = propagateNull,
          returnNullable = returnNullable,
          isDeterministic = isDeterministic,
          nonFoldable = nonFoldable
        )

}
