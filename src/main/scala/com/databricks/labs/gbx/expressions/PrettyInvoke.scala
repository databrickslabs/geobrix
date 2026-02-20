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
    isDeterministic: Boolean = true
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

    /** Overrides toString: readable function name and args; long literals redacted as "literal(...)". */
    override def toString(): String = {
        val args = arguments.map {
            case literal: Literal if literal.value.toString.length > 20 => s"literal(...)"
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
          isDeterministic = isDeterministic
        )

}
