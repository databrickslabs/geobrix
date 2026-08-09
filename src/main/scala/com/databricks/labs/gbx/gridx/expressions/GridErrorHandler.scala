package com.databricks.labs.gbx.gridx.expressions

import scala.util.control.NonFatal

/** Shared degrade guard for GridX expression companions.
  *
  * GridX has no metadata carrier, so a bad-DATA condition degrades to NULL — the shape's
  * null. This helper wraps ONLY the data-touching work (cell-id parse, coordinate
  * encode/decode, geometry build); parameter validation (resolution range, grid spec,
  * arity, argument type) must be done and allowed to raise BEFORE calling `safeEval`, so a
  * usage error is never silently swallowed into an all-NULL column.
  *
  * `NonFatal`, not `Throwable`: OutOfMemoryError / StackOverflowError / InterruptedException
  * must propagate and fail the task. */
object GridErrorHandler {

    /** Run `body`; on a NonFatal exception return `nullValue` (the return type's null). */
    def safeEval[T](nullValue: T)(body: => T): T =
        try body
        catch { case NonFatal(_) => nullValue }
}
