package com.databricks.labs.gbx.util

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class SysUtilsTest extends AnyFunSuite {

    // ========== runCommand Tests ==========

    test("runCommand should execute simple echo command") {
        val result = SysUtils.runCommand(Seq("echo", "hello"))
        
        val (stdout1, stdout2, stderr) = result
        stdout1 should include ("hello")
        stdout2 should include ("hello")
        stderr shouldBe empty
    }

    test("runCommand should capture stdout") {
        val result = SysUtils.runCommand(Seq("echo", "test output"))
        
        val (_, stdout, _) = result
        stdout.trim shouldBe "test output"
    }

    test("runCommand should return empty stderr for successful command") {
        val result = SysUtils.runCommand(Seq("echo", "success"))
        
        val (_, _, stderr) = result
        stderr shouldBe empty
    }

    test("runCommand should handle multi-word output") {
        val result = SysUtils.runCommand(Seq("echo", "multiple", "words", "here"))
        
        val (stdout1, stdout2, _) = result
        stdout1 should include ("multiple")
        stdout1 should include ("words")
        stdout2 shouldBe stdout1
    }

    test("runCommand should execute true command") {
        val result = SysUtils.runCommand(Seq("true"))
        
        val (stdout1, stdout2, stderr) = result
        stdout1 shouldBe empty
        stdout2 shouldBe empty
        stderr shouldBe empty
    }

    test("runCommand should return tuple with three elements") {
        val result = SysUtils.runCommand(Seq("echo", "test"))
        
        result shouldBe a [(_,_,_)]
        result._1 should not be null
        result._2 should not be null
        result._3 should not be null
    }

    // ========== runScript Tests ==========

    test("runScript should execute simple echo script") {
        val result = SysUtils.runScript(Array("echo", "hello"))
        
        val (exitCode, stdout, stderr) = result
        exitCode shouldBe "0"
        stdout should include ("hello")
        stderr shouldBe empty
    }

    test("runScript should return 0 exit code for successful command") {
        val result = SysUtils.runScript(Array("echo", "success"))
        
        val (exitCode, _, _) = result
        exitCode shouldBe "0"
    }

    test("runScript should capture stdout from script") {
        val result = SysUtils.runScript(Array("echo", "test output"))
        
        val (_, stdout, _) = result
        stdout.trim shouldBe "test output"
    }

    test("runScript should execute true command") {
        val result = SysUtils.runScript(Array("true"))
        
        val (exitCode, stdout, stderr) = result
        exitCode shouldBe "0"
        stdout shouldBe empty
        stderr shouldBe empty
    }

    test("runScript should handle multi-word arguments") {
        val result = SysUtils.runScript(Array("echo", "first", "second", "third"))
        
        val (exitCode, stdout, _) = result
        exitCode shouldBe "0"
        stdout should include ("first")
        stdout should include ("second")
        stdout should include ("third")
    }

    test("runScript should return tuple with three string elements") {
        val result = SysUtils.runScript(Array("echo", "test"))
        
        result shouldBe a [(_,_,_)]
        result._1 should not be null
        result._2 should not be null
        result._3 should not be null
    }

    // ========== getLastOutputLine Tests ==========

    test("getLastOutputLine should extract last line from single line output") {
        val prompt = ("0", "hello world", "")
        
        val lastLine = SysUtils.getLastOutputLine(prompt)
        
        lastLine shouldBe "hello world"
    }

    test("getLastOutputLine should extract last line from multi-line output") {
        val prompt = ("0", "line1\nline2\nline3", "")
        
        val lastLine = SysUtils.getLastOutputLine(prompt)
        
        lastLine shouldBe "line3"
    }

    test("getLastOutputLine should handle output with newline at end") {
        val prompt = ("0", "line1\nline2\n", "")
        
        val lastLine = SysUtils.getLastOutputLine(prompt)
        
        // Split on \n returns array with empty string at end, so .last is ""
        // But actually the implementation returns the last element after split
        // which for "line1\nline2\n" is "" (empty string after final \n)
        // However, Scala's split behavior is: "line1\nline2\n".split("\n") = Array("line1", "line2")
        // The trailing \n is consumed, not creating an empty element
        lastLine shouldBe "line2"
    }

    test("getLastOutputLine should return empty string for empty output") {
        val prompt = ("0", "", "")
        
        val lastLine = SysUtils.getLastOutputLine(prompt)
        
        lastLine shouldBe ""
    }

    test("getLastOutputLine should ignore stderr") {
        val prompt = ("0", "stdout output", "stderr output")
        
        val lastLine = SysUtils.getLastOutputLine(prompt)
        
        lastLine shouldBe "stdout output"
    }

    test("getLastOutputLine should ignore exit code") {
        val prompt = ("1", "output line", "")
        
        val lastLine = SysUtils.getLastOutputLine(prompt)
        
        lastLine shouldBe "output line"
    }

    test("getLastOutputLine should work with runCommand output") {
        val cmdResult = SysUtils.runCommand(Seq("echo", "test"))
        
        val lastLine = SysUtils.getLastOutputLine(cmdResult)
        
        lastLine.trim shouldBe "test"
    }

    test("getLastOutputLine should work with runScript output") {
        val scriptResult = SysUtils.runScript(Array("echo", "script output"))
        
        val lastLine = SysUtils.getLastOutputLine(scriptResult)
        
        lastLine.trim shouldBe "script output"
    }

    // ========== Integration Tests ==========

    test("runCommand and getLastOutputLine should work together") {
        val result = SysUtils.runCommand(Seq("echo", "first\nsecond\nthird"))
        val lastLine = SysUtils.getLastOutputLine(result)
        
        lastLine should include ("third")
    }

    test("runScript and getLastOutputLine should work together") {
        val result = SysUtils.runScript(Array("echo", "line1\nline2"))
        val lastLine = SysUtils.getLastOutputLine(result)
        
        lastLine should not be empty
    }

}
