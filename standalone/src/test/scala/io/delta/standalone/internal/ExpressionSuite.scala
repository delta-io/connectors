package io.delta.standalone.internal

import scala.collection.JavaConverters._

import io.delta.standalone.data.RowRecord
import io.delta.standalone.expressions._
import io.delta.standalone.internal.actions.AddFile
import io.delta.standalone.types.{IntegerType, StructField, StructType}

// scalastyle:off funsuite
import org.scalatest.FunSuite

// scalastyle:off println
class ExpressionSuite extends FunSuite {
  // scalastyle:on funsuite

  private def testPredicate(
      predicate: Expression,
      expectedResult: Boolean,
      record: RowRecord = null) = {
    println(predicate.toString())
    println(predicate.eval(record))
    assert(predicate.eval(record) == expectedResult)
  }

  private def testPartitionFilter(
      inputSchema: StructType,
      inputFiles: Seq[AddFile],
      filters: Seq[Expression],
      expectedMatchedFiles: Seq[AddFile]) = {
    println("filters:\n\t" + filters.map(_.toString()).mkString("\n\t"))
    val matchedFiles = DeltaLogImpl.filterFileList(inputSchema, inputFiles, filters)
    assert(matchedFiles.length == expectedMatchedFiles.length)
    assert(matchedFiles.forall(expectedMatchedFiles.contains(_)))
  }

  test("basic predicate") {
    testPredicate(new And(Literal.False, Literal.False), expectedResult = false)
    testPredicate(new And(Literal.True, Literal.False), expectedResult = false)
    testPredicate(new And(Literal.False, Literal.True), expectedResult = false)
    testPredicate(new And(Literal.True, Literal.True), expectedResult = true)

    testPredicate(new EqualTo(Literal.of(1), Literal.of(1)), expectedResult = true)
    testPredicate(new EqualTo(Literal.of(1), Literal.of(2)), expectedResult = false)

    testPredicate(new LessThan(Literal.of(1), Literal.of(1)), expectedResult = false)
    testPredicate(new LessThan(Literal.of(1), Literal.of(2)), expectedResult = true)

    val inSet = (0 to 10).map(Literal.of).asJava
    testPredicate(new In(Literal.of(1), inSet), expectedResult = true)
    testPredicate(new In(Literal.of(100), inSet), expectedResult = false)
  }

  test("basic partition filter") {
    val add00 = AddFile("1", Map("col1" -> "0", "col2" -> "0"), 0, 0, dataChange = true)
    val add01 = AddFile("2", Map("col1" -> "0", "col2" -> "1"), 0, 0, dataChange = true)
    val add02 = AddFile("2", Map("col1" -> "0", "col2" -> "2"), 0, 0, dataChange = true)
    val add10 = AddFile("3", Map("col1" -> "1", "col2" -> "0"), 0, 0, dataChange = true)
    val add11 = AddFile("4", Map("col1" -> "1", "col2" -> "1"), 0, 0, dataChange = true)
    val add12 = AddFile("4", Map("col1" -> "1", "col2" -> "2"), 0, 0, dataChange = true)
    val add20 = AddFile("4", Map("col1" -> "2", "col2" -> "0"), 0, 0, dataChange = true)
    val add21 = AddFile("4", Map("col1" -> "2", "col2" -> "1"), 0, 0, dataChange = true)
    val add22 = AddFile("4", Map("col1" -> "2", "col2" -> "2"), 0, 0, dataChange = true)
    val inputFiles = Seq(add00, add01, add02, add10, add11, add12, add20, add21, add22)

    val schema = new StructType(Array(
      new StructField("col1", new IntegerType()),
      new StructField("col2", new IntegerType())))

    val f1Expr1 = new EqualTo(new Column("col1"), Literal.of(0))
    val f1Expr2 = new EqualTo(new Column("col2"), Literal.of(1))
    val f1 = new And(f1Expr1, f1Expr2)

    testPartitionFilter(schema, inputFiles, f1 :: Nil, add01 :: Nil)
    testPartitionFilter(schema, inputFiles, f1Expr1 :: f1Expr2 :: Nil, add01 :: Nil)

    val f2Expr1 = new LessThan(new Column("col1"), Literal.of(1))
    val f2Expr2 = new LessThan(new Column("col2"), Literal.of(1))
    val f2 = new And(f2Expr1, f2Expr2)
    testPartitionFilter(schema, inputFiles, f2 :: Nil, add00 :: Nil)
    testPartitionFilter(schema, inputFiles, f2Expr1 :: f2Expr2 :: Nil, add00 :: Nil)

    val f3Expr1 = new EqualTo(new Column("col1"), Literal.of(2))
    val f3Expr2 = new LessThan(new Column("col2"), Literal.of(1))
    val f3 = new Or(f3Expr1, f3Expr2)
    testPartitionFilter(schema, inputFiles, f3 :: Nil, Seq(add20, add21, add22, add00, add10))

    val inSet4 = (2 to 10).map(Literal.of).asJava
    val f4 = new In(new Column("col1"), inSet4)
    testPartitionFilter(schema, inputFiles, f4 :: Nil, add20 :: add21 :: add22 :: Nil)

    val inSet5 = (100 to 110).map(Literal.of).asJava
    val f5 = new In(new Column("col1"), inSet5)
    testPartitionFilter(schema, inputFiles, f5 :: Nil, Nil)
  }
}
