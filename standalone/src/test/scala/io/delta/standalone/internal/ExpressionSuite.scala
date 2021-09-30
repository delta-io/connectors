package io.delta.standalone.internal

import java.math.{BigDecimal => BigDecimalJ}
import java.util.{Objects, Arrays => ArraysJ}
import java.sql.{Date, Timestamp}
import scala.collection.JavaConverters._

import io.delta.standalone.data.RowRecord
import io.delta.standalone.expressions._
import io.delta.standalone.internal.actions.AddFile
import io.delta.standalone.types._

// scalastyle:off funsuite
import org.scalatest.FunSuite

// scalastyle:off println
class ExpressionSuite extends FunSuite {
  // scalastyle:on funsuite

  private def testPredicate(
      predicate: Expression,
      expectedResult: Option[Boolean] = None,
      record: RowRecord = null) = {
//    println(predicate.toString())
//    println(predicate.eval(record))
    assert(predicate.eval(record) == expectedResult.getOrElse(null))
  }

  test("logical predicates") {
    // AND tests
    testPredicate(
      new And(Literal.of(null, new BooleanType()), Literal.False))
    testPredicate(
      new And(Literal.False, Literal.of(null, new BooleanType())))
    testPredicate(
      new And(Literal.of(true, new BooleanType()), Literal.of(null, new BooleanType())))
    testPredicate(
      new And(Literal.of(null, new BooleanType()), Literal.of(null, new BooleanType())))
    testPredicate(new And(Literal.False, Literal.False), expectedResult = Some(false))
    testPredicate(new And(Literal.True, Literal.False), expectedResult = Some(false))
    testPredicate(new And(Literal.False, Literal.True), expectedResult = Some(false))
    testPredicate(new And(Literal.True, Literal.True), expectedResult = Some(true))
    intercept[RuntimeException] {
      new And(Literal.of(1, new IntegerType()), Literal.of(2, new IntegerType())).eval(null)
    }
    intercept[RuntimeException] {
      new And(Literal.False, Literal.of(null, new NullType())) // is this desired behavior?
    }

    // OR tests
    testPredicate(
      new Or(Literal.of(null, new BooleanType()), Literal.False), None)
    testPredicate(
      new Or(Literal.False, Literal.of(null, new BooleanType())), None)
    testPredicate(
      new Or(Literal.of(true, new BooleanType()), Literal.of(null, new BooleanType())), None)
    testPredicate(
      new Or(Literal.of(null, new BooleanType()), Literal.of(null, new BooleanType())), None)
    testPredicate(new Or(Literal.False, Literal.False), expectedResult = Some(false))
    testPredicate(new Or(Literal.True, Literal.False), expectedResult = Some(true))
    testPredicate(new Or(Literal.False, Literal.True), expectedResult = Some(true))
    testPredicate(new Or(Literal.True, Literal.True), expectedResult = Some(true))
    // TODO: is this what we want? should it fail upon creation instead of eval???
    intercept[RuntimeException] {
      new Or(Literal.of(1, new IntegerType()), Literal.of(2, new IntegerType())).eval(null)
    }
    intercept[RuntimeException] {
      new Or(Literal.False, Literal.of(null, new NullType())) // is this desired behavior?
    }

    // NOT tests
    testPredicate(new Not(Literal.False), expectedResult = Some(true))
    testPredicate(new Not(Literal.True), expectedResult = Some(false))
    testPredicate(new Not(Literal.of(null, new BooleanType())), None)
    intercept[RuntimeException] {
      new Not(Literal.of(1, new IntegerType())).eval(null)
    }
  }

  // TODO: do we need to test the (x,  null), (null, x) = null for every BinaryComparison?
  // what about for dataType != dataType?


  test("comparison predicates") {
    // EQUALTO tests
    // IntegerType
    testPredicate(
      new EqualTo(Literal.of(1, new IntegerType()), Literal.of(1, new IntegerType())),
      expectedResult = Some(true))
    testPredicate(
      new EqualTo(Literal.of(1, new IntegerType()), Literal.of(2, new IntegerType())),
      expectedResult = Some(false))
    testPredicate(
      new EqualTo(Literal.of(2, new IntegerType()), Literal.of(1, new IntegerType())),
      expectedResult = Some(false))
    // BooleanType
    testPredicate(new EqualTo(Literal.True, Literal.True), expectedResult = Some(true))
    testPredicate(new EqualTo(Literal.True, Literal.False), expectedResult = Some(false))
    testPredicate(new EqualTo(Literal.False, Literal.True), expectedResult = Some(false))
    // FloatType
    // TODO: future work--should we support automatic casting between compatible types?
    testPredicate(
      new EqualTo(Literal.of(1.0F, new FloatType()), Literal.of(1.0F, new FloatType())),
      expectedResult = Some(true))
    testPredicate(
      new EqualTo(Literal.of(1.0F, new FloatType()), Literal.of(2.0F, new FloatType())),
      expectedResult = Some(false))
    testPredicate(
      new EqualTo(Literal.of(2.0F, new FloatType()), Literal.of(1.0F, new FloatType())),
      expectedResult = Some(false))
    // LongType
    testPredicate(
      new EqualTo(Literal.of(1L, new LongType()), Literal.of(1L, new LongType())),
      expectedResult = Some(true))
    testPredicate(
      new EqualTo(Literal.of(1L, new LongType()), Literal.of(2L, new LongType())),
      expectedResult = Some(false))
    testPredicate(
      new EqualTo(Literal.of(2L, new LongType()), Literal.of(1L, new LongType())),
      expectedResult = Some(false))
    // ByteType
    // ShortType
    // DoubleType
    testPredicate(
      new EqualTo(Literal.of(1.0, new DoubleType()), Literal.of(1.0, new DoubleType())),
      expectedResult = Some(true))
    testPredicate(
      new EqualTo(Literal.of(1.0, new DoubleType()), Literal.of(2.0, new DoubleType())),
      expectedResult = Some(false))
    testPredicate(
      new EqualTo(Literal.of(2.0, new DoubleType()), Literal.of(1.0, new DoubleType())),
      expectedResult = Some(false))
    // DecimalType
    // TimestampType
    // DateType

    // LESSTHAN tests
    // IntegerType
    testPredicate(
      new LessThan(Literal.of(1, new IntegerType()), Literal.of(1, new IntegerType())),
      expectedResult = Some(false))
    testPredicate(
      new LessThan(Literal.of(1, new IntegerType()), Literal.of(2, new IntegerType())),
      expectedResult = Some(true))
    testPredicate(
      new LessThan(Literal.of(2, new IntegerType()), Literal.of(1, new IntegerType())),
      expectedResult = Some(false))
    // BooleanType
    // FloatType
    // LongType
    // ByteType
    // ShortType
    // DoubleType
    // DecimalType
    // TimestampType
    // DateType

    // LESSTHANOREQUAL tests
    // IntegerType
    testPredicate(
      new LessThanOrEqual(Literal.of(1, new IntegerType()), Literal.of(1, new IntegerType())),
      expectedResult = Some(true))
    testPredicate(
      new LessThanOrEqual(Literal.of(1, new IntegerType()), Literal.of(2, new IntegerType())),
      expectedResult = Some(true))
    testPredicate(
      new LessThanOrEqual(Literal.of(2, new IntegerType()), Literal.of(1, new IntegerType())),
      expectedResult = Some(false))
    // BooleanType
    // FloatType
    // LongType
    // ByteType
    // ShortType
    // DoubleType
    // DecimalType
    // TimestampType
    // DateType

    // GREATERTHAN tests
    // IntegerType
    testPredicate(
      new GreaterThan(Literal.of(1, new IntegerType()), Literal.of(1, new IntegerType())),
      expectedResult = Some(false))
    testPredicate(
      new GreaterThan(Literal.of(1, new IntegerType()), Literal.of(2, new IntegerType())),
      expectedResult = Some(false))
    testPredicate(
      new GreaterThan(Literal.of(2, new IntegerType()), Literal.of(1, new IntegerType())),
      expectedResult = Some(true))
    // BooleanType
    // FloatType
    // LongType
    // ByteType
    // ShortType
    // DoubleType
    // DecimalType
    // TimestampType
    // DateType

    // GREATERTHANOREQUAL tests
    // IntegerType
    testPredicate(
      new GreaterThanOrEqual(Literal.of(1, new IntegerType()), Literal.of(1, new IntegerType())),
      expectedResult = Some(true))
    testPredicate(
      new GreaterThanOrEqual(Literal.of(1, new IntegerType()), Literal.of(2, new IntegerType())),
      expectedResult = Some(false))
    testPredicate(
      new GreaterThanOrEqual(Literal.of(2, new IntegerType()), Literal.of(1, new IntegerType())),
      expectedResult = Some(true))
    // BooleanType
    // FloatType
    // LongType
    // ByteType
    // ShortType
    // DoubleType
    // DecimalType
    // TimestampType
    // DateType
  }

  test("null predicates") {
    // ISNOTNULL tests
    testPredicate(new IsNotNull(Literal.of(null, new NullType())), Some(false))
    testPredicate(new IsNotNull(Literal.of(null, new BooleanType())), Some(false))
    testPredicate(new IsNotNull(Literal.False), Some(true))

    // ISNULL tests
    testPredicate(new IsNull(Literal.of(null, new NullType())), Some(true))
    testPredicate(new IsNull(Literal.of(null, new BooleanType())), Some(true))
    testPredicate(new IsNull(Literal.False), Some(false))
  }

  test("in predicate") {
    // IN TESTS
    // TODO: test all types? uses comparator same as the other comparison expressions
    // value == null throws exception
    intercept[IllegalArgumentException] {
      new In(null, List(Literal.True, Literal.True).asJava)
    }
    // elems == null throws exception
    intercept[IllegalArgumentException] {
      new In(Literal.True, null)
    }
    // empty elems throws exception
    intercept[IllegalArgumentException] {
      new In(Literal.True, List().asJava)
    }
    // mismatched DataTypes throws exception
    intercept[IllegalArgumentException] {
      new In(Literal.of(1, new IntegerType()), List(Literal.True, Literal.True).asJava)
    }
    intercept[IllegalArgumentException] {
      new In(Literal.True, List(Literal.of(1, new IntegerType()), Literal.True).asJava)
    }
    // value.eval() can't be null
    intercept[IllegalArgumentException] {
      new In(new Literal(null, new BooleanType()), List(Literal.True).asJava).eval(null)
    }
    // an element.eval() can't be null
    intercept[IllegalArgumentException] {
      new In(Literal.True, List(new Literal(null, new BooleanType())).asJava).eval(null)
    }
    // test correct output
    testPredicate( new In(Literal.of(1, new IntegerType()),
      (0 to 10).map{Literal.of(_, new IntegerType)}.asJava), Some(true))
    testPredicate( new In(Literal.of(100, new IntegerType()),
      (0 to 10).map{Literal.of(_, new IntegerType)}.asJava), Some(false))
    testPredicate( new In(Literal.of(10, new IntegerType()),
      (0 to 10).map{Literal.of(_, new IntegerType)}.asJava), Some(true))
  }

  private def testLiteral(literal: Literal, expectedResult: Option[Any]) = {
        println(literal.toString())
        println(literal.eval(null))
        println(expectedResult.getOrElse(null))
    assert(Objects.equals(literal.eval(null), expectedResult.getOrElse(null)))
  }

  test("literal tests") {
    // LITERAL tests
    testLiteral(Literal.True, Some(true))
    testLiteral(Literal.False, Some(false))
    testLiteral(Literal.of(8.toByte, new ByteType()), Some(8.toByte))
    testLiteral(Literal.of(1.0, new DoubleType()), Some(1.0))
    testLiteral(Literal.of(2.0F, new FloatType()), Some(2.0F))
    testLiteral(Literal.of(5, new IntegerType()), Some(5))
    testLiteral(Literal.of(10L, new LongType()), Some(10L))
    testLiteral(Literal.of(null, new NullType()), None)
    // test null with different DataTypes?
    testLiteral(Literal.of(5.toShort, new ShortType()), Some(5.toShort))
    testLiteral(Literal.of("test", new StringType()), Some("test"))
    val curr_time = System.currentTimeMillis()
    testLiteral(
      Literal.of(new Timestamp(curr_time), new TimestampType()), Some(new Timestamp(curr_time)))
    testLiteral(Literal.of(new Date(curr_time), new DateType()), Some(new Date(curr_time)))
    testLiteral(Literal.of(new BigDecimalJ("0.1"), new DecimalType(1, 1)),
      Some(new BigDecimalJ("0.1")))
    assert(ArraysJ.equals(
      Literal.of("test".getBytes(), new BinaryType()).eval(null).asInstanceOf[Array[Byte]],
      "test".getBytes()))
    testLiteral(Literal.of(List(1, 2, 3).asJava, new ArrayType(new IntegerType(), false)),
      Some(List(1, 2, 3).asJava))
    testLiteral(
      Literal.of(Map("a"-> 1, "b"->2).asJava,
        new MapType(new StringType(), new IntegerType(), false)),
      Some(Map("a"-> 1, "b"->2).asJava))
    // TODO: StructType & RowRecord

    // TODO: test validate literal (nested maps  / arrays included)
  }

  test("column tests") {
    // COLUMN tests
  }


  // combined expressions & expression tree tests
  // to do'S FOR TESTING? / LOOK through added code to find stuff that needs to be tested?
  // filterFileList (see below)


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


  test("basic partition filter") {
//    val schema = new StructType(Array(
//      new StructField("col1", new IntegerType()),
//      new StructField("col2", new IntegerType())))
//
//    val add00 = AddFile("1", Map("col1" -> "0", "col2" -> "0"), 0, 0, dataChange = true)
//    val add01 = AddFile("2", Map("col1" -> "0", "col2" -> "1"), 0, 0, dataChange = true)
//    val add02 = AddFile("2", Map("col1" -> "0", "col2" -> "2"), 0, 0, dataChange = true)
//    val add10 = AddFile("3", Map("col1" -> "1", "col2" -> "0"), 0, 0, dataChange = true)
//    val add11 = AddFile("4", Map("col1" -> "1", "col2" -> "1"), 0, 0, dataChange = true)
//    val add12 = AddFile("4", Map("col1" -> "1", "col2" -> "2"), 0, 0, dataChange = true)
//    val add20 = AddFile("4", Map("col1" -> "2", "col2" -> "0"), 0, 0, dataChange = true)
//    val add21 = AddFile("4", Map("col1" -> "2", "col2" -> "1"), 0, 0, dataChange = true)
//    val add22 = AddFile("4", Map("col1" -> "2", "col2" -> "2"), 0, 0, dataChange = true)
//    val inputFiles = Seq(add00, add01, add02, add10, add11, add12, add20, add21, add22)
//
//    val f1Expr1 = new EqualTo(schema.column("col1"), Literal.of(0))
//    val f1Expr2 = new EqualTo(schema.column("col2"), Literal.of(1))
//    val f1 = new And(f1Expr1, f1Expr2)
//
//    testPartitionFilter(schema, inputFiles, f1 :: Nil, add01 :: Nil)
//    testPartitionFilter(schema, inputFiles, f1Expr1 :: f1Expr2 :: Nil, add01 :: Nil)
//
//    val f2Expr1 = new LessThan(schema.column("col1"), Literal.of(1))
//    val f2Expr2 = new LessThan(schema.column("col2"), Literal.of(1))
//    val f2 = new And(f2Expr1, f2Expr2)
//    testPartitionFilter(schema, inputFiles, f2 :: Nil, add00 :: Nil)
//    testPartitionFilter(schema, inputFiles, f2Expr1 :: f2Expr2 :: Nil, add00 :: Nil)
//
//    val f3Expr1 = new EqualTo(schema.column("col1"), Literal.of(2))
//    val f3Expr2 = new LessThan(schema.column("col2"), Literal.of(1))
//    val f3 = new Or(f3Expr1, f3Expr2)
//    testPartitionFilter(schema, inputFiles, f3 :: Nil, Seq(add20, add21, add22, add00, add10))
//
//    val inSet4 = (2 to 10).map(Literal.of).asJava
//    val f4 = new In(schema.column("col1"), inSet4)
//    testPartitionFilter(schema, inputFiles, f4 :: Nil, add20 :: add21 :: add22 :: Nil)
//
//    val inSet5 = (100 to 110).map(Literal.of).asJava
//    val f5 = new In(schema.column("col1"), inSet5)
//    testPartitionFilter(schema, inputFiles, f5 :: Nil, Nil)
  }

  test("not null partition filter") {
    val schema = new StructType(Array(
      new StructField("col1", new IntegerType(), true),
      new StructField("col2", new IntegerType(), true)))

    val add0Null = AddFile("1", Map("col1" -> "0", "col2" -> null), 0, 0, dataChange = true)
    val addNull1 = AddFile("1", Map("col1" -> null, "col2" -> "1"), 0, 0, dataChange = true)
    val inputFiles = Seq(add0Null, addNull1)

    val f1 = new IsNotNull(schema.column("col1"))
    testPartitionFilter(schema, inputFiles, f1 :: Nil, add0Null :: Nil)
  }
}
