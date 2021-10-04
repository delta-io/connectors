package io.delta.standalone.internal

import java.math.{BigDecimal => BigDecimalJ}
import java.util.{Objects, Arrays => ArraysJ}
import java.sql.{Date => DateJ, Timestamp => TimestampJ}

import scala.collection.JavaConverters._

import io.delta.standalone.data.RowRecord
import io.delta.standalone.expressions._
import io.delta.standalone.internal.actions.AddFile
import io.delta.standalone.internal.data.PartitionRowRecord
import io.delta.standalone.internal.exception.DeltaErrors
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
    intercept[IllegalArgumentException] {
      new And(Literal.of(1, new IntegerType()), Literal.of(2, new IntegerType())).eval(null)
    }
    intercept[IllegalArgumentException] {
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
    intercept[IllegalArgumentException] {
      new Or(Literal.of(1, new IntegerType()), Literal.of(2, new IntegerType())).eval(null)
    }
    intercept[IllegalArgumentException] {
      new Or(Literal.False, Literal.of(null, new NullType())) // is this desired behavior?
    }

    // NOT tests
    testPredicate(new Not(Literal.False), expectedResult = Some(true))
    testPredicate(new Not(Literal.True), expectedResult = Some(false))
    testPredicate(new Not(Literal.of(null, new BooleanType())), None)
    intercept[IllegalArgumentException] {
      new Not(Literal.of(1, new IntegerType())).eval(null)
    }
  }

  // TODO: do we need to test the (x,  null), (null, x) = null for every BinaryComparison?
  //  what about for dataType != dataType?


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
    // todo: future work--should we support automatic casting between compatible types?
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

  test("In predicate") {
    // IN TESTS
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
    // todo: test all types? uses comparator same as the other comparison expressions
    testPredicate( new In(Literal.of(1, new IntegerType()),
      (0 to 10).map{Literal.of(_, new IntegerType)}.asJava), Some(true))
    testPredicate( new In(Literal.of(100, new IntegerType()),
      (0 to 10).map{Literal.of(_, new IntegerType)}.asJava), Some(false))
    testPredicate( new In(Literal.of(10, new IntegerType()),
      (0 to 10).map{Literal.of(_, new IntegerType)}.asJava), Some(true))
  }

  private def testLiteral(literal: Literal, expectedResult: Option[Any]) = {
//        println(literal.toString())
//        println(literal.eval(null))
        println(expectedResult.getOrElse(null))
    assert(Objects.equals(literal.eval(null), expectedResult.getOrElse(null)))
  }

  test("Literal tests") {
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
      Literal.of(new TimestampJ(curr_time), new TimestampType()), Some(new TimestampJ(curr_time)))
    testLiteral(Literal.of(new DateJ(curr_time), new DateType()), Some(new DateJ(curr_time)))
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
    // TODO: if we're only filtering on partition columns, do we need literals (and columns)
    //  of Array, Map or Record?

    // test ValidateLiteral
    // TODO: can we have null of any DataType? (add tests for maps, arrays, record etc pending ^^)
    intercept[IllegalArgumentException] {
      Literal.of(Array(1, 2), new BinaryType())
    }
    intercept[IllegalArgumentException] {
      Literal.of(Array(1, 2), new BooleanType())
    }
    intercept[IllegalArgumentException] {
      Literal.of(Array(1, 2), new ByteType())
    }
    intercept[IllegalArgumentException] {
      Literal.of(Array(1, 2), new DateType())
    }
    intercept[IllegalArgumentException] {
      Literal.of(Array(1, 2), DecimalType.USER_DEFAULT)
    }
    intercept[IllegalArgumentException] {
      Literal.of(1, new DoubleType())
    }
    intercept[IllegalArgumentException] {
      Literal.of(1.0, new FloatType())
    }
    intercept[IllegalArgumentException] {
      Literal.of(1.0, new IntegerType())
    }
    intercept[IllegalArgumentException] {
      Literal.of(Array(1, 2), new LongType())
    }
    intercept[IllegalArgumentException] {
      Literal.of(1, new NullType())
    }
    intercept[IllegalArgumentException] {
      Literal.of(Array(1, 2), new ShortType())
    }
    intercept[IllegalArgumentException] {
      Literal.of(Array(1, 2), new StringType())
    }
    intercept[IllegalArgumentException] {
      Literal.of(Array(1, 2), new TimestampType())
    }
  }

  private def testColumn(fieldName: String,
                         dataType: DataType,
                         record: RowRecord,
                        expectedResult: Option[Any]) = {
//    println((new Column(fieldName, dataType)).eval(record))
//    println(expectedResult.getOrElse(null))
    assert(Objects.equals(new Column(fieldName, dataType).eval(record),
      expectedResult.getOrElse(null)))
  }

  test("Column tests") {
    // COLUMN tests
    val schema = new StructType(Array(
      new StructField("testInt", new IntegerType(), true),
      new StructField("testLong", new LongType(), true),
      new StructField("testByte", new ByteType(), true),
      new StructField("testShort", new ShortType(), true),
      new StructField("testBoolean", new BooleanType(), true),
      new StructField("testFloat", new FloatType(), true),
      new StructField("testDouble", new DoubleType(), true),
      new StructField("testString", new StringType(), true),
      new StructField("testBinary", new BinaryType(), true),
      new StructField("testDecimal", DecimalType.USER_DEFAULT, true),
      new StructField("testTimestamp", new TimestampType(), true),
      new StructField("testDate", new DateType(), true)))
    val partRowRecord = new PartitionRowRecord(schema,
      Map("testInt"->"1",
        "testLong"->"10",
        "testByte" ->"8",
      "testShort" -> "100",
        "testBoolean" -> "true",
        "testFloat" -> "20.0",
        "testDouble" -> "22.0",
        "testString" -> "onetwothree",
        "testBinary" -> "\u0001\u0005\u0008",
        "testDecimal" -> "0.123",
        "testTimestamp" -> (new TimestampJ(12345678)).toString(),
        "testDate" -> "1970-01-01"))
    testColumn("testInt", new IntegerType(), partRowRecord, Some(1))
    testColumn("testLong", new LongType(), partRowRecord, Some(10L))
    testColumn("testByte", new ByteType(), partRowRecord, Some(8.toByte))
    testColumn("testShort", new ShortType(), partRowRecord, Some(100.toShort))
    testColumn("testBoolean", new BooleanType(), partRowRecord, Some(true))
    testColumn("testFloat", new FloatType(), partRowRecord, Some(20.0F))
    testColumn("testDouble", new DoubleType(), partRowRecord, Some(22.0))
    testColumn("testString", new StringType(), partRowRecord, Some("onetwothree"))
    assert(Array(1.toByte, 5.toByte, 8.toByte) sameElements
      (new Column("testBinary", new BinaryType())).eval(partRowRecord).asInstanceOf[Array[Byte]])
    testColumn("testDecimal", DecimalType.USER_DEFAULT, partRowRecord,
      Some(new BigDecimalJ("0.123")))
    testColumn("testTimestamp", new TimestampType(), partRowRecord, Some(new TimestampJ(12345678)))
    testColumn("testDate", new DateType(), partRowRecord, Some(new DateJ(70, 0, 1)))
    // test struct, array, map with regular partition row record??
  }

  private def buildPartitionRowRecord(dataType: DataType, nullable: Boolean, value: String) = {
    new PartitionRowRecord(new StructType(Array(new StructField("test", dataType, nullable))),
      Map("test" -> value))
  }

  test("PartitionRowRecord tests") {
    val testPartitionRowRecord = buildPartitionRowRecord(new IntegerType(), true, "5")
    assert(buildPartitionRowRecord(new IntegerType(), true, "").isNullAt("test"))
    assert(!testPartitionRowRecord.isNullAt("test"))
    intercept[IllegalArgumentException] {
      testPartitionRowRecord.isNullAt("foo")
    }

    // field does not exist
    // should this be tested for every getter?
    intercept[IllegalArgumentException]{
      testPartitionRowRecord.getInt("foo")
    }
    // test wrong DataType
    // should this be tested for every getter?
    intercept[ClassCastException]{
      println(testPartitionRowRecord.getLong("test"))
    }

    //primitive types can't be null (per rowrecord interface?)
    //getInt
    assert(buildPartitionRowRecord(new IntegerType(), true, "0").getInt("test") == 0)
    assert(buildPartitionRowRecord(new IntegerType(), true, "5").getInt("test") == 5)
    assert(buildPartitionRowRecord(new IntegerType(), true, "-5").getInt("test") == -5)
    intercept[NullPointerException]{
      buildPartitionRowRecord(new IntegerType(), true, "").getInt("test")
    }
    //getLong
    assert(buildPartitionRowRecord(new LongType(), true, "0").getLong("test") == 0L)
    assert(buildPartitionRowRecord(new LongType(), true, "5").getLong("test") == 5L)
    assert(buildPartitionRowRecord(new LongType(), true, "-5").getLong("test") == -5L)
    intercept[NullPointerException]{
      buildPartitionRowRecord(new LongType(), true, "").getLong("test")
    }
    //getByte
    //long
    //byte
    //short
    //boolean
    //float
    //double

    // non primitive types can be null ONLY when nullable (test both)
    //string
    //binary
    assert(buildPartitionRowRecord(new BinaryType(), true, "").getBinary("test") == null)
    assert(buildPartitionRowRecord(new BinaryType(), true, "\u0001\u0002").getBinary("test")
      sameElements Array(1.toByte, 2.toByte))
    assert(buildPartitionRowRecord(new BinaryType(), true, "\u0000").getBinary("test")
      sameElements Array(0.toByte))
    intercept[NullPointerException]{
      buildPartitionRowRecord(new BinaryType(), false, "").getBinary("test")
    }
    //bigdecimal
    //timestamp
    //date

    intercept[UnsupportedOperationException]{
      testPartitionRowRecord.getRecord("test")
    }
    intercept[UnsupportedOperationException]{
      testPartitionRowRecord.getList("test")
    }
    intercept[UnsupportedOperationException]{
      testPartitionRowRecord.getMap("test")
    }
  }

  // TODO: add nested expression tree tests?

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
    val schema = new StructType(Array(
      new StructField("col1", new IntegerType()),
      new StructField("col2", new IntegerType())))

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

    val f1Expr1 = new EqualTo(schema.column("col1"), Literal.of(0, new IntegerType()))
    val f1Expr2 = new EqualTo(schema.column("col2"), Literal.of(1, new IntegerType()))
    val f1 = new And(f1Expr1, f1Expr2)

    testPartitionFilter(schema, inputFiles, f1 :: Nil, add01 :: Nil)
    testPartitionFilter(schema, inputFiles, f1Expr1 :: f1Expr2 :: Nil, add01 :: Nil)

    val f2Expr1 = new LessThan(schema.column("col1"), Literal.of(1, new IntegerType()))
    val f2Expr2 = new LessThan(schema.column("col2"), Literal.of(1, new IntegerType()))
    val f2 = new And(f2Expr1, f2Expr2)
    testPartitionFilter(schema, inputFiles, f2 :: Nil, add00 :: Nil)
    testPartitionFilter(schema, inputFiles, f2Expr1 :: f2Expr2 :: Nil, add00 :: Nil)

    val f3Expr1 = new EqualTo(schema.column("col1"), Literal.of(2, new IntegerType()))
    val f3Expr2 = new LessThan(schema.column("col2"), Literal.of(1, new IntegerType()))
    val f3 = new Or(f3Expr1, f3Expr2)
    testPartitionFilter(schema, inputFiles, f3 :: Nil, Seq(add20, add21, add22, add00, add10))

    val inSet4 = (2 to 10).map(Literal.of(_, new IntegerType())).asJava
    val f4 = new In(schema.column("col1"), inSet4)
    testPartitionFilter(schema, inputFiles, f4 :: Nil, add20 :: add21 :: add22 :: Nil)

    val inSet5 = (100 to 110).map(Literal.of(_, new IntegerType())).asJava
    val f5 = new In(schema.column("col1"), inSet5)
//    testPartitionFilter(schema, inputFiles, f5 :: Nil, Nil)
  }

  // TODO: add additional partition filter tests

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
