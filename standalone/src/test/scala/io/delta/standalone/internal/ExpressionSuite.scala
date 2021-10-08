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

  private def testException[T <: Throwable](
                           f: => Any,
                           messageContains: String)(implicit manifest: Manifest[T]) = {
    val e = intercept[T]{
      f;
    }.getMessage
    assert(e.contains(messageContains))
  }

  test("logical predicates") {
    // AND tests
    testPredicate(
      new And(Literal.ofNull(new BooleanType()), Literal.False))
    testPredicate(
      new And(Literal.False, Literal.ofNull(new BooleanType())))
    testPredicate(
      new And(Literal.True, Literal.ofNull(new BooleanType())))
    testPredicate(
      new And(Literal.ofNull(new BooleanType()), Literal.ofNull(new BooleanType())))
    testPredicate(new And(Literal.False, Literal.False), expectedResult = Some(false))
    testPredicate(new And(Literal.True, Literal.False), expectedResult = Some(false))
    testPredicate(new And(Literal.False, Literal.True), expectedResult = Some(false))
    testPredicate(new And(Literal.True, Literal.True), expectedResult = Some(true))
    testException[IllegalArgumentException](
      new And(Literal.of(1), Literal.of(2)).eval(null),
      "'And' expression children.eval results must be Booleans")
    testException[IllegalArgumentException]( // is this desired behavior?
      new And(Literal.False, Literal.ofNull(new NullType())),
      "BinaryOperator left and right DataTypes must be the same")

    // OR tests
    testPredicate(
      new Or(Literal.ofNull(new BooleanType()), Literal.False), None)
    testPredicate(
      new Or(Literal.False, Literal.ofNull(new BooleanType())), None)
    testPredicate(
      new Or(Literal.ofNull(new BooleanType()), Literal.ofNull(new BooleanType())), None)
    testPredicate(
      new Or(Literal.ofNull(new BooleanType()), Literal.ofNull(new BooleanType())), None)
    testPredicate(new Or(Literal.False, Literal.False), expectedResult = Some(false))
    testPredicate(new Or(Literal.True, Literal.False), expectedResult = Some(true))
    testPredicate(new Or(Literal.False, Literal.True), expectedResult = Some(true))
    testPredicate(new Or(Literal.True, Literal.True), expectedResult = Some(true))
    // TODO: is this what we want? should it fail upon creation instead of eval???
    testException[IllegalArgumentException](
      new Or(Literal.of(1), Literal.of(2)).eval(null),
      "'Or' expression left.eval and right.eval results must be Booleans")
    testException[IllegalArgumentException](
      new Or(Literal.False, Literal.ofNull(new NullType())),
      "BinaryOperator left and right DataTypes must be the same") // is this desired behavior?

    // NOT tests
    testPredicate(new Not(Literal.False), expectedResult = Some(true))
    testPredicate(new Not(Literal.True), expectedResult = Some(false))
    testPredicate(new Not(Literal.ofNull(new BooleanType())), None)
    testException[IllegalArgumentException](
      new Not(Literal.of(1)).eval(null),
      "'Not' expression child.eval result must be a Boolean")
  }

  // TODO: do we need to test the (x,  null), (null, x) = null for every BinaryComparison?
  //  what about for dataType != dataType?

  test("comparison predicates") {
    // (small, big)
    val literals = Seq(
      (Literal.of(1), Literal.of(2), Literal.of(1)), // IntegerType
      (Literal.of(1.0F), Literal.of(2.0F), Literal.of(1.0F)), // FloatType
      (Literal.of(1L), Literal.of(2L), Literal.of(1L)), // LongType
      (Literal.of(1.toShort), Literal.of(2.toShort), Literal.of(1.toShort)), // ShortType
      (Literal.of(1.0), Literal.of(2.0), Literal.of(1.0)), // DoubleType
      (Literal.of(1.toByte), Literal.of(2.toByte), Literal.of(1.toByte)), // ByteType
      (Literal.of(new BigDecimalJ("0.1")), Literal.of(new BigDecimalJ("0.2")),
        Literal.of(new BigDecimalJ("0.1"))), // DecimalType
      (Literal.False, Literal.True, Literal.False), // BooleanType
      (Literal.of(new TimestampJ(0)), Literal.of(new TimestampJ(1000000)),
      Literal.of(new TimestampJ(0))), // TimestampType
      (Literal.of(new DateJ(0)), Literal.of(new DateJ(1000000)),
        Literal.of(new DateJ(0))), // DateType
      (Literal.of("apples"), Literal.of("oranges"), Literal.of("apples")), // StringType
      (Literal.of("apples".getBytes()), Literal.of("oranges".getBytes()), Literal.of("apples".getBytes())), // BinaryType
      // todo: add additional tests for custom implemented binary comparisons?
    )

    val predicates = Seq(
      ((a: Literal, b: Literal) => new LessThan(a, b), (true, false, false)),
      ((a: Literal, b: Literal) => new LessThanOrEqual(a, b), (true, false, true)),
      ((a: Literal, b: Literal) => new GreaterThan(a, b), (false, true, false)),
      ((a: Literal, b: Literal) => new GreaterThanOrEqual(a, b), (false, true, true)),
      ((a: Literal, b: Literal) => new EqualTo(a, b), (false, false, true)),
    )

    literals.foreach { case (small, big, small2) =>
      predicates.foreach { case (predicateCreator, (smallBig, bigSmall, smallSmall)) =>
        testPredicate(predicateCreator(small, big), Some(smallBig))
        testPredicate(predicateCreator(big, small), Some(bigSmall))
        testPredicate(predicateCreator(small, small2), Some(smallSmall))
        }
    }
    // todo: future work--should we support automatic casting between compatible types?
  }

  test("null predicates") {
    // ISNOTNULL tests
    testPredicate(new IsNotNull(Literal.ofNull(new NullType())), Some(false))
    testPredicate(new IsNotNull(Literal.ofNull(new BooleanType())), Some(false))
    testPredicate(new IsNotNull(Literal.False), Some(true))

    // ISNULL tests
    testPredicate(new IsNull(Literal.ofNull(new NullType())), Some(true))
    testPredicate(new IsNull(Literal.ofNull(new BooleanType())), Some(true))
    testPredicate(new IsNull(Literal.False), Some(false))
  }

  test("In predicate") {
    // IN TESTS
    testException[IllegalArgumentException](
      new In(null, List(Literal.True, Literal.True).asJava),
      "'In' expression 'value' cannot be null")
    testException[IllegalArgumentException](
      new In(Literal.True, null),
      "'In' expression 'elems' cannot be null")
    testException[IllegalArgumentException](
      new In(Literal.True, List().asJava),
      "'In' expression 'elems' cannot be empty")
    // mismatched DataTypes throws exception
    testException[IllegalArgumentException](
      new In(Literal.of(1), List(Literal.True, Literal.True).asJava),
      "In expression 'elems' and 'value' must all be of the same DataType")
    testException[IllegalArgumentException](
      new In(Literal.True, List(Literal.of(1), Literal.True).asJava),
      "In expression 'elems' and 'value' must all be of the same DataType")
    // value.eval() null -> null
    testPredicate(new In(Literal.ofNull(new BooleanType()), List(Literal.True).asJava), None)
    // value in list (w/ null in  list)
    testPredicate(new In(Literal.True, List(Literal.True,
      Literal.ofNull(new BooleanType())).asJava),
      Some(true))
    // value not in list (w/ null in list)
    testPredicate(new In(Literal.False, List(Literal.True,
      Literal.ofNull(new BooleanType())).asJava),
      None)
    // test correct output
    // todo: test all types? uses comparator same as the other comparison expressions
    testPredicate( new In(Literal.of(1),
      (0 to 10).map{Literal.of(_)}.asJava), Some(true))
    testPredicate( new In(Literal.of(100),
      (0 to 10).map{Literal.of(_)}.asJava), Some(false))
    testPredicate( new In(Literal.of(10),
      (0 to 10).map{Literal.of(_)}.asJava), Some(true))
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
    testLiteral(Literal.of(8.toByte), Some(8.toByte))
    testLiteral(Literal.of(1.0), Some(1.0))
    testLiteral(Literal.of(2.0F), Some(2.0F))
    testLiteral(Literal.of(5), Some(5))
    testLiteral(Literal.of(10L), Some(10L))
    testLiteral(Literal.ofNull(new NullType()), None)
    testLiteral(Literal.ofNull(new BooleanType()), None)
    testLiteral(Literal.of(5.toShort), Some(5.toShort))
    testLiteral(Literal.of("test"), Some("test"))
    val curr_time = System.currentTimeMillis()
    testLiteral(
      Literal.of(new TimestampJ(curr_time)), Some(new TimestampJ(curr_time)))
    testLiteral(Literal.of(new DateJ(curr_time)), Some(new DateJ(curr_time)))
    testLiteral(Literal.of(new BigDecimalJ("0.1")),
      Some(new BigDecimalJ("0.1")))
    assert(ArraysJ.equals(
      Literal.of("test".getBytes()).eval(null).asInstanceOf[Array[Byte]],
      "test".getBytes()))
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
    testException[IllegalArgumentException](
      new Column("testArray", new ArrayType(new BooleanType(), true)),
      "Can't create a column with DataType: ArrayType")
    testException[IllegalArgumentException](
      new Column("testMap", new MapType(new StringType(), new StringType(),true)),
      "Can't create a column with DataType: MapType")
    testException[IllegalArgumentException](
      new Column("testStruct", new StructType(Array(new StructField("test", new BooleanType())))),
      "Can't create a column with DataType: StructType")
  }

  private def buildPartitionRowRecord(dataType: DataType, nullable: Boolean, value: String) = {
    new PartitionRowRecord(new StructType(Array(new StructField("test", dataType, nullable))),
      Map("test" -> value))
  }

  test("PartitionRowRecord tests") {
    val testPartitionRowRecord = buildPartitionRowRecord(new IntegerType(), true, "5")
    assert(buildPartitionRowRecord(new IntegerType(), true, "").isNullAt("test"))
    assert(buildPartitionRowRecord(new IntegerType(), true, null).isNullAt("test"))

    assert(!testPartitionRowRecord.isNullAt("test"))
    testException[IllegalArgumentException](
      testPartitionRowRecord.isNullAt("foo"),
      "requirement failed")

    // field does not exist
    // should this be tested for every getter?
    testException[IllegalArgumentException](
      testPartitionRowRecord.getInt("foo"),
      "requirement failed")

    // test wrong DataType
    // should this be tested for every getter?
    testException[ClassCastException](
      testPartitionRowRecord.getLong("test"),
      "Mismatched DataType for Field")

    //primitive types can't be null (per rowrecord interface?)
    val primTypes = Seq(
      (new IntegerType(), (x: PartitionRowRecord) => x.getInt("test"), "0", 0),
      (new LongType(), (x: PartitionRowRecord) => x.getLong("test"), "0", 0L),
      (new ByteType(), (x: PartitionRowRecord) => x.getByte("test"), "0", 0.toByte),
      (new ShortType(), (x: PartitionRowRecord) => x.getShort("test"), "0", 0.toShort),
      (new BooleanType(), (x: PartitionRowRecord) => x.getBoolean("test"), "true", true),
      (new FloatType(), (x: PartitionRowRecord) => x.getFloat("test"), "0", 0.0F),
      (new DoubleType(), (x: PartitionRowRecord) => x.getDouble("test"), "0.0", 0.0)
    )

    primTypes.foreach { case (dataType: DataType, f: (PartitionRowRecord => Any), s: String, v) =>
      assert(f(buildPartitionRowRecord(dataType, true, s)) == v)
      testException[NullPointerException](
        f(buildPartitionRowRecord(dataType, true, "")),
        "null value found for primitive DataType"
      )
    }

    val curr_time = System.currentTimeMillis()
    // non primitive types can be null ONLY when nullable (test both)
    val nonPrimTypes = Seq(
      (new StringType(), (x: PartitionRowRecord) => x.getString("test"), "foo", "foo"),
      (DecimalType.USER_DEFAULT, (x: PartitionRowRecord) => x.getBigDecimal("test"), "0.01",
        new BigDecimalJ("0.01")),
      (new TimestampType(), (x: PartitionRowRecord) => x.getTimestamp("test"),
        (new TimestampJ(curr_time)).toString(), new TimestampJ(curr_time)),
      (new DateType(), (x: PartitionRowRecord) => x.getDate("test"), "1970-01-01",
        DateJ.valueOf("1970-01-01"))
    )
    nonPrimTypes.foreach { case (dataType: DataType, f: (PartitionRowRecord => Any), s: String, v) =>
      assert(Objects.equals(f(buildPartitionRowRecord(dataType, true, s)), v))
      assert(f(buildPartitionRowRecord(dataType, true, "")) == null)
      testException[NullPointerException](
        f(buildPartitionRowRecord(dataType, false, "")),
        "null value found for non-nullable Field"
      )
    }

    assert(buildPartitionRowRecord(new BinaryType(), true, "").getBinary("test") == null)
    assert(buildPartitionRowRecord(new BinaryType(), true, "\u0001\u0002").getBinary("test")
      sameElements Array(1.toByte, 2.toByte))
    testException[NullPointerException](
      buildPartitionRowRecord(new BinaryType(), false, "").getBinary("test"),
      "null value found for non-nullable Field")

    testException[UnsupportedOperationException](
      testPartitionRowRecord.getRecord("test"),
      "Struct is not a supported partition type.")
    testException[UnsupportedOperationException](
      testPartitionRowRecord.getList("test"),
      "Array is not a supported partition type.")
    intercept[UnsupportedOperationException](
      testPartitionRowRecord.getMap("test"),
      "Map is not a supported partition type.")
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

    val f1Expr1 = new EqualTo(schema.column("col1"), Literal.of(0))
    val f1Expr2 = new EqualTo(schema.column("col2"), Literal.of(1))
    val f1 = new And(f1Expr1, f1Expr2)

    testPartitionFilter(schema, inputFiles, f1 :: Nil, add01 :: Nil)
    testPartitionFilter(schema, inputFiles, f1Expr1 :: f1Expr2 :: Nil, add01 :: Nil)

    val f2Expr1 = new LessThan(schema.column("col1"), Literal.of(1))
    val f2Expr2 = new LessThan(schema.column("col2"), Literal.of(1))
    val f2 = new And(f2Expr1, f2Expr2)
    testPartitionFilter(schema, inputFiles, f2 :: Nil, add00 :: Nil)
    testPartitionFilter(schema, inputFiles, f2Expr1 :: f2Expr2 :: Nil, add00 :: Nil)

    val f3Expr1 = new EqualTo(schema.column("col1"), Literal.of(2))
    val f3Expr2 = new LessThan(schema.column("col2"), Literal.of(1))
    val f3 = new Or(f3Expr1, f3Expr2)
    testPartitionFilter(schema, inputFiles, f3 :: Nil, Seq(add20, add21, add22, add00, add10))

    val inSet4 = (2 to 10).map(Literal.of(_)).asJava
    val f4 = new In(schema.column("col1"), inSet4)
    testPartitionFilter(schema, inputFiles, f4 :: Nil, add20 :: add21 :: add22 :: Nil)

    val inSet5 = (100 to 110).map(Literal.of(_)).asJava
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
