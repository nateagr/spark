
// scalastyle:off println
package org.apache.spark.sql

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.GenericMapper.MapperFunc
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, FloatType, IntegerType, LongType, Metadata}
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}


class SparkColumnarTest extends SparkFunSuite {
  type ExtensionsBuilder = SparkSessionExtensions => Unit

  private def stop(spark: SparkSession): Unit = {
    spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  private def withSession(f: SparkSession => Unit): Unit = {
    val builder = SparkSession.builder().master("local[1]")
      .config("spark.sql.extensions", "org.apache.spark.sql.GenericMapperExtension")
    val spark = builder.getOrCreate()
    try f(spark) finally {
      stop(spark)
    }
  }

  test("test Spark columnar") {
    val func: MapperFunc = (size, vectors) => {
      val result = new OnHeapColumnVector(size, IntegerType)
      (0 until size).foreach(i =>
        result.putInt(i, vectors.head.getInt(i) * 1000 + vectors(1).getInt(i)))
      result.asInstanceOf[ColumnVector]
    }

    withSession { session =>
      import session.sqlContext.implicits._
      val df = session.read.parquet("/Users/g.racic/spark-udf/display_click_samples")
        .select(GenericMapper(func, IntegerType, 'partnerid, 'contextid),
        'partnerid, 'contextid)
      df.explain(true)
      df.collect().foreach(println)
    }
  }
}

class GenericMapperExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectColumnar(_ => ColumnarMapperRule(ColumnarMapperPreRule()))
  }
}

trait ColExpression extends Expression with Serializable {

  def supportsColumnar: Boolean = true

  def columnarEval(batch: ColumnarBatch): Any = {
    throw new NoSuchMethodException(s"No implementation of columnarEval for ${this.getClass}")
  }

  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    other.isInstanceOf[ColExpression]
  }

  override def hashCode(): Int = super.hashCode()
}

object GenericMapper {
  type MapperFunc = (Int, Seq[ColumnVector]) => ColumnVector

  def apply(func: MapperFunc, outputType: DataType, deps: Column*): Column = {
    new Column(ColMapperExpr(func, deps.map(_.expr), outputType))
  }
}

case class ColMapperExpr(func: MapperFunc,
                         children: Seq[Expression], dataType: DataType)
  extends ColExpression {
  override def columnarEval(batch: ColumnarBatch): Any = {
    func(batch.numRows(), (0 until batch.numCols()).map(idx => batch.column(idx)))
  }
  override def nullable: Boolean = false
  override def eval(input: InternalRow): Any = throw new NoSuchMethodException()
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw new NoSuchMethodException()
}

object ColBindReferences extends Logging {

  def bindReference[A <: ColExpression](expression: A,
                                        input: AttributeSeq,
                                        allowFailures: Boolean = false): A = {
    expression.transform { case a: AttributeReference =>
      val ordinal = input.indexOf(a.exprId)
      if (ordinal == -1) {
        if (allowFailures) {
          a
        } else {
          sys.error(s"Couldn't find $a in ${input.attrs.mkString("[", ",", "]")}")
        }
      } else {
        new ColBoundReference(ordinal, a.dataType, input(ordinal).nullable)
      }
    }.asInstanceOf[A]
  }

  def bindReferences[A <: ColExpression](expressions: Seq[A], input: AttributeSeq): Seq[A] = {
    expressions.map(ColBindReferences.bindReference(_, input))
  }
}

class ColBoundReference(ordinal: Int, dataType: DataType, nullable: Boolean)
  extends BoundReference(ordinal, dataType, nullable) with ColExpression {

  override def columnarEval(batch: ColumnarBatch): Any = {
    batch.column(ordinal)
  }
}

class ColAlias(child: ColExpression, name: String)(
  override val exprId: ExprId = NamedExpression.newExprId,
  override val qualifier: Seq[String] = Seq.empty,
  override val explicitMetadata: Option[Metadata] = None)
  extends Alias(child, name)(exprId, qualifier, explicitMetadata) with ColExpression {

  override def columnarEval(batch: ColumnarBatch): Any = child.columnarEval(batch)
}

class ColAttributeReference(name: String,
                            dataType: DataType,
                            nullable: Boolean = true,
                            override val metadata: Metadata = Metadata.empty)(
                            override val exprId: ExprId = NamedExpression.newExprId,
                            override val qualifier: Seq[String] = Seq.empty[String])
  extends AttributeReference(name, dataType, nullable, metadata)(exprId, qualifier)
    with ColExpression {

  // No columnarEval because they are bound to inputs (i.e. transformed into ColBoundReference)
}

class ColProjectExec(projectList: Seq[NamedExpression], child: SparkPlan)
  extends ProjectExec(projectList, child) {

  override def supportsColumnar: Boolean =
    projectList.forall(_.asInstanceOf[ColExpression].supportsColumnar)

  override def supportCodegen: Boolean = false

  override def doExecuteColumnar() : RDD[ColumnarBatch] = {
    val boundProjectList = ColBindReferences.bindReferences(
      projectList.asInstanceOf[Seq[ColExpression]], child.output)
    child.executeColumnar().mapPartitions(it => BatchIterator(it,
      batch => {
        val newColumns = boundProjectList.map(
          expr => expr.columnarEval(batch).asInstanceOf[ColumnVector]
        ).toArray
        new ColumnarBatch(newColumns, batch.numRows())
      })
    )
  }

  // We have to override equals because subclassing ProjectExec is not that clean
  // One of the issues is that the generated equals will see ColProjectExec and ProjectExec
  // as being equal and this can result in the withNewChildren method not actually replacing
  // anything
  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    other.isInstanceOf[ColProjectExec]
  }

  override def hashCode(): Int = super.hashCode()
}

class NoReplacementException(str: String) extends RuntimeException(str) {}

case class ColumnarMapperPreRule() extends Rule[SparkPlan] {
  def replaceWithColExpression(exp: Expression): ColExpression = exp match {
    case a: Alias => new ColAlias(replaceWithColExpression(a.child),
        a.name)(a.exprId, a.qualifier, a.explicitMetadata)
    case att: AttributeReference =>
      new ColAttributeReference(att.name, att.dataType, att.nullable,
        att.metadata)(att.exprId, att.qualifier)
    case c: ColExpression => c
    case exp =>
      throw new NoReplacementException(s"expression " +
        s"${exp.getClass} ${exp} is not currently supported.")
  }

  def replaceWithColumnarPlan(plan: SparkPlan): SparkPlan =
    try {
      plan match {
        case plan: ProjectExec =>
          new ColProjectExec(plan.projectList.map(exp =>
            replaceWithColExpression(exp).asInstanceOf[NamedExpression]),
            replaceWithColumnarPlan(plan.child))
        case p =>
          logWarning(s"Columnar processing for ${p.getClass} is not currently supported.")
          p.withNewChildren(p.children.map(replaceWithColumnarPlan))
      }
    } catch {
      case exp: NoReplacementException =>
        logWarning(s"Columnar processing for ${plan.getClass} is not currently supported" +
          s"because ${exp.getMessage}")
        plan
    }

  override def apply(plan: SparkPlan): SparkPlan = replaceWithColumnarPlan(plan)
}

case class ColumnarMapperPostRule() extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan match {
    case plan => plan
  }
}

case class ColumnarMapperRule(pre: Rule[SparkPlan],
                              post: Rule[SparkPlan] = ColumnarMapperPostRule())
  extends ColumnarRule {
  override def preColumnarTransitions: Rule[SparkPlan] = pre
  override def postColumnarTransitions: Rule[SparkPlan] = post
}

case class BatchIterator(itr: Iterator[ColumnarBatch], f: ColumnarBatch => ColumnarBatch)
  extends Iterator[ColumnarBatch] {
  var batch: ColumnarBatch = null

  private def closeCurrentBatch(): Unit = {
    if (batch != null) {
      batch.close
      batch = null
    }
  }

  override def hasNext: Boolean = {
    closeCurrentBatch()
    itr.hasNext
  }

  override def next(): ColumnarBatch = {
    closeCurrentBatch()
    batch = f(itr.next())
    batch
  }
}
