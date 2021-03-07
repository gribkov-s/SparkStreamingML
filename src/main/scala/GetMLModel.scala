
import org.apache.spark.sql._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, StandardScaler, IndexToString}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator


object GetMLModel extends App {

  val spark = SparkSession.builder
      .master("local[*]")
      .appName("GetMLModel")
      .getOrCreate()

  val data = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/iris-1801-dbdda0.csv")
    .randomSplit(Array(0.7, 0.3))

  val trainingData = data(0)
  val testData = data(1)
  //----------------------------------------------------------------------

  val labelIndexer  = new StringIndexer()
    .setInputCol("species")
    .setOutputCol("label")
    .fit(trainingData)

  val assembler = new VectorAssembler()
    .setInputCols(Array("sepal_length", "sepal_width", "petal_length", "petal_width"))
    .setOutputCol("features")

  val classifier = new LogisticRegression()
    .setMaxIter(20)
    .setRegParam(0.2)
    .setElasticNetParam(0.8)
    .setFeaturesCol("features")
    .setLabelCol("label")

  val scaler = new StandardScaler()
    .setInputCol("features")
    .setOutputCol("scaledFeatures")

  val indexToString = new IndexToString()
    .setInputCol("prediction")
    .setOutputCol("speciesPred")
    .setLabels(labelIndexer.labels)

  //----------------------------------------------------------------------
  val pipeline = new Pipeline()
    .setStages(Array(labelIndexer, assembler, scaler, classifier, indexToString))

  val model = pipeline.fit(trainingData)
  //----------------------------------------------------------------------

  val predicted= model.transform(testData)
  predicted.show()

  val evaluator = new BinaryClassificationEvaluator().setLabelCol("label")
  println(s"areaUnderROC: ${evaluator.evaluate(predicted)}\n")
  //----------------------------------------------------------------------

  model.write.overwrite().save("src/main/resources/model")

}
