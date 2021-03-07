
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.PipelineModel


object MLOnStream extends App {

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("MLOnStream")
    .getOrCreate()

  import spark.implicits._

  val model = PipelineModel.load("src/main/resources/model")

  val inputStream = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("subscribe", "iris_no_class_topic")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()

  val inputSchema = new StructType()
    .add("sepal_length", DoubleType)
    .add("sepal_width", DoubleType)
    .add("petal_length", DoubleType)
    .add("petal_width", DoubleType)


  val inputData = inputStream
    .select(from_json($"value".cast("string"), inputSchema).as("data"))
    .select("data.*")

  val outputData = model.transform(inputData)
    .select($"sepal_length", $"sepal_width", $"petal_length", $"petal_width", $"speciesPred".as("species"))
    .selectExpr("CAST(current_timestamp() AS STRING) AS key", "to_json(struct(*)) AS value")

  outputData.writeStream
    .format("kafka")
    .outputMode("append")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("topic", "iris_with_class_topic")
    .option("checkpointLocation","checkpoints")
    .start()
    .awaitTermination()

}
