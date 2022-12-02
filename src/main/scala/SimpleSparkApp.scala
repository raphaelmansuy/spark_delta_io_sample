// import Spark libraries

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import java.sql.Timestamp
import java.time.Instant
import org.apache.spark.sql.functions._
import io.delta.tables.DeltaTable

final case class User(
    id: Int,
    lastName: String,
    firstName: String,
    age: Int,
    numFriends: Int,
    date_created: Timestamp
)

object SimpleSparkApp {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("A sample Spark app")
      // Set master to local if you want to run it locally
      .master("local")
      .config("spark.some.config.option", "config-value")
      // Configure Delta support for Spark
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
      )
      .getOrCreate()

    // User(id, firstName, lastName, age, numFriends, timestamp)

    val columns =
      Seq("id", "firstName", "lastName", "age", "numFriends", "date_created")
    val keyColumns = List("id")
    val partitionColumns = List("age")
    val updatesColumn = "date_created"
    val createdColumn = "date_created"

    // Create a list of users
    val users = (1 to 100).map(_ => Faker.randomUser()).toList
    val userSarah =
      User(1, "Smith", "Sarah", 30, 100, Timestamp.from(Instant.now()))

    // Create a DataFrame from the list of users
    val usersDF = spark.createDataFrame(users ++ List(userSarah))

    // Create dedublicated DataFrame from the list of users that have the same id, choose the latest date_created if there are duplicates rows
    val usersDFDistinct = usersDF
      .orderBy(col(updatesColumn).desc)
      .dropDuplicates(keyColumns)

    // Show the DataFrame
    //
    // +---+-----+---+----------+
    // | id| name|age|numFriends|
    // +---+-----+---+----------+
    // |  1| John| 33|       100|
    // |  2| Mary| 22|       200|
    // |  3|Peter| 44|       300|

    usersDF.show()

    // Show the deduplicated DataFrame
    usersDFDistinct.show()

    // Calculate the size in bytes of the DataFrame in memory using Catalyst
    val sizeInBytes =
      usersDFDistinct.queryExecution.optimizedPlan.stats.sizeInBytes

    // Calculate the size in bytes of the DataFrame in memory using Spark
    println(f"🚀 Size in bytes of the result: $sizeInBytes")
    // Size in MB
    val sizeInMB: Double = (sizeInBytes).toDouble / 1024.0 / 1024.0
    println(f"🚀 Size in MB of the result: $sizeInMB")

    // Calculate the ideal number of partitions, the minimum number of partitions is 1
    val idealNumPartitions = Math.max(1, Math.ceil(sizeInMB / 128).toInt)
    println(f"🚀 Ideal number of partitions: $idealNumPartitions")

    // Repartition the DataFrame
    val usersDFRepartitioned =
      usersDFDistinct.repartition(idealNumPartitions.toInt)

    // Write the DataFrame to parquet
    usersDFRepartitioned.write
      .mode("overwrite")
      .option("compression", "snappy")
      .partitionBy(partitionColumns: _*)
      .mode("overwrite")
      .save("./users.parquet")

    // Read the DataFrame from parquet
    val usersDFRead = spark.read.parquet("./users.parquet")

    // Select the users with name "Sara" using SQL
    println("👉 Select the users with name 'Sara' using SQL")
    usersDFRead.createOrReplaceTempView("users")
    val usersDFReadSara = spark.sql("SELECT * FROM users WHERE firstName = 'Sara'")
    usersDFReadSara.show()

    // Save the usersDFRepartitioned DataFrame as a Delta table
    println("👉 Save the usersDFRepartitioned DataFrame as a Delta table")

    // if the table does not exist, create it
    // Test if the directory exists and if it is empty

    val path = "./users.delta"
    val directory = new java.io.File(path)

    if (!directory.exists || directory.listFiles.isEmpty) {

      print("👉 Creating the Delta table")

      usersDFRepartitioned.write
        .format("delta")
        .option("compression", "snappy")
        .partitionBy(partitionColumns: _*)
        .save(path)

    } else {

      println("👉 The table already exists")
      val targetTable = DeltaTable.forPath(spark, "./users.delta")

      val mergeExpr = targetTable
        .as("target")
        .merge(
          usersDFRepartitioned.as("source"),
          "target.id = source.id  AND target.date_created > source.date_created"
        )
        .whenMatched
        .updateExpr(
          Map(
            "id" -> "source.id",
            "firstName" -> "source.firstName",
            "lastName" -> "source.lastName",
            "age" -> "source.age",
            "numFriends" -> "source.numFriends",
            "date_created" -> "source.date_created"
          )
        )
        .whenNotMatched
        .insertExpr(
          Map(
            "id" -> "source.id",
            "firstName" -> "source.firstName",
            "lastName" -> "source.lastName",
            "age" -> "source.age",
            "numFriends" -> "source.numFriends",
            "date_created" -> "source.date_created"
          )
        )
        .execute()

        // Repartition the Delta table with partionKeys

    }

    // Stop the SparkSession

    spark.stop()

  }
}
