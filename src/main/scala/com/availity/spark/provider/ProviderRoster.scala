package com.availity.spark.provider

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StructType, StructField, DateType, StringType, LongType}
import org.apache.spark.sql.functions.{count, lit, array, collect_list, col, month, avg, map_from_entries, map_from_arrays, struct, sum}

import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory

object ProviderRoster  {	

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    
    var spark: SparkSession = null

    try 
      {
        // Step 1: Read application and transformation configs
        val applicationConfig: Config = ConfigFactory.parseResources("application-dev.conf")
        val transformationsConfig: Config = ConfigFactory.parseResources("transformation-dev.conf")
        logger.info("Step 1: Application and transformation configurations successfully retrieved.")

        // Step 2: Initialize SparkSession
        val spark = SparkSession.builder()
          .appName(applicationConfig.getString("app.spark_session.appName"))
          .master(applicationConfig.getString("app.spark_session.master"))
          .getOrCreate()
        logger.info("Step 2: SparkSession initialized.")
 
        // Step 3: Read input files (providers.csv and visits.csv) to DataFrames
        val provider_df: DataFrame = read_csv_file_to_df(spark = spark, conf = applicationConfig.getConfig("app.provider_csv"))
        val visit_df   : DataFrame = read_csv_file_to_df(spark = spark, conf = applicationConfig.getConfig("app.visit_csv"))

        // Step 4: Register the DataFrames as tables
        provider_df.createOrReplaceTempView("provider_table")
        visit_df.createOrReplaceTempView("visit_table")
        logger.info("Step 4: Dataframes successfully registered as temporary views / tables.")

        // Step 5: Extract the sql transformation scripts from transformation.conf file
        val question_1_query: String = transformationsConfig.getString("transformations.question_1_cte")
        val question_2_query: String = transformationsConfig.getString("transformations.question_2_cte")
        logger.info("Step 5: Transformation scripts successfully retrieved as strings.")

        // Step 6: create dataframes from the query strings
        val answer_to_question_1_df = spark.sql(question_1_query)
        val answer_to_question_2_df = spark.sql(question_2_query) 
        logger.info("Step 6: Dataframes successfully created from query strings.")

        // Step 7: write the dataframes as json files
        write_df_to_json_file(answer_to_question_1_df, applicationConfig.getString("app.output_paths.answer_to_question_1_path"))
        write_df_to_json_file(answer_to_question_2_df, applicationConfig.getString("app.output_paths.answer_to_question_2_path"))
      }
    catch 
      {
        case e: Exception =>
          logger.error("An error occurred during the execution of the application.", e)
          throw e // Re-throwing after logging for further handling if necessary
      }
    finally 
      {
          if (spark != null) {
            logger.info("Stopping SparkSession.")
            spark.stop()
          }
      }
  }

  //----------------------------------------------------------------------------------
  /**
   * Function to read csv files and load it to a Dataframe 
   *
   * @param spark  for SparkSession
   * @param conf   for csv configuration
   * @return DataFrame
   */
  def read_csv_file_to_df(spark: SparkSession, conf: Config): DataFrame = {
    try {
      val csvPath: String = conf.getString("csv_path")
      val csvDelimiter: String = conf.getString("csv_delimiter")
      val csvHeader: Boolean = if (conf.getString("csv_header")=="true") true else false
      
      val csvSchema = new StructType(
        conf.getConfigList("csv_schema").toArray.map { schemaItem =>
          val field = schemaItem.asInstanceOf[com.typesafe.config.Config]
          StructField(field.getString("name"), field.getString("type") match {
            case "LongType"   => LongType
            case "StringType" => StringType
            case "DateType"   => DateType
          }, field.getBoolean("nullable"))
        }
      )

      logger.info(s"Reading CSV file: $csvPath")

      val df = spark.read
        .option("header", csvHeader)
        .option("delimiter", csvDelimiter)
        .option("mode", "FAILFAST")
        .schema(csvSchema)
        .csv(csvPath)
      logger.info(s"Step 3: ${csvPath} successfully parsed to dataframe")
      
      df

    } catch {
      case e: Exception =>
        logger.error("Step 3: Error reading CSV file.", e)
        throw e
    }
  }

  //----------------------------------------------------------------------------------
  /**
   * Function to write dataframe to json
   *
   * @param input_df    for provider_table
   * @param output_path  for transformation config
   * @return DataFrame
   */
  def write_df_to_json_file(input_df: DataFrame, json_path: String): Unit = {
    try {
      input_df.coalesce(1) // Ensure single file output
        .write
        .mode("overwrite")
        .option("pretty", "true")
        .json(json_path)
      logger.info(s"Step 7: Dataframe successfully written as json file at ${json_path}")

    } catch {
      case e: Exception =>
        logger.error("Step 7: Error writing df to json.", e)
        throw e
    }
  }

}