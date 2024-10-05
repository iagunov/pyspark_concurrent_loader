"""
PySpark concurrent loader(Thread Pool design pattern).

This application is designed to process data from multiple sources and load it into a target table
in a distributed environment using Apache Spark. The app reads the latest work logs, processes data in parallel 
using multithreading, and logs the results to a journal (Hive table).

Key components:
- SparkJob: Manages the lifecycle of a Spark session, allowing context management for ease of use.
- JournalWriter: Writes job execution states (e.g., DONE, ERROR) into a Hive table for tracking processing status.
- Processor: Handles data transformation and loading for each source view, leveraging the Spark session.
- App: The main driver class that orchestrates the entire workflow, including logging, parallel processing, 
  and error handling.

Main features:
- Parallel processing of multiple source views using a ThreadPoolExecutor.
- Dynamic partition overwrite for efficient data writing.
- Comprehensive logging for both Spark operations and data processing steps.
- Fault tolerance with error logging and status recording to the journal.

Execution flow:
1. The app retrieves the latest work logs.
2. Processes each data source in parallel.
3. Logs the outcome of each source processing to the journal.

"""
import sys
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql import SparkSession


class SparkJob:
    """Spark session class with context manager support."""

    def __init__(self, app_name, config, log):
        """
        Initialize SparkJob instance.

        :param app_name: Name of the Spark application.
        :param config: Dictionary with Spark configuration parameters.
        :param log: Logger instance for logging SparkJob actions.
        """
        self.app_name = app_name
        self.config = config
        self.logger = log
        self.spark = self.create_spark_session()

    def create_spark_session(self):
        """
        Creates and configures a Spark session.

        :return: SparkSession instance.
        """
        self.logger.info("Creating Spark session...")
        spark_builder = SparkSession.builder \
            .appName(self.app_name) \
            .master(self.config.get("master", "yarn"))

        for key, value in self.config.items():
            spark_builder = spark_builder.config(key, value)

        return spark_builder.getOrCreate()

    def stop(self):
        """Stops the Spark session."""
        self.logger.info("Terminating the Spark session...")
        self.spark.stop()

    def __enter__(self):
        """Enter method for context management support."""
        self.logger.info("Entering SparkJob context...")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit method for context management support."""
        self.logger.info("Exiting SparkJob context...")
        self.stop()


class JournalWriter:
    """Writes job states to a journal (Hive table)."""

    def __init__(self, spark, hive_db_stg, uploader_state_table, logger):
        self.spark = spark
        self.hive_db_stg = hive_db_stg
        self.uploader_state_table = uploader_state_table
        self.logger = logger

    def write_journal(
            self, source_code, source_db, status_code,
            business_dt, rec_count, status_message="UNKNOWN_ERROR"
            ):
        try:
            self.spark.sql(f"""
                INSERT INTO {self.hive_db_stg}.{self.uploader_state_table}
                VALUES 
                ("{source_db}", "{status_code}", "{business_dt}",
                "{rec_count}", "{status_message}", "{source_code}")
            """)
            self.logger.info(f"Log entry for {source_code}: {status_code}.")
        except Exception as e:
            self.logger.error(
                "Error writing to journal for %s: %s", source_code, {e}, exc_info=True)


class Processor:
    """Processes data from various source views and writes results to the target table."""

    def __init__(self, spark, journal_writer, logger, hive_db_pa, hive_db_stg, target_table, source_x_feature):
        self.spark = spark
        self.journal_writer = journal_writer
        self.logger = logger
        self.hive_db_pa = hive_db_pa
        self.target_table = target_table
        self.hive_db_stg = hive_db_stg
        self.source_x_feature = source_x_feature


    def process_view(self, row):
        source_code = row['source_code']
        source_db = row['source_db']
        source_loading_id = row['source_loading_id']
        business_dt = row['business_dt']

        self.logger.info("Starting process for %s.", source_code, exc_info=True)
        try:
            # Here is the query to get source data. You can write any query here.
            # For example:
            df_source = self.spark.sql(f"""
                SELECT object_id, feature_txt, feature_val, feature_dt, 
                       current_timestamp() AS datechange
                FROM {source_db}.{source_code}
            """)

            # To overwrite the partition when reloading, use the option
            # .option("partitionOverwriteMode", "dynamic")
            df_source.write.mode("overwrite").option("partitionOverwriteMode", "dynamic") \
                .insertInto(f'{self.hive_db_pa}.{self.target_table}')

            # This count allows you to check how many lines were actually written.
            # Due to partitioning, full scan does not occur,
            # a fast file scan of the metadata of parquet files is used here.
            feature_count_row = self.spark.sql(f"""
                SELECT count(*) AS feature_count FROM {self.hive_db_pa}.{self.target_table}
                WHERE feature_code IN (
                    SELECT object_id FROM {self.hive_db_stg}.{self.source_x_feature}
                    WHERE source_code="{source_code}")
                AND gregor_dt = "{business_dt}"
            """).collect()[0]

            feature_count = feature_count_row['feature_count']

            self.journal_writer.write_journal(
                source_code, source_db, "DONE", business_dt, feature_count, "SUCCEEDED"
                )
            self.logger.info(f"Shutting down the process for {source_code}.", exc_info=True)

        except Exception as e:
            self.logger.critical("Error in the process %s: %s", source_code, str(e), exc_info=True)
            self.journal_writer.write_journal(
                source_code, source_db, "ERROR", source_loading_id, business_dt, 0, str(e)
                )


class App:
    """Main class for running the data processing application."""
    def __init__(self, params):
        """
        Initialize App instance.

        :param params: Dictionary with configuration parameters for the app.
        """
        self.params = params
        self.logger = self.setup_logger()

    def setup_logger(self):
        """
        Set up logging for the application.

        :return: Configured logger instance.
        """
        logger = logging.getLogger(__name__)
        formatter = logging.Formatter('[%(asctime)s] %(levelname)s @ line %(lineno)d: %(message)s')
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel("INFO")
        handler.setFormatter(formatter)
        logger.setLevel("INFO")
        logger.addHandler(handler)
        return logger

    def run(self):
        """
        Main method to execute the data processing workflow.

        Fetches data from the latest logs, processes it in parallel, and logs the outcomes.
        """
        source_x_feature = self.params['SOURCE_X_FEATURE']
        uploader_state_table = self.params['UPLOADER_STATE_TABLE']
        hive_db_pa = self.params['DB_PA']
        hive_db_stg = self.params['DB_STG']
        target_table = self.params['TARGET_TABLE']

        try:
            with SparkJob("juggler", self.params['SPARK_CONFIG'], self.logger) as spark:
                journal_writer = JournalWriter(
                    spark, hive_db_stg, uploader_state_table, self.logger)

                processor = Processor(
                    spark, journal_writer, self.logger, hive_db_pa,
                    hive_db_stg, target_table, source_x_feature
                    )

                #Here is the query to get latest work log for each source code.
                #You can write any query here.
                query = f"""
                    SELECT source_code, source_db, business_dt
                    FROM (
                        SELECT source_code, source_db, status_code, business_dt,
                            ROW_NUMBER() OVER (PARTITION BY source_code ORDER BY datechange DESC) AS rn
                        FROM {hive_db_stg}.{uploader_state_table}
                    ) WHERE rn = 1 AND status_code = "READY"
                """
                # Executing query to get latest work log for each source code.
                df_latest_work_log = spark.spark.sql(query)
                # Getting list of latest work logs to the driver.
                work_log_list = list(set(df_latest_work_log.collect()))

                # Setting max number of workers to 5.
                # This is protection against excessive use of the infrastructure,
                # one process per view, but no more than 5 in total.
                # It can be increased if needed.
                max_workers_num = 5
                num_workers = min(len(work_log_list), max_workers_num)

                # Creating a pool of workers. Each worker will process one view.
                with ThreadPoolExecutor(max_workers=num_workers) as executor:
                    future_to_row = {
                        executor.submit(
                            processor.process_view, row): row for row in work_log_list
                    }

                    # Waiting for all futures to complete. This is done in a separate thread.
                    # And all errors will be logged separately.
                    # It will not block the other ones.
                    for future in as_completed(future_to_row):
                        row = future_to_row[future]
                        try:
                            future.result()
                        except Exception as e:
                            # Log errors. It will be written to the journal
                            self.logger.error("ERROR durring the processing view %s : %s",
                                              row['source_code'], str(e), exc_info=True)
                            journal_writer.write_journal(row['source_code'], row['source_db'],
                                                         "ERROR", row['businesss_dt'], 0, str(e))
        except Exception as e:
            self.logger.error("Critical error in the MAIN process: %s ", str(e), exc_info=True)


if __name__ == "__main__":
    app = App(params={
        "SOURCE_X_FEATURE": "source_x_feature",
        "UPLOADER_STATE_TABLE": "uploader_state",
        "DB_PA": "db_pa",
        "DB_STG": "db_stg",
        "TARGET_TABLE": "target_table",
        "SPARK_CONFIG": {
            "spark.executor.cores": "4",
            "spark.driver.memory": "128g",
            "spark.executor.memory": "8g",
            "spark.sql.sources.partitionOverwriteMode": "dynamic",
            "spark.dynamicAllocation.enabled": "true",
            "spark.dynamicAllocation.minExecutors": "2",
            "spark.dynamicAllocation.maxExecutors": "10",
            "spark.executor.memoryOverhead": "2g",
            "spark.driver.memoryOverhead": "2g",
            "spark.driver.maxResultSize": "0",
            "spark.submit.deployMode": "client",
            "spark.sql.shuffle.partitions": "15000",
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            "spark.sql.execution.arrow.maxRecordsPerBatch": "50000",
            "spark.dynamicAllocation.shuffleTracking.enabled": "true",
            "spark.dynamicAllocation.executorIdleTimeout": "30s",
            "spark.dynamicAllocation.cachedExecutorIdleTimeout": "5s",
            "spark.sql.parquet.binaryAsString": "false",
            "spark.sql.hive.convertMetastoreParquet": "true",
            "spark.shuffle.service.enabled": "true",
            "spark.network.timeout": "3000",
            "spark.sql.optimizer.metadataOnly": "true",
            "spark.kryoserializer.buffer.max": "2047mb",
            "spark.sql.parquet.int96RebaseModeInRead": "CORRECTED",
            "spark.sql.sources.partitionColumnTypeInference.enabled": "true",
            "spark.hadoop.validateOutputSpecs": "false",
            "spark.sql.catalogImplementation": "hive",
            "spark.port.maxRetries": "150",
            "spark.sql.codegen.wholeStage": "true",
            "spark.hadoop.hive.exec.dynamic.partition.mode": "nonstrict"
        }
    })
    app.run()
