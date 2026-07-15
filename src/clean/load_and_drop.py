import os

GCS_CONNECTOR_JAR = os.path.expanduser("~/spark-jars/gcs-connector-hadoop3-2.2.21-shaded.jar")
ADC_CREDENTIALS_PATH = os.path.expanduser("~/.config/gcloud/application_default_credentials.json")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ADC_CREDENTIALS_PATH
os.environ["PYSPARK_SUBMIT_ARGS"] = f"--jars {GCS_CONNECTOR_JAR} pyspark-shell"

from pyspark.sql import SparkSession, DataFrame


def get_spark_session(app_name: str = "credit_risk_cleaning") -> SparkSession:
    """Create or retrieve a local SparkSession configured for GCS access via ADC."""
    return (
        SparkSession.builder
        .appName(app_name) #specifies the ui header
        .master("local[*]") #tell spark to run locally
        .config("spark.jars", GCS_CONNECTOR_JAR) #specify the gcs connector jar
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .config("spark.hadoop.fs.gs.auth.type", "APPLICATION_DEFAULT")
        .getOrCreate()
    )


def load_raw_data(spark: SparkSession, gcs_path: str) -> DataFrame:
    """Load raw CSV data from a GCS path into a DataFrame."""
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(gcs_path)
    )


def drop_member_id(df: DataFrame) -> DataFrame:
    """Drop the member_id column, which is entirely null in the raw dataset."""
    if "member_id" in df.columns:
        return df.drop("member_id")
    return df