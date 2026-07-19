from  functools import reduce
from pyspark.sql import  DataFrame

# internal domain imports 
from src.clean.load_and_drop import get_spark_session, load_raw_data, drop_member_id
from src.clean.clean_term import clean_term
from src.clean.clean_dates import clean_dates
from src.clean.clean_sparse import clean_sparse
from src.clean.drop_shifted_rows import drop_shifted_rows
from src.clean.validate_categoricals import validate_categoricals
from src.clean.label_target import label_target
from src.clean.add_vintage_year import add_vintage_year
from src.clean.add_loan_age import add_loan_age
from src.clean.add_dti_bucket import add_dti_bucket
from src.clean.add_delinquency_flag import add_delinquency_flag
from src.aggregations.compute_npl_ratio import compute_npl_ratio
from src.aggregations.compute_default_rate_curve import compute_default_rate_curve

RAW_GCS_PATH="gs://credit-risk-batch-pipeline/raw/accepted_2007_to_2018Q4.csv.gz"
BQ_PROJECT="gcp-de-learning-498109" # replace with your project id
BQ_DATASET="credit_risk_batch_pipeline"# dataset name
NPL_RATIO_TABLE=f"{BQ_PROJECT}.{BQ_DATASET}.npl_ratio_by_dimension"# used to store the nlp ratios
DEFAULT_RATE_CURVE_TABLE=f"{BQ_PROJECT}.{BQ_DATASET}.default_rate_vintage_curve"# used to store bad loan default rate curves
NPL_DIMENSIONS=("grade","addr_state","purpose","vintage_year")
BQ_TEMP_GCS_BUCKET="credit-risk-batch-pipeline" # bucket name

def write_to_bigquery(df:DataFrame,table:str) -> None:
    (
        df.write.format("bigquery") # use bigquery as the destination
        .option("table", table) # write to the specified table
        .option("temporaryGcsBucket",BQ_TEMP_GCS_BUCKET) # write to a temporary bucket to avoid exceeding BQ limits
        .mode("overwrite") # overwrite the table if it already exists
        .save()
    )



def main() -> None:
    spark=get_spark_session(app_name="credit_risk_batch_pipeline")
    df=load_raw_data(spark,RAW_GCS_PATH)

    #transform
    df=drop_member_id(df)
    df=clean_term(df)
    df=clean_dates(df)
    df=clean_sparse(df)
    df=drop_shifted_rows(df)
    validate_categoricals(df)


    #add new columns
    df=label_target(df)
    df=add_vintage_year(df)
    df=add_loan_age(df)
    df=add_dti_bucket(df)
    df=add_delinquency_flag(df)

    # --- Checkpoint: persist labeled/feature-engineered dataset before aggregation ---
    # Written so the data quality report can read a stable snapshot independently,
    # without rerunning the full load->clean->feature-engineer chain.
    # Overwrite mode is intentional: only one checkpoint is kept at a time; history
    # lives in the baseline snapshot JSON, not in the checkpoint itself.
    CHECKPOINT_PATH = "gs://credit-risk-batch-pipeline/checkpoints/labeled_dataset/"
    df.write.mode("overwrite").parquet(CHECKPOINT_PATH)
    
    #load and compute aggregations
    npl_ratio_df= reduce( # reduce is a python builtin function that takes a function and an iterable and applies the function to each element of the iterable
           DataFrame.unionByName, # unionByName is a pyspark function that takes two dataframes and returns a dataframe with the union of the two dataframes
           (compute_npl_ratio(df,dimension) for dimension in NPL_DIMENSIONS),# compute_npl_ratio is a function that takes a dataframe and a dimension and returns a dataframe with the npl ratio for that dimension
    )
    write_to_bigquery(npl_ratio_df,NPL_RATIO_TABLE)# write the npl ratios to bigquery
    default_rate_curve_df=compute_default_rate_curve(df) #
    write_to_bigquery(default_rate_curve_df,DEFAULT_RATE_CURVE_TABLE)# write the default rate curves to bigquery
    spark.stop()

if __name__=="__main__":
    main()

    