from src.clean.load_and_drop import get_spark_session, load_raw_data, drop_member_id

GCS_PATH = "gs://credit-risk-batch-pipeline/raw/accepted_2007_to_2018Q4.csv.gz"


def main() -> None:
    spark=get_spark_session() #creates or returns a spark session
    raw_df=load_raw_data(spark,GCS_PATH) #loads raw data from gcs
    clean_df=drop_member_id(raw_df) #drops member_id column

    print(f"RawCount:{clean_df.count()}")
    print(f"Column.Count:{len(clean_df.columns)}")

if __name__ == "__main__":#if this file is run as a script
    main()