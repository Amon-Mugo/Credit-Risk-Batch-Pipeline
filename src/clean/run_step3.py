from src.clean.load_and_drop import get_spark_session,load_raw_data,drop_member_id

from src.clean.clean_term import clean_term
from src.clean.clean_dates import clean_dates

GCS_PATH="gs://credit-risk-batch-pipeline/raw/accepted_2007_to_2018Q4.csv.gz"

def main() -> None:
    spark=get_spark_session("credit_risk_step3")
    df=load_raw_data(spark,GCS_PATH)
    df=drop_member_id(df)
    df=clean_term(df)
    df=clean_dates(df)
    df.select("id","issue_d","earliest_cr_line","last_pymnt_d","next_pymnt_d","last_credit_pull_d").show(10,truncate=False)
    df.printSchema()
    spark.stop()


if __name__ == "__main__":
    main()