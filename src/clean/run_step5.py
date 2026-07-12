from src.clean.load_and_drop import get_spark_session, load_raw_data, drop_member_id

from src.clean.clean_term import clean_term
from src.clean.clean_dates import clean_dates
from src.clean.clean_sparse import clean_sparse
from src.clean.profile_loan_status import profile_loan_status
from src.clean.label_target import label_target

GCS_PATH = "gs://credit-risk-batch-pipeline/raw/accepted_2007_to_2018Q4.csv.gz"


def main() -> None:
    spark = get_spark_session("credit_risk_step5")
    df = load_raw_data(spark, GCS_PATH)
    df = drop_member_id(df)
    df = clean_term(df)
    df = clean_dates(df)
    df = clean_sparse(df)

    status_distribution = profile_loan_status(df)
    status_distribution.show(30, truncate=False)

    pre_label_count = df.count()
    df = label_target(df)
    post_label_count = df.count()

    print(f"Rows before label_target: {pre_label_count}")
    print(f"Rows after label_target:  {post_label_count}")
    print(f"Rows excluded:            {pre_label_count - post_label_count}")

    df.groupBy("target").count().orderBy("target").show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()