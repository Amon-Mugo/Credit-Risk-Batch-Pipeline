"""Step 5a runner: verify drop_shifted_rows + validate_categoricals against
the real dataset.

Chains the full pipeline through Week 2.5 Step 4, applies drop_shifted_rows
to remove column-shift-corrupted rows, then runs validate_categoricals to
confirm purpose/addr_state are clean. Prints row counts before/after the
drop and a success message if validation passes without raising.
"""

from src.clean.load_and_drop import get_spark_session, load_raw_data, drop_member_id
from src.clean.clean_term import clean_term
from src.clean.clean_dates import clean_dates
from src.clean.clean_sparse import clean_sparse
from src.clean.label_target import label_target
from src.clean.add_vintage_year import add_vintage_year
from src.clean.add_loan_age import add_loan_age
from src.clean.add_dti_bucket import add_dti_bucket
from src.clean.add_delinquency_flag import add_delinquency_flag
from src.clean.drop_shifted_rows import drop_shifted_rows
from src.clean.validate_categoricals import validate_categoricals

GCS_PATH = "gs://credit-risk-batch-pipeline/raw/accepted_2007_to_2018Q4.csv.gz"


def main() -> None:
    spark = get_spark_session()
    df = load_raw_data(spark, GCS_PATH)
    df = drop_member_id(df)
    df = clean_term(df)
    df = clean_dates(df)
    df = clean_sparse(df)
    df = label_target(df)
    df = add_vintage_year(df)
    df = add_loan_age(df)
    df = add_dti_bucket(df)
    df = add_delinquency_flag(df)

    pre_drop_count = df.count()
    df = drop_shifted_rows(df)
    post_drop_count = df.count()

    print(f"\nRows before drop_shifted_rows: {pre_drop_count}")
    print(f"Rows after drop_shifted_rows: {post_drop_count}")
    print(f"Rows dropped: {pre_drop_count - post_drop_count}")

    validate_categoricals(df)
    print("\nvalidate_categoricals passed: purpose and addr_state are clean.")


if __name__ == "__main__":
    main()