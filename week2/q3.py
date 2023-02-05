from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(log_prints=True, retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")


@task()
def get_df(path: Path) -> pd.DataFrame:
    """turn CSV to DF"""
    df = pd.read_parquet(path)
    print(f"rows: {len(df)}")
    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table="tests.test-fung-zoom",
        project_id="cn-ops-spdigital-dev-dev",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow(log_prints=True)
def etl_gcs_to_bq(months: list[int], year: int, color: str):
    """Main ETL flow to load data into Big Query"""

    for month in months:
        path = extract_from_gcs(color, year, month)
        df = get_df(path)
        write_bq(df)


if __name__ == "__main__":
    color = "yellow"
    year = 2019
    months = [2, 3]

    etl_gcs_to_bq(months=months, year=year, color=color)
