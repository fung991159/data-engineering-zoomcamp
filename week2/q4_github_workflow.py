from prefect.filesystems import GitHub
from prefect import flow, task


@flow(log_prints=True)
def etl_web_to_gcs() -> None:
    github_block = GitHub.load("get-web-to-gcs")
    github_block.get_directory("week2")


if __name__ == "__main__":
    etl_web_to_gcs()
