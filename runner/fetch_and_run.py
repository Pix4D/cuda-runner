import boto3
import logging
import os
import tempfile
import sys

out_filename = "archive"


class ConfigurationError(Exception):
    pass


class FetchAndRun:
    def __init__(self, logger):
        region = os.getenv("AWS_DEFAULT_REGION")
        if not region:
            raise ConfigurationError("The environment variable 'AWS_DEFAULT_REGION' must be set.")
        boto3.setup_default_session(region_name=region)

        self.s3_bucket = os.getenv("S3_BUCKET")
        if not self.s3_bucket:
            raise ConfigurationError("The environment variable 'S3_BUCKET' must be set.")

        self.s3_key = os.getenv("S3_KEY")
        if not self.s3_key:
            raise ConfigurationError("The environment variable 'S3_KEY' must be set.")

        self.cmd_exec = os.getenv("CMD_EXEC")
        if not self.s3_key:
            raise ConfigurationError("The environment variable 'CMD_EXEC' must be set.")

        self.work_dir = tempfile.TemporaryDirectory()
        os.chdir(self.work_dir)

    def __del__(self):
        self.work_dir.cleanup()

    def fetch(self):
        s3 = boto3.client("s3")
        with open(out_filename, "wb") as data:
            s3.download_fileobj(self.s3_bucket, self.s3_key, data)

    def unpack(self):
        print("foo")

    def execute(self):
        print("bar")

    def fetch_and_run(self):
        self.fetch()
        self.unpack()
        self.execute()


def main():
    logger = logging.getLogger("fetch_and_run")
    try:
        fetch = FetchAndRun(logger)
        fetch.fetch_and_run()
    except Exception as ex:
        logger.critical("Fetch and run failed: " + str(ex))
        sys.exit(1)


if __name__ == "__main__":
    main()
