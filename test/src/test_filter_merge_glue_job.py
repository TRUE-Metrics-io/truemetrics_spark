import sys
import os

sys.path.append(os.path.abspath("src"))

from src.filter_merge_glue_job import FilterMergeGlueJob


def test_filter_merge_glue_job_process_data():
    # Create test job and load test data.
    job = FilterMergeGlueJob()
    data = job.load_data(
        s3_paths=[
            "s3://truemetrics-spark-test-data/filter-merge-glue-job/true-v2-input-files-correct-schema-truncated-for-easy-visual-validation/"
        ]
    )

    # There should only be 2 files in the test data directory.
    assert data.count() == 2

    # Process the data.
    processed_data = job.process_data(data)
    assert processed_data.count() == 2
