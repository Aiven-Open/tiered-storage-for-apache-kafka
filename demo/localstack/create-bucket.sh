#!/bin/sh
# LocalStack init hook: creates the bucket named in the BUCKET_NAME environment variable.
set -e
awslocal s3 mb "s3://${BUCKET_NAME}"
