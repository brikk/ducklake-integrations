docker compose run --rm \
  -e PG_DB=bench_lake \
  -e S3_ENDPOINT=s3.us-east-va.io.cloud.ovh.us \
  -e S3_USE_SSL=true \
  -e S3_URL_STYLE=path \
  -e S3_REGION=us-east-va \
  -e S3_KEY_ID=$1 \
  -e S3_SECRET=$2 \
  -e S3_BUCKET=rumble-recsys-outbox \
  -e S3_DATA_PREFIX=bench_ducklake/ \
  -e TPCH_SCALE_FACTOR=0 \
  bootstrap