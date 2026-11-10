#!/bin/bash
# run_dataflow.sh - run the same pipeline on Google Cloud Dataflow (from Cloud Shell).
#   bash run_dataflow.sh
# 1. enables the Dataflow API, creates a least-privilege worker service account
# 2. creates a Python virtualenv with apache-beam[gcp]
# 3. submits the job (DataflowRunner, us-central1, up to 4 e2-standard-2 workers) and waits
# 4. prints the job's timing and the results written to the bucket
set -euo pipefail
PROJECT_ID="cedar-pottery-508921-u1"
REGION="us-central1"
BUCKET="cedar-pottery-508921-u1-pages"
SA="hw7-dataflow@${PROJECT_ID}.iam.gserviceaccount.com"
JOB="hw7-links-trigrams-$(date +%Y%m%d-%H%M%S)"
OUT="gs://$BUCKET/hw7_output/dataflow/"
HERE="$(cd "$(dirname "$0")" && pwd)"; cd "$HERE"
G="gcloud --project=$PROJECT_ID --quiet"

echo "==> enabling APIs"
$G services enable dataflow.googleapis.com compute.googleapis.com storage.googleapis.com

echo "==> worker service account (least privilege)"
$G iam service-accounts describe "$SA" >/dev/null 2>&1 || {
  $G iam service-accounts create hw7-dataflow --display-name="HW7 Dataflow workers"; sleep 10; }
$G projects add-iam-policy-binding "$PROJECT_ID" --condition=None \
  --member="serviceAccount:$SA" --role=roles/dataflow.worker >/dev/null
gcloud storage buckets add-iam-policy-binding "gs://$BUCKET" --project="$PROJECT_ID" \
  --member="serviceAccount:$SA" --role=roles/storage.objectAdmin --quiet >/dev/null

if [ ! -x .venv/bin/python ]; then
  echo "==> creating virtualenv with apache-beam[gcp] (one time, ~2-3 min)"
  python3 -m venv .venv
  .venv/bin/pip install -q --upgrade pip
  .venv/bin/pip install -q -r requirements.txt
fi
echo "==> Python $(.venv/bin/python -V 2>&1 | cut -d' ' -f2), Beam $(.venv/bin/python -c 'import apache_beam as b; print(b.__version__)')"

gcloud storage rm -r "$OUT" --quiet >/dev/null 2>&1 || true
echo "==> submitting Dataflow job $JOB (takes ~6-10 minutes incl. worker start-up)"
START=$(date +%s)
.venv/bin/python beam_links_trigrams.py \
    --runner DataflowRunner --project "$PROJECT_ID" --region "$REGION" \
    --job_name "$JOB" --service_account_email "$SA" \
    --temp_location "gs://$BUCKET/hw7_tmp/temp" --staging_location "gs://$BUCKET/hw7_tmp/staging" \
    --machine_type e2-standard-2 --num_workers 2 --max_num_workers 4 \
    --input "gs://$BUCKET/pages/*.html" --output "$OUT"
END=$(date +%s)
echo "==> DATAFLOW RUN TIME (submit -> done, seen from Cloud Shell): $((END-START)) seconds"

JOB_ID=$($G dataflow jobs list --region="$REGION" --filter="name=$JOB" --format='value(id)' | head -1)
echo "==> job $JOB_ID"
$G dataflow jobs describe "$JOB_ID" --region="$REGION" \
   --format='table(name,currentState,createTime,startTime,currentStateTime)'
echo "Console: https://console.cloud.google.com/dataflow/jobs/$REGION/$JOB_ID?project=$PROJECT_ID"
echo
gcloud storage cat "${OUT}results.txt"
echo
gcloud storage cat "${OUT}results.json" | grep totals
