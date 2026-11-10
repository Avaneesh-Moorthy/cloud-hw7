#!/bin/bash
# run_local.sh - run the pipeline on THIS computer (laptop) with Beam's local runner.
#   bash run_local.sh
# 1. downloads the 12,000 pages from the bucket into ./pages (once; ~0.9 GB)
# 2. creates a Python virtualenv with apache-beam
# 3. runs the pipeline with --runner DirectRunner and times it
# 4. prints the results (local_output/results.txt)
set -euo pipefail
BUCKET="cedar-pottery-508921-u1-pages"
HERE="$(cd "$(dirname "$0")" && pwd)"; cd "$HERE"

if [ "$(ls pages 2>/dev/null | wc -l)" -lt 12000 ]; then
  echo "==> downloading gs://$BUCKET/pages/ (one time)"
  gcloud storage cp -r "gs://$BUCKET/pages" .
fi
echo "==> $(ls pages | wc -l) pages in ./pages"

if [ ! -x .venv/bin/python ]; then
  echo "==> creating virtualenv with apache-beam (one time, ~2 min)"
  python3 -m venv .venv
  .venv/bin/pip install -q --upgrade pip
  .venv/bin/pip install -q -r requirements.txt
fi
echo "==> Python $(.venv/bin/python -V 2>&1 | cut -d' ' -f2), Beam $(.venv/bin/python -c 'import apache_beam as b; print(b.__version__)')"

echo "==> running on the local runner (DirectRunner)"
rm -rf local_output
START=$(date +%s)
.venv/bin/python beam_links_trigrams.py --runner DirectRunner \
    --input "pages/*.html" --output local_output/
END=$(date +%s)
echo "==> LOCAL RUN TIME: $((END-START)) seconds (whole command, including Python start-up)"
echo
cat local_output/results.txt
echo
cat local_output/results.json | grep totals
