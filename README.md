# CS528 HW7: Apache Beam / Cloud Dataflow on the HW2 pages

Code in [`hw7/`](hw7/). One pipeline, `beam_links_trigrams.py`, computes over `gs://cedar-pottery-508921-u1-pages/pages/*.html`:

1. top 5 files by **outgoing** links and top 5 by **incoming** links
2. top 5 most frequent word **trigrams** (3 consecutive words of the visible text)

It also reports how many items tie with 5th place, since the generated data has many ties.

```bash
# on a laptop (local runner; downloads the pages to ./pages first)
cd hw7 && bash run_local.sh

# on Google Cloud Dataflow (from Cloud Shell)
cd hw7 && bash run_dataflow.sh
```

| File | Purpose |
|---|---|
| `beam_links_trigrams.py` | the Beam pipeline (runs unchanged on DirectRunner and DataflowRunner) |
| `run_local.sh` | download pages, create venv, run locally, print results and run time |
| `run_dataflow.sh` | enable API, create worker service account, submit to Dataflow, print timing and results |
| `requirements.txt` | `apache-beam[gcp]` |

Dataflow output: `gs://cedar-pottery-508921-u1-pages/hw7_output/dataflow/results.txt` (and `results.json`).
