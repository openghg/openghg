#!/usr/bin/env bash
#SBATCH --job-name=fpfluxlegacy1704
#SBATCH --account=chem007981
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --time=04:00:00
#SBATCH --output=/group/chem/acrg/object_stores/temp/OCO2_test/fpfluxlegacy1704-%j.out

set -euo pipefail

repo=/group/chem/acrg/brendan_for_codex/openghg
results=/group/chem/acrg/object_stores/temp/OCO2_test/benchmarks
mkdir -p "$results"
cd "$repo"

export MPLCONFIGDIR="${TMPDIR:-/tmp}/matplotlib-${SLURM_JOB_ID}"
mkdir -p "$MPLCONFIGDIR"

.venv/bin/python -m benchmarks.benchmark_fp_x_flux_legacy \
  --output-json "$results/issue-1704-legacy-${SLURM_JOB_ID}.json" \
  --baseline-json "$results/issue-1704-18300036.json" \
  --stages 1 3 7 14 31 \
  --workers 4 \
  --max-stage-seconds 1200 \
  --max-memory-mb 14000
