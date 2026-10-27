#!/usr/bin/env bash
#SBATCH --job-name=fpflux1704
#SBATCH --account=chem007981
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --time=04:00:00
#SBATCH --output=/group/chem/acrg/object_stores/temp/OCO2_test/fpflux1704-%j.out

set -euo pipefail

repo=/group/chem/acrg/brendan_for_codex/openghg
results=/group/chem/acrg/object_stores/temp/OCO2_test/benchmarks
mkdir -p "$results"
cd "$repo"

export MPLCONFIGDIR="${TMPDIR:-/tmp}/matplotlib-${SLURM_JOB_ID}"
mkdir -p "$MPLCONFIGDIR"

.venv/bin/python benchmarks/benchmark_fp_x_flux_irregular.py \
  --output-json "$results/issue-1704-${SLURM_JOB_ID}.json" \
  --repaired-footprint /group/chem/acrg/object_stores/temp/OCO2_test/benchmarks/issue-1704-january-time32.zarr \
  --benchmark-mode "${BENCHMARK_MODE:-both}" \
  --stages 1 3 7 14 31 \
  --time-chunk 32 \
  --workers 4 \
  --max-stage-seconds 1200 \
  --max-memory-mb 14000
