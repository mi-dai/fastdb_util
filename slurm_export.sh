#!/bin/bash
#SBATCH --job-name=fastdb_export
#SBATCH --account=YOUR_ACCOUNT
#SBATCH --qos=regular
#SBATCH --constraint=cpu
#SBATCH --licenses=cfs
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --time=02:00:00
#SBATCH --array=0-9          # 10 nodes (adjust --num-nodes below to match)
#SBATCH --output=logs/export_%A_%a.out

NUM_NODES=10
OUT_DIR=/pscratch/sd/X/YOUR_USERNAME/fastdb_export/

source /global/cfs/cdirs/lsst/groups/TD/setup_td.sh
export PYTHONPATH=/dvs_ro/cfs/cdirs/desc-td/SOFTWARE/fastdb_deployment/fastdb_client:$PYTHONPATH

python example_run.py "$OUT_DIR" \
    --bypass-object-search \
    --chunk-size 1000 \
    --num-nodes "$NUM_NODES" \
    --node-index "$SLURM_ARRAY_TASK_ID" \
    --log-file "logs/export_${SLURM_ARRAY_JOB_ID}_${SLURM_ARRAY_TASK_ID}.log"
