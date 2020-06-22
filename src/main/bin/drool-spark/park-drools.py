import os

cmd = '''
python3 ../common/run_spark.py \
--spark-class \
com.wb.drools.spark.Test02 \
'''
# --deploy-mode cluster \
# >/dev/null 2>&1 &
os.system (cmd)
