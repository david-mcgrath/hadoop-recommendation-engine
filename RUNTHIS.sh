curl \
    -X PUT \
    -k \
    -u s226-3bfa7401ec1542-d9ec086b762a:d1474eac-8e00-454b-a444-c9c64190ef6c \
    -H "X-Spark-service-instance-id: a36b185b-2ada-4e98-a226-3bfa7401ec15" \
    --data-binary "@~/engine.py" \
    https://spark.bluemix.net/tenant/data/engine.py

~/spark-submit.sh \
  --vcap ./vcap.json \
  --deploy-mode cluster \
  --conf spark.service.spark_version=1.6 \
  ~/run.py