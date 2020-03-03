set -m

/entrypoint.sh couchbase-server &

sleep 15

# Setup index and memory quota
curl -v -X POST http://127.0.0.1:8091/pools/default -d memoryQuota=256 -d indexMemoryQuota=256

# Setup services
curl -v http://127.0.0.1:8091/node/controller/setupServices -d services=kv%2Cn1ql%2Cindex

# Setup credentials
curl -v http://127.0.0.1:8091/settings/web -d port=8091 -d username=Administrator -d password=123456

curl -i -u Administrator:123456 -X POST http://127.0.0.1:8091/settings/indexes -d 'storageMode=memory_optimized'

curl -v -X POST http://127.0.0.1:8091/pools/default/buckets
curl -X POST -u Administrator:123456 \
	-d name=test -d ramQuotaMB=200 -d authType=none \
	http://127.0.0.1:8091/pools/default/buckets

fg 1