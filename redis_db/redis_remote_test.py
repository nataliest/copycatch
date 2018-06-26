import redis
r = redis.StrictRedis(host='redis-db.7ptpwl.ng.0001.use1.cache.amazonaws.com', port=6379, db=5)

print("Connected")
r.sadd('redis', 'test')
print("Added stuff")
