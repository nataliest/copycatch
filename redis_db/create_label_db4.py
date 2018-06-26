import pickle
import redis

tags_dict = pickle.load(open("image_labels.p", "rb" ))

r = redis.StrictRedis(host='redis-db.7ptpwl.ng.0001.use1.cache.amazonaws.com', port=6379, db=4)

key_count = 0
for key, val in tags_dict.items():
    key_count += 1
    if key_count % 100 == 0:
        print("Processed {}".format(key_count))
    r.set(key, val)
