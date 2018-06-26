import pickle
import redis

tags_dict = pickle.load(open("tag_keys.p", "rb" ))

r = redis.StrictRedis(host='redis-db.7ptpwl.ng.0001.use1.cache.amazonaws.com', port=6379, db=1)

tot_keys = len(tags_dict)
key_count = 0
for key, vals in tags_dict.items():
    key_count += 1
    if key_count % 10 == 0:
        print("Processed {}/{}".format(key_count, tot_keys))
    for tag in vals:
        r.sadd(key, tag)
