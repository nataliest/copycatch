import redis

from collections import OrderedDict

r_tags = redis.StrictRedis(host='redis-db.7ptpwl.ng.0001.use1.cache.amazonaws.com', port=6379, db=1)
r_tag_levels = redis.StrictRedis(host='redis-db.7ptpwl.ng.0001.use1.cache.amazonaws.com', port=6379, db=2)

def get_img_id_list(incoming_img_tags, r_tags, r_tag_levels):
    tags = sort_by_level(incoming_img_tags, r_tag_levels)
   # tags = incoming_img_tags
    img_list_prev = set(r_tags.smembers(tags[0]))
    img_list_next = []

    for tag in tags[1:]:
        img_list_next = set(r_tags.smembers(tag))
        img_list_prev = img_list_prev.intersection(img_list_next)
        img_list_next = []

    print("Filter returned {} images".format(len(img_list_prev)))
   # print(img_list_prev)
    return img_list_prev

def sort_by_level(tags, r_tag_levels):
    od = OrderedDict()
    for tag in tags:
        level = str(r_tag_levels.get(tag))
        if level in od:
            od[level].append(tag)
        else:
            od[level] = [tag]
    print(tags)
    od = OrderedDict(sorted(od.items(), key=lambda t: t[0]))
    res_list = []
    for key in od:
        res_list = od[key] + res_list
    print(res_list)
    return res_list


#incoming_img_tags = ['/m/014sv8', '/m/01g317', '/m/01gmv2', '/m/02p0tk3', '/m/035r7c', '/m/039xj_', '/m/03q69', '/m/0463sg', '/m/04hgtk', '/m/05r655', '/m/0dzct', '/m/0k0pj', '/m/01gkx_', '/m/09j2d']
incoming_img_tags = ['/m/01g317', '/m/04hgtk', '/m/04yx4', '/m/07j7r', '/m/09j2d', '/m/09j5n']
incoming_img_tags = ['/m/01lrl', '/m/0bt9lr', '/m/0jbk']
get_img_id_list(incoming_img_tags, r_tags, r_tag_levels)
