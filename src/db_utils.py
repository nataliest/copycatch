import redis
from collections import OrderedDict

def update_db(incoming_img_tags, id, r_tags):
    for tag in incoming_img_tags:
        r_tags.sadd(tag, id)

def get_img_id_list(incoming_img_tags, r_tags, r_tag_levels):
    tags = list(sort_by_level(incoming_img_tags, r_tag_levels))
    
    img_list_prev = set(r_tags.smembers(tags[0]))
    img_list_next = []
    
    for tag in tags[1:]:
        img_list_next = set(r_tags.smembers(tag))
        img_list_prev = img_list_prev.intersection(img_list_next)
        img_list_next = []
    
    print("Filter returned {} images".format(len(img_list_prev)))
 
    return img_list_prev

def sort_by_level(tags, r_tag_levels):
    od = OrderedDict()
    for tag in tags:
        level = str(r_tag_levels.get(tag))
        if level in od:
            od[level].append(tag)
        else:
            od[level] = [tag]
    
    od = OrderedDict(sorted(od.items(), key=lambda t: t[0]))
    res_list = []
    for key in od:
        res_list = od[key] + res_list
   
    return res_list


