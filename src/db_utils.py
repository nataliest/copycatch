import redis
from collections import OrderedDict

def update_db(incoming_img_tags, img_id, r_tags):
    r_tags.incr('size')
    for tag in incoming_img_tags:
        r_tags.sadd(tag, img_id)
    
def get_img_id_list(incoming_img_tags, 
                    r_tags, 
                    r_tag_levels):
    tags = list(sort_by_level(incoming_img_tags, r_tag_levels))
    
    img_list_prev = set(r_tags.smembers(tags[0]))
    img_list_next = []
    
    for tag in tags[1:]:
        img_list_next = set(r_tags.smembers(tag))
        img_list_prev = img_list_prev.intersection(img_list_next)
        img_list_next = []
 
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

# def update_stats_db(db=r_stats,
#                     labels=[], 
#                     tot_size=0, 
#                     num_filtered=0,
#                     redis_time=0,
#                     spark_time=0,
#                     tot_time=0,
#                     structural_similarity=0,
#                     url_incoming='',
#                     url_original=''):
#     stats_list = []

#     tag_labels = str(labels_list)
#     stats_list.append(labels)

#     total_img = r_tags.get('size')
#     stats_list.append(tot_size)

#     num_filtered = len(img_list)
#     stats_list.append(num_filtered)

#     stats_list.append(redis_time)
#     stats_list.append(spark_time)

#     tot_time = time.time() - tot_start_time
#     stats_list.append(tot_time)

#     ssim = compare_ssim(incoming_im_resized[0], result[0][0], multichannel=mult)
#     stats_list.append(structural_similarity)

#     k_src.key = "{}{}.jpg".format(incoming_im_resized[3],incoming_img_id)
#     k_dst.key = "{}{}.jpg".format(result[0][3], result[0][2])
#     url_orig = k_dst.generate_url(expires_in=0, query_auth=False)
#     url_incoming = k_src.generate_url(expires_in=0, query_auth=False)
#     stats_list.append(url_orig)
#     stats_list.append(url_incoming)
#     print(stats_list)
#     for stat in stats_list:
#         r_stats.lpush(result[0][2], stat)

