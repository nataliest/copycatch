import redis
from collections import OrderedDict

def filter_tags(tags):
    #    for t in tags:
    #check if there are redindant tags, i.e. airplane+aircraft+vehicle
    #airplane is the only tag we're interested in
    #dict: {airplane: [aircraft, vehicle], aircraft: [vehicle], vehicle: []}
    #dict: {vehicle:[airplane,car,]}
    #filter out only lowest-level tags
    #dog carnivore airplane vehicle animal aircraft
    #add parents to the set
    #keep iterating and adding to the set of items to delete
    #perform image_id intersection only on narrowest tags
    return filtered

def get_img_id_list(incoming_img_tags, r_tags, r_tag_levels):
    tags = list(sort_by_level(incoming_img_tags))
    
    img_list_prev = set(r_tags.smembers(tags[0]))
    img_list_next = []
    
    for tag in tags[1:]:
        img_list_next = set(r_tags.smembers(tag))
        img_list_prev = img_list_prev.intersection(img_list_next)
        img_list_next = []
    
    print("Filter returned {} images".format(len(img_list_prev)))
    #    print(img_list_prev)
    return img_list_prev

def sort_by_level(tags):
    # TODO: implement optimization, i.e. sorting from highest level to lowest
    return tags

# TODO check sorted collections
#def sort_by_level(tags, r_tag_levels):
#    od = OrderedDict()
#    for tag in tags:
#        level = str(r_tag_levels.get(tag))
#        if level in od:
#            od[level].append(tag)
#        else:
#            od[level] = [tag]
#    print(tags)
#    print(od)
