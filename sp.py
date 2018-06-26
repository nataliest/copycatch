from __future__ import print_function
import argparse
import time
import sys
# custom modules
from image_compare import * 
from  db_utils import *
from  aws_s3_utils import * 

# S3 imports
import boto
from boto.s3.key import Key
from boto.exception import S3ResponseError

# Spark imports
from pyspark import SparkConf, SparkContext

# Redis imports
import redis


def parse_arguments():
    parser = argparse.ArgumentParser(description="spark-submit python file argument parser")
    parser.add_argument('-id', '--image-id', dest='image_id', type=str, help='Incoming image id; also s3 bucket key.')
    parser.add_argument('-mbn', '--main-bucket-name', dest='main_bucket_name', type=str, default='open-images-bucket', help='Main databaset bucket, approx. 600GB.')
    parser.add_argument('-tbn', '--temp-bucket-name', dest='temp_bucket_name', type=str, default='temp-oi-bucket', help='S3 bucket for temporary storage of uploaded images.')
    args = parser.parse_args()
    return vars(args)

if __name__ == "__main__":

    args = parse_arguments()

    incoming_bucket = args['temp_bucket_name']
    main_bucket = args['main_bucket_name']
    incoming_img_id = args['image_id']

    start_time = time.time()
    
    new_size = (128, 128)
    
    
    # TAGS => DB = 1
    # LEVELS => DB = 2
    # IMG_ID - tags[] for validation dataset => DB = 3
    r_incoming_tags = redis.StrictRedis(host='redis-db.7ptpwl.ng.0001.use1.cache.amazonaws.com', port=6379, db=3)
    r_tags = redis.StrictRedis(host='redis-db.7ptpwl.ng.0001.use1.cache.amazonaws.com', port=6379, db=1)
    r_tag_levels = redis.StrictRedis(host='redis-db.7ptpwl.ng.0001.use1.cache.amazonaws.com', port=6379, db=2)
    

    incoming_img_same = '000013ba71c12506'
    incoming_img_same = '0bd24be55bcff9eb'

    incoming_img_tags = ['/m/05s2s', '/m/07j7r', '/m/0d4v4']
    incoming_img_tags = r_incoming_tags.smembers(incoming_img_id)
    print("\n\n\nGOT TAGS FOR INCOMING IMAGE:", incoming_img_id)
    print(incoming_img_tags)
    print("\n\n\n")
    # incoming_img_tags = get_tags_for_incoming(image_id = incoming_img_id)
    img_list = get_img_id_list(incoming_img_tags, r_tags, r_tag_levels)
    print("\n\nFiltered image ids in {} seconds. Getting images from S3...\n\n\n".format(time.time()-start_time))
    start_time = time.time() 


   
    # connect to S3 bucket
    c = boto.connect_s3()
    b_incoming = c.get_bucket(incoming_bucket, validate=False)
    b = c.get_bucket(main_bucket, validate=False)


    incoming_im_resized = load_from_S3(image_id=incoming_img_id, image_size=new_size, b=b_incoming, aws_connection=c, bucket_name=incoming_bucket)
    if incoming_im_resized == None:
        print("\n\n\nIncoming image is NULL\n\n\n")
        sys.exit()

    timepoint = time.time() - start_time

    print("\n\nFetched images and tags in {} seconds\n\n".format(timepoint))
    start_time = time.time()

    conf = SparkConf()
    conf.setMaster("spark://10.0.0.6:7077")
    conf.setAppName("CopyCatch")
    conf.set("spark.executor.memory", "1000m")
    conf.set("spark.executor.cores", "2")
    conf.set("spark.executor.instances", "15")
    conf.set("spark.driver.memory", "5000m")

    sc = SparkContext(conf = conf)
    print('\n\n\n\n\n================================\n\n\n')
    img_list = list(img_list)
    dataRDD = sc.parallelize(img_list, len(img_list)//100 )
   
        
    rdd = dataRDD.map(lambda x: load_from_S3(image_id=x, image_size=new_size, b=b, aws_connection=c, bucket_name=main_bucket)).filter(lambda x: compare_images(incoming_im_resized, x))
#    rdd = rdd.filter(lambda x: compare_images(incoming_im_resized, x))
    
    result = rdd.take(1)
    print("\n\n=========\n",result,"\n\n\n")
    print("Spark finished in {} seconds".format(time.time() - start_time))
    
    if result == []:
        print("\n\n\n\n\nNo match found, adding to the db...\n\n")
    else:
        print("\n\n\n\n\nDuplicate found...\n\n")
       # if replace_im:
       #     print("\n\nNew image is higher quality:\nReplacing...\n\n")
       # else:
       #     print("\n\nKeeping the old image.\n\n")


