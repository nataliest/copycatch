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

from skimage.measure import compare_ssim 
import boto
from boto.s3.key import Key
from boto.exception import S3ResponseError
import io
import PIL
from PIL import Image
import numpy as np


def is_not_none(arr):
    if not isinstance(arr, np.ndarray):
        return False
    else:
        return True

def parse_arguments():
    parser = argparse.ArgumentParser(description="spark-submit python file argument parser")
    parser.add_argument('-id', '--image-id', dest='image_id', type=str, help='Incoming image id; also s3 bucket key.')
    parser.add_argument('-mbn', '--main-bucket-name', dest='main_bucket_name', type=str, default='open-images-bucket', help='Main databaset bucket, approx. 600GB.')
    parser.add_argument('-tbn', '--temp-bucket-name', dest='temp_bucket_name', type=str, default='small-open-images-bucket', help='S3 bucket for temporary storage of uploaded images.')
    parser.add_argument('-ak', '--access-key', dest='access_key', type=str, default='', help='AWS access key.')
    parser.add_argument('-sk', '--secret-key', dest='secret_key', type=str, default='', help='AWS secret key.')
    args = parser.parse_args()
    return vars(args)

if __name__ == "__main__":

    tot_start_time = time.time()

    args = parse_arguments()

    incoming_bucket = args['temp_bucket_name']
    main_bucket = args['main_bucket_name']
    incoming_img_id = args['image_id']
    awsak = args['access_key']
    awssk = args['secret_key']

    start_time = time.time()
    
    new_size = (128, 128)
    
    
    # TAGS => DB = 1
    # LEVELS => DB = 2
    # IMG_ID - tags[] for validation dataset => DB = 3
    r_incoming_tags = redis.StrictRedis(host='redis-db.7ptpwl.ng.0001.use1.cache.amazonaws.com', port=6379, db=3)
    r_tags = redis.StrictRedis(host='redis-db.7ptpwl.ng.0001.use1.cache.amazonaws.com', port=6379, db=1)
    r_tag_levels = redis.StrictRedis(host='redis-db.7ptpwl.ng.0001.use1.cache.amazonaws.com', port=6379, db=2)
    r_labels = redis.StrictRedis(host='redis-db.7ptpwl.ng.0001.use1.cache.amazonaws.com', port=6379, db=4)    

    incoming_img_tags = list(r_incoming_tags.smembers(incoming_img_id))
    print("\n\n\nGOT TAGS FOR INCOMING IMAGE:", incoming_img_id)
    labels_list = []
    for i in incoming_img_tags:
        labels_list.append(r_labels.get(i))
    print(labels_list)
    print("\n\n\n")
   
    img_list = get_img_id_list(incoming_img_tags, r_tags, r_tag_levels)
    
    redis_time = time.time()-start_time

    print("Filter returned {} images".format(len(img_list)))
    print("\n\nFiltered image ids in {} seconds. Getting incoming image from S3...\n\n\n".format(redis_time))
    start_time = time.time() 


    incoming_im_resized = load_from_S3(ak=awsak, sk=awssk, image_id=incoming_img_id, image_size=new_size,  bucket_name=incoming_bucket)
    if not isinstance(incoming_im_resized[0], np.ndarray):
        print(incoming_im_resized)
        print("\n\n\nIncoming image is NULL\n\n\n")
        sys.exit()
    
    grayscale = False
    if len(np.shape(incoming_im_resized[0])) < 3:
        grayscale = True
    timepoint = time.time() - start_time  

    
    print("\n\nFetched incoming image in {} seconds\n\nStarting Spark.....\n\n\n".format(timepoint))
    
    start_time = time.time()

    conf = SparkConf()
    conf.setMaster("spark://10.0.0.12:7077")
    conf.setAppName("CopyCatch")
    conf.set("spark.executor.memory", "1000m")
    conf.set("spark.executor.cores", "2")
    conf.set("spark.executor.instances", "15")
    conf.set("spark.driver.memory", "5000m")

    sc = SparkContext(conf = conf)
    sc.setLogLevel("ERROR")

    img_list = list(img_list)
    num_ids = len(img_list)
    partition = num_ids // 2

    dataRDD = sc.parallelize(img_list, partition)

    mult = not grayscale
    rdd = dataRDD.map(lambda x: load_from_S3(gs=grayscale, ak=awsak, sk=awssk, image_id=x, image_size=new_size,  bucket_name=main_bucket))
    rdd = rdd.filter(lambda y: is_not_none(y[0]))
    rdd = rdd.filter(lambda x: compare_images(incoming=incoming_im_resized, existing=x, same_size_MSE_cutoff=3000, diff_size_MSE_cutoff=5000))
    result = rdd.take(10)
    
    spark_time = time.time() - start_time

    print("Spark finished in {} seconds".format(spark_time))
    
    c = boto.connect_s3()
    src = c.get_bucket(incoming_bucket, validate=False)
    dst = c.get_bucket(main_bucket, validate=False)
    k_src = Key(src)
    k_dst = Key(dst)

    if result == []:
        print("\n\n\n\n\nNo match found, adding to the db...\n\n")
        print("Adding to the database..")
            # connect to S3 bucket

        k_src.key = "{}.jpg".format(incoming_img_id)
       
        k_dst.key = "valid/img{}.jpg".format(incoming_img_id)
        dst.copy_key(k_dst.key, src.name, k_src.key) 
        print("Updating tags database..")
        update_db(incoming_img_tags, "img{}".format(incoming_img_id), r_tags)    
    else:
        print("\n\n\n\n\nDuplicate found...\n\n")
        r_stats = redis.StrictRedis(host='redis-db.7ptpwl.ng.0001.use1.cache.amazonaws.com', port=6379, db=5)
        stats_list = []

        tag_labels = str(labels_list)
        stats_list.append(tag_labels)

        total_img = r_tags.get('size')
        stats_list.append(total_img)

        num_filtered = len(img_list)
        stats_list.append(num_filtered)

        stats_list.append(redis_time)
        stats_list.append(spark_time)

        tot_time = time.time() - tot_start_time
        stats_list.append(tot_time)

        ssim = compare_ssim(incoming_im_resized[0], result[0][0])
        stats_list.append(ssim)

        k_src.key = "{}{}.jpg".format(incoming_im_resized[3],incoming_img_id)
        k_dst.key = "{}{}.jpg".format(result[0][?][3], result[0][?])
        url_orig = k_dst.generate_url(expires_in=0, query_auth=False)
        url_incoming = k_src.generate_url(expires_in=0, query_auth=False)
        stats_list.append(url_orig)
        stats_list.append(url_incoming)
        print(stats_list)
        for stat in stats_list:
            r_stats.lpush(result[0][2], stat)
        # id => [tags as words, total num, num filtered, redis tag retr time, spark filter time, tot time, struct sim, url original, url new]
        # get urls
        # get structural similarity
        # save tags
        # save redis retrieval time
        # save spark run time
        # key = id
        # print total images
        # num images returned by tag filter



