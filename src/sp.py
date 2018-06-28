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
def is_not_none(arr):
    if not isinstance(arr, np.ndarray):
        return False
    else:
        return True

def parse_arguments():
    parser = argparse.ArgumentParser(description="spark-submit python file argument parser")
    parser.add_argument('-id', '--image-id', dest='image_id', type=str, help='Incoming image id; also s3 bucket key.')
    parser.add_argument('-mbn', '--main-bucket-name', dest='main_bucket_name', type=str, default='open-images-bucket', help='Main databaset bucket, approx. 600GB.')
    parser.add_argument('-tbn', '--temp-bucket-name', dest='temp_bucket_name', type=str, default='temp-oi-bucket', help='S3 bucket for temporary storage of uploaded images.')
    parser.add_argument('-ak', '--access-key', dest='access_key', type=str, default='', help='AWS access key.')
    parser.add_argument('-sk', '--secret-key', dest='secret_key', type=str, default='', help='AWS secret key.')
    args = parser.parse_args()
    return vars(args)

if __name__ == "__main__":

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

    incoming_img_tags = r_incoming_tags.smembers(incoming_img_id)
    print("\n\n\nGOT TAGS FOR INCOMING IMAGE:", incoming_img_id)
    labels_list = []
    for i in list(incoming_img_tags):
        labels_list.append(r_labels.get(i))
    print(labels_list)
    print("\n\n\n")
    # incoming_img_tags = get_tags_for_incoming(image_id = incoming_img_id)
    img_list = get_img_id_list(incoming_img_tags, r_tags, r_tag_levels)
    print("\n\nFiltered image ids in {} seconds. Getting incoming image from S3...\n\n\n".format(time.time()-start_time))
    start_time = time.time() 


   
    # connect to S3 bucket
#    c = boto.connect_s3()
#    b_incoming = c.get_bucket(incoming_bucket, validate=False)
#    b = c.get_bucket(main_bucket, validate=False)
    c, b_incoming, b = '', '', ''

    incoming_im_resized = load_from_S3(ak=awsak, sk=awssk, image_id=incoming_img_id, image_size=new_size, b=b_incoming, aws_connection=c, bucket_name='small-open-images-bucket')
    if not isinstance(incoming_im_resized[0], np.ndarray):
        print(incoming_im_resized)
        print("\n\n\nIncoming image is NULL\n\n\n")
        sys.exit()
    
    grayscale = False
    if len(np.shape(incoming_im_resized[0])) < 3:
        grayscale = True
    timepoint = time.time() - start_time
    print(type(incoming_im_resized))
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
   # print('\n\n\n\n\n================================\n\n\n')
    img_list = list(img_list)
    num_ids = len(img_list)
    partition = 1
    if num_ids > 100000:
        partition = 10000
    elif num_ids > 10000:
        partition = 5000
    elif num_ids > 1000:
        partition = 100
    elif num_ids > 100:
        partition = 50
    else:
        partition = 10
    dataRDD = sc.parallelize(img_list, partition)
    print(incoming_im_resized)   
    print(np.shape(incoming_im_resized[0]))
    mult = not grayscale
    print(mult)        
#    rdd = dataRDD.map(lambda x: load_from_S3(gs=grayscale, ak=awsak, sk=awssk, image_id=x, image_size=new_size, b=b, aws_connection=c, bucket_name=main_bucket)).filter(lambda y: is_not_none(y[0])).filter(lambda y: compare_ssim(incoming_im_resized[0], y[0], multichannel=mult)>0.4).filter(lambda x: compare_images(incoming_im_resized, x))
#    rdd = dataRDD.map(lambda x: load_from_S3(ak=awsak, sk=awssk, image_id=x, image_size=new_size, b=b, aws_connection=c, bucket_name=main_bucket)).filter(lambda x: compare_images(incoming_im_resized, x))
#    rdd = rdd.filter(lambda x: compare_images(incoming_im_resized, x))
#    rdd = dataRDD.map(lambda x: load_from_S3(gs=grayscale, ak=awsak, sk=awssk, image_id=x, image_size=new_size, b=b, aws_connection=c, bucket_name=main_bucket)).filter(lambda y: is_not_none(y[0])).filter(lambda x: compare_images(incoming_im_resized, x))
    rdd = dataRDD.map(lambda x: load_from_S3(gs=grayscale, ak=awsak, sk=awssk, image_id=x, image_size=new_size, b=b, aws_connection=c, bucket_name=main_bucket))
    rdd = rdd.filter(lambda y: is_not_none(y[0]))
    rdd = rdd.filter(lambda x: compare_images(incoming_im_resized, x))
    result = rdd.take(1)
   # print("\n\n=========\n",result,"\n\n\n")
    print("Spark finished in {} seconds".format(time.time() - start_time))
    
    if result == []:
        print("\n\n\n\n\nNo match found, adding to the db...\n\n")
#        add = input("Would you like to add the image to the db?")
#        if add:
#            src = b_incoming
#            dst = b
#            k = Key(src)
#            k
#            k.key = "valid/img{}.jpg".format(incoming_img_id)
#            dst.copy_key(k.key, src.name, k.key)    
    else:
        print("\n\n\n\n\nDuplicate found...\n\n")
       # if replace_im:
       #     print("\n\nNew image is higher quality:\nReplacing...\n\n")
       # else:
       #     print("\n\nKeeping the old image.\n\n")


