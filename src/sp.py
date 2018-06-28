from __future__ import print_function
import argparse
import time
import sys
# custom modules
#from image_compare import * 
from  db_utils import *
#from  aws_s3_utils import * 

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

def load_from_S3(image_id=None, image_size=(128,128), b=None, aws_connection=None, bucket_name=None, ak=None, sk=None, gs=False):
    img = None
    size = None
    aws_connection = boto.connect_s3(aws_access_key_id=ak, aws_secret_access_key=sk)

    b = aws_connection.get_bucket(bucket_name, validate=False)
    # possible folders in the S3 bucket
    prefix = ['', 'train/', 'test/', 'valid/']
    for p in prefix:
        k = Key(b)
        k.key = '{}{}.jpg'.format(p, image_id)
        try:        
            s = k.get_contents_as_string()
            img = Image.open(io.BytesIO(s))
            size = img.size
            img = np.asarray(img.resize(image_size))
            print(np.shape(img))
            if not gs and len(np.shape(img)) < 3:
                return None, None, image_id
            aws_connection.close()
            return img, size, image_id
        except S3ResponseError:
            print(k.key, "not found")
            pass
    aws_connection.close()
    return img, size, image_id
def mse(imageA, imageB):
    # the 'Mean Squared Error' between the two images is the
    # sum of the squared difference between the two images;
    # NOTE: the two images must have the same dimension
    print(np.size(imageA), np.size(imageB))
    err = np.sum((imageA.astype("float") - imageB.astype("float")) ** 2)
    err /= float(imageA.shape[0] * imageA.shape[1])
    # return the MSE, the lower the error, the more "similar" the two images are
    return err

def compare_images(
    incoming=(None, (128,128),"img_id"), 
    existing=(None, (128,128),"img_id"),
    same_size_MSE_cutoff=0.6,
    diff_size_MSE_cutoff=1000):
    print("in compare")
    print(type(incoming[0]))
    print(type(existing[0]))    
    if not isinstance(existing[0], np.ndarray) :
        return False
    print("passed 1")    
    existing_im = incoming[0]
    incoming_im = existing[0]
    
    shape_existing = existing[1]
    shape_incoming = incoming[1]
    same_size = shape_existing == shape_incoming
    

    existing_im_array = existing_im
    incoming_im_array = incoming_im
    if np.shape(existing_im_array) != np.shape(incoming_im_array):
        return False      
    print("passed 2")
    images_MSE = mse(existing_im_array, incoming_im_array)
    
    if same_size:
        if images_MSE < same_size_MSE_cutoff:
            print("Same image")
            return True
        else:
            print("Different image")
            return False
    else:
        if images_MSE < diff_size_MSE_cutoff:
            print("Same image")
            return True
        else:
            print("Different image")
            return False

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

    incoming_img_tags = list(r_incoming_tags.smembers(incoming_img_id))
    print("\n\n\nGOT TAGS FOR INCOMING IMAGE:", incoming_img_id)
    labels_list = []
    for i in incoming_img_tags:
        labels_list.append(r_labels.get(i))
    print(labels_list)
    print("\n\n\n")
   
    img_list = get_img_id_list(incoming_img_tags, r_tags, r_tag_levels)
    print("\n\nFiltered image ids in {} seconds. Getting incoming image from S3...\n\n\n".format(time.time()-start_time))
    start_time = time.time() 


    incoming_im_resized = load_from_S3(ak=awsak, sk=awssk, image_id=incoming_img_id, image_size=new_size,  bucket_name='small-open-images-bucket')
    if not isinstance(incoming_im_resized[0], np.ndarray):
        print(incoming_im_resized)
        print("\n\n\nIncoming image is NULL\n\n\n")
        sys.exit()
    
    grayscale = False
    if len(np.shape(incoming_im_resized[0])) < 3:
        grayscale = True
    timepoint = time.time() - start_time

    print("img_list", img_list)    

    test_list = []
    for img in img_list:
        im = load_from_S3(ak=awsak, sk=awssk, image_id=img, image_size=new_size,  bucket_name='open-images-bucket')
        print(im)
        test_list.append(im)

    print(len(test_list))
    for img in test_list:
        print(compare_images(incoming=incoming_im_resized, existing=img))
    
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
    partition = 0
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

    mult = not grayscale
    rdd = dataRDD.map(lambda x: load_from_S3(gs=grayscale, ak=awsak, sk=awssk, image_id=x, image_size=new_size,  bucket_name=main_bucket)).filter(lambda x: compare_images(incoming_im_resized, x))
#    rdd = rdd.filter(lambda y: is_not_none(y[0]))
#    rdd = rdd.filter(lambda x: compare_images(incoming_im_resized, x))
    result = rdd.take(1)
     
    print("Spark finished in {} seconds".format(time.time() - start_time))
    
    if result == []:
        print("\n\n\n\n\nNo match found, adding to the db...\n\n")
        print("Adding to the database..")
            # connect to S3 bucket
        c = boto.connect_s3()
        src = c.get_bucket('small-open-images-bucket', validate=False)
        dst = c.get_bucket(main_bucket, validate=False)
        k_src = Key(src)
        k_src.key = "{}.jpg".format(incoming_img_id)
        k_dst = Key(dst)
        k_dst.key = "valid/img{}.jpg".format(incoming_img_id)
        dst.copy_key(k_dst.key, src.name, k_src.key) 
        print("Updating tags database..")
        update_db(incoming_img_tags, "img{}".format(incoming_img_id), r_tags)    
    else:
        print("\n\n\n\n\nDuplicate found...\n\n")



