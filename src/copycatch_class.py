from __future__ import print_function

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

# other image processing helper packages
from skimage.measure import compare_ssim 
import numpy as np



class CopyCatch(object):
    """Main Class: CopyCatch"""
    def __init__(self, 
                incoming_bucket = None,
                main_bucket = None,
                incoming_img_id = None,
                aws_access_key = None,
                aws_secret_key = None,
                same_size_MSE_cutoff = 3000, 
                diff_size_MSE_cutoff = 5000,
                structural_similarity_cutoff = 0.7,
                redis_host = 'redis-db.7ptpwl.ng.0001.use1.cache.amazonaws.com'):

        super(CopyCatch, self).__init__()
        self.incoming_bucket = incoming_bucket
        self.main_bucket = main_bucket
        self.incoming_img_id = incoming_img_id
        self.awsak = aws_access_key
        self.awssk = aws_secret_key
        self.same_size_MSE_cutoff = same_size_MSE_cutoff
        self.diff_size_MSE_cutoff = diff_size_MSE_cutoff
        self.ssim_cutoff = structural_similarity_cutoff
        self.redis_host = redis_host


        self.r_tags = redis.StrictRedis(host=redis_host, port=6379, db=1)
        self.r_tag_levels = redis.StrictRedis(host=redis_host, port=6379, db=2)
        self.r_incoming_tags = redis.StrictRedis(host=redis_host, port=6379, db=3)
        self.r_labels = redis.StrictRedis(host=redis_host, port=6379, db=4)    
        self.r_stats = redis.StrictRedis(host=redis_host, port=6379, db=5)

        self.incoming_img_tags = []
        self.incoming_img_tag_labels = ''

        self.img_list = []
        self.incoming_img_multichannel = True
        self.incoming_im_resized = []

        self.result = []
        self.stats_list = []


    def load_incoming_image(self):

        self.incoming_im_resized = load_from_S3(ak=self.awsak, 
                                            sk=self.awssk, 
                                            image_id=self.incoming_img_id, 
                                            image_size=(128, 128), 
                                            bucket_name=self.incoming_bucket)


        if not isinstance(self.incoming_im_resized[0], np.ndarray):

            print(self.incoming_im_resized)
            print("\n\n\nIncoming image is NULL\n\n\n")
            sys.exit()
        

        if len(np.shape(self.incoming_im_resized[0])) < 3:

            self.incoming_img_multichannel = False



    def get_incoming_image_tags_labels(self):

        if self.incoming_img_id[-2:] == 'wm':

            self.incoming_img_tags = list(self.r_incoming_tags.smembers(self.incoming_img_id[:-2]))

        else:

            self.incoming_img_tags = list(self.r_incoming_tags.smembers(self.incoming_img_id))


        print("\n\n\nGOT TAGS FOR INCOMING IMAGE:", self.incoming_img_id)
        

        for i in self.incoming_img_tags:

            self.incoming_img_tag_labels += self.r_labels.get(i) + ', '
            

        self.incoming_img_tag_labels = self.incoming_img_tag_labels[:-2]

        self.stats_list.append(self.incoming_img_tag_labels)

        print(self.incoming_img_tag_labels)
        print("\n\n\n")


       
    def get_image_ids_for_filtering(self):     

        self.img_list = list(get_img_id_list(self.incoming_img_tags, self.r_tags, self.r_tag_levels))
        total_img = self.r_tags.get('size')
        self.stats_list.append(total_img)
        num_filtered = len(self.img_list)
        self.stats_list.append(num_filtered)


    def find_copy_spark(self):


        conf = SparkConf()
        conf.setMaster("spark://10.0.0.12:7077")
        conf.setAppName("CopyCatch")
        conf.set("spark.executor.memory", "1000m")
        conf.set("spark.executor.cores", "2")
        conf.set("spark.executor.instances", "15")
        conf.set("spark.driver.memory", "5000m")

        sc = SparkContext(conf = conf)
#        sc.setLogLevel("ERROR")

        partition = self.get_partition_size(len(self.img_list))
        # set the following variables to avoid "can't pickle _thread.lock objects" error
        img_list = self.img_list
        incoming_img_multichannel = self.incoming_img_multichannel
        awsak = self.awsak
        awssk = self.awssk
        main_bucket = self.main_bucket
        incoming_im_resized = self.incoming_im_resized
        same_size_MSE_cutoff = self.same_size_MSE_cutoff
        diff_size_MSE_cutoff = self.diff_size_MSE_cutoff
        ssim_cutoff = self.ssim_cutoff


        
        img_ids_rdd = sc.parallelize(img_list, partition)


        img_rdd = img_ids_rdd.map\
        (lambda x: load_from_S3(gs=(not incoming_img_multichannel), 
                                                         ak=awsak, 
                                                         sk=awssk, 
                                                         image_id=x,  
                                                         bucket_name=main_bucket))

        img_rdd_null_filter = img_rdd.filter\
        (lambda x: is_same_size(incoming_im_resized[0], x[0]))

        ssim_img_filter = img_rdd_null_filter.filter\
        (lambda x: compare_ssim(incoming_im_resized[0], 
                                x[0], 
                                multichannel=incoming_img_multichannel) > ssim_cutoff)

        mse_img_filter = ssim_img_filter.filter\
        (lambda x: compare_images(incoming=incoming_im_resized, 
                                  existing=x, 
                                  same_size_MSE_cutoff=same_size_MSE_cutoff, 
                                  diff_size_MSE_cutoff=diff_size_MSE_cutoff))


        result = mse_img_filter.take(1)
        self.result = result



    def save_runtimes(self, redis_time, spark_time, tot_time):
        
        self.stats_list.append(round(redis_time, 2))
        self.stats_list.append(round(spark_time, 2))
        self.stats_list.append(round(tot_time, 2))



    def update_stats_db(self, redis_time, spark_time, tot_time):

        """stats database: for frontend to read

    id => [tags as words, 
           total num, 
           num filtered, 
           redis tag retr time, 
           spark filter time, 
           tot time, 
           structural similarity, 
           url original, 
           url new]
        """

        # connect to S3 bucket
        c = boto.connect_s3()
        src = c.get_bucket(self.incoming_bucket, validate=False)
        dst = c.get_bucket(self.main_bucket, validate=False)
        k_src = Key(src)
        k_dst = Key(dst)
        

        # database updates differ depending on the result
        if self.result == []:
            print("\n\n\n\n\nNo match found, adding to the db...\n\n")
            print("Adding to the database..")
                
            k_src.key = "{}.jpg".format(self.incoming_img_id)          
            k_dst.key = "valid/img{}.jpg".format(self.incoming_img_id)

            # copy image from source bucket to the main bucket
            dst.copy_key(k_dst.key, src.name, k_src.key) 


            print("Updating tags database..")
            update_db(self.incoming_img_tags, "img{}".format(self.incoming_img_id), self.r_tags)  

            sample_diff_img = load_from_S3(ak=self.awsak, 
                                           sk=self.awssk, 
                                           image_id=self.img_list[0],   
                                           bucket_name=self.main_bucket)

            self.save_runtimes(redis_time, spark_time, tot_time)
            
            stats_list.append("Not found")

            ssim = compare_ssim(self.incoming_im_resized[0], 
                                sample_diff_img[0], 
                                multichannel=(not self.incoming_img_multichannel))
            self.stats_list.append(str(round(ssim * 100, 2))+"%")

            # get URLs for images to display on the UI
            k_src.key = "{}{}.jpg".format(self.incoming_im_resized[3], self.incoming_img_id)
            k_dst.key = "{}{}.jpg".format(sample_diff_img[3], sample_diff_img[2])
            url_orig = k_dst.generate_url(expires_in=0, query_auth=False)
            url_incoming = k_src.generate_url(expires_in=0, query_auth=False)

            self.stats_list.append(url_orig)
            self.stats_list.append(url_incoming)

        else:
            print("\n\n\n\n\nDuplicate found...\n\n")
            
            self.save_runtimes(redis_time, spark_time, tot_time)

            self.stats_list.append("Found")

            ssim = compare_ssim(self.incoming_im_resized[0], 
                                self.result[0][0], 
                                multichannel=(not self.incoming_img_multichannel))
            self.stats_list.append(str(round(ssim * 100, 2))+"%")

            k_src.key = "{}{}.jpg".format(self.incoming_im_resized[3],self.incoming_img_id)
            k_dst.key = "{}{}.jpg".format(self.result[0][3], self.result[0][2])
            url_orig = k_dst.generate_url(expires_in=0, query_auth=False)
            url_incoming = k_src.generate_url(expires_in=0, query_auth=False)
            self.stats_list.append(url_orig)
            self.stats_list.append(url_incoming)


        for stat in self.stats_list:
            self.r_stats.rpush(sample_diff_img[2], stat)
            


    def get_partition_size(self, num_ids):
        partition = 1
        if num_ids < 10:
            partition = 2
        else:
            partition = num_ids // 10
        return partition



