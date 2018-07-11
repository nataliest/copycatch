from __future__ import print_function
import argparse
import time

from copycatch_class import CopyCatch



        

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


    copycatcher = CopyCatch(incoming_bucket=incoming_bucket,
                            main_bucket = main_bucket,
                            incoming_img_id = incoming_img_id,
                            aws_access_key = awsak,
                            aws_secret_key = awssk)
    
    
    copycatcher.load_incoming_image()


    img_loading_time = time.time() - start_time
    print("\n\n\nIncoming image loaded from S3 bucket. Getting tags...\n")
    start_time = time.time()


    copycatcher.get_incoming_image_tags_labels()


    print("Got tags for incoming image:\n")
    print(copycatcher.incoming_img_tag_labels)
    print("\n\n\nGetting images with the same ids from the database...")
    start_time = time.time()


    copycatcher.get_image_ids_for_filtering()


    redis_time = time.time()-start_time
    print("Filter returned {} images".format(len(copycatcher.img_list)))
    print("\n\nFiltered image ids in {} seconds.\n\n\n".format(redis_time))
    start_time = time.time()


    copycatcher.find_copy_spark()


    spark_time = time.time() - start_time
    print("Spark finished in {} seconds".format(spark_time))
    print(copycatcher.result)
    
#    copycatcher.update_stats_db()


    


