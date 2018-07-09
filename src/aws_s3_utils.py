# S3 imports
import boto
from boto.s3.key import Key
from boto.exception import S3ResponseError
import io
import PIL
from PIL import Image
import numpy as np

def load_from_S3(image_id=None, 
                 image_size=(128,128), 
                 b=None, 
                 aws_connection=None, 
                 bucket_name=None, 
                 ak=None, 
                 sk=None, 
                 gs=False):
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
            if not gs and len(np.shape(img)) < 3:
                return None, None, image_id, p
            aws_connection.close()
            return img, size, image_id, p
        except S3ResponseError:
            pass
    aws_connection.close()
    return img, size, image_id, ''
