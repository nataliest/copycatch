# S3 imports
import boto
from boto.s3.key import Key
from boto.exception import S3ResponseError
import io
import PIL
from PIL import Image

def load_from_S3(image_id=None, image_size=(128,128), b=None, aws_connection=None, bucket_name=None):
    img = None
    size = None

   # b_incoming = c.get_bucket('open-images-bucket')
    b = aws_connection.get_bucket(bucket_name, validate=False)
    # possible folders in the S3 bucket
    prefix = ['', 'train/', 'test']
    for p in prefix:
        k = Key(b)
        k.key = '{}{}.jpg'.format(p, image_id)
        try:        
            s = k.get_contents_as_string()
            img = Image.open(io.BytesIO(s))
            size = img.size
            img = img.resize(image_size)
            return img, size, image_id
        except S3ResponseError:
            pass
    return img, size, image_id
