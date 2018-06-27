# image compare functions

# image processing imports
import numpy as np
import PIL
from PIL import Image


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
    
    if not isinstance(existing[0], np.ndarray) :
        return False
    
    existing_im = incoming[0]
    incoming_im = existing[0]
    
    shape_existing = existing[1]
    shape_incoming = incoming[1]
    same_size = shape_existing == shape_incoming
    
#    if shape_existing[0] / shape_existing[1] != shape_incoming[0] / shape_incoming[1]:
#        print("Image is new, add image to the DB")
       #add_img_s3(from_bucket, to_bucket, incoming[2])
       #check if exists, delete first
#        return False
#    else:
#        pass
    
    # REPLACE IM SCRIPT
    #     replace_im = False
    #     if shape_existing[0] > shape_incoming[0] and shape_existing[1] < shape_incoming[1]:
    #         replace_im = True
    
    #     new_size = shape_incoming
    # else:
    #     new_size = shape_existing
    #     print(new_size)
    
   # existing_im_array = np.asarray(existing_im)
   # incoming_im_array = np.asarray(incoming_im)
    existing_im_array = existing_im
    incoming_im_array = incoming_im
    if np.shape(existing_im_array) != np.shape(incoming_im_array):
        return False      
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


