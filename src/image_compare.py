# image compare functions

# image processing imports
import numpy as np
import PIL
from PIL import Image


def is_not_none(arr):
    return isinstance(arr, np.ndarray)

def is_same_size(arr1, arr2):
    if isinstance(arr1, np.ndarray) and isinstance(arr2, np.ndarray):
        return np.shape(arr1) == np.shape(arr2)

def mse(imageA, imageB):

    error = np.sum((imageA.astype("float") - imageB.astype("float")) ** 2)
    error /= float(imageA.shape[0] * imageA.shape[1])
    # return the MSE, the lower the error, the more "similar" the two images are
    return error

def compare_images(
    incoming=(None, (128,128),"img_id"), 
    existing=(None, (128,128),"img_id"),
    same_size_MSE_cutoff=0.6,
    diff_size_MSE_cutoff=1000):
    
    if not isinstance(existing[0], np.ndarray):
        return False
    
    existing_im = incoming[0]
    incoming_im = existing[0]
    
    shape_existing = existing[1]
    shape_incoming = incoming[1]
    same_size = shape_existing == shape_incoming
    

    existing_im_array = existing_im
    incoming_im_array = incoming_im

    if np.shape(existing_im_array) != np.shape(incoming_im_array):
        return False      
    images_MSE = mse(existing_im_array, incoming_im_array)
    
    if same_size:
        if images_MSE < same_size_MSE_cutoff:
            return True
        else:
            return False
    else:
        if images_MSE < diff_size_MSE_cutoff:
            return True
        else:
            return False


