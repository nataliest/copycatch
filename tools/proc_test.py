import subprocess

cmd = "spark-submit --master spark://10.0.0.12:7077 --driver-memory 5000m --py-files image_compare.py,db_utils.py,aws_s3_utils.py sp.py --image-id 000ada55d36b4bcb"
proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
(out, err) = proc.communicate()
print "program output:", out
