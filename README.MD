# CopyCatch - efficient copy finder for large databases

Insight Data Engineering Project. <br>
The UI is temporarily displayed on my personal website: <br>
<a href="http://nataliest.com">nataliest.com</a> (opens in the same tab, sorry about that)

CopyCatch aims to solve:
- copyright infringement problem
- file duplication problem

After filtering almost two million images by tags in under a second, the app performs MSE-error calculation and structural similarity comparison on the filtered images and the incoming image in parallel using Spark. <br>
The image metadata is stored in Redis.

The results displayed on the website were obtained using 6 worker nodes and 18 executors 2 cores each.

<img src="./docs/CopyCatch_pipeline.png" alt="pipeline_pp">

Dataset:
Open Images Dataset: ~600GB of images with tags. <br>
The dataset csv metadata parsing scripts are not included.



    ├── docs
    │   └── CopyCatch_pipeline.png
    ├── flaskapp
    │   ├── assets
    │   │   └── aboutme.JPG
    │   ├── flaskapp.py
    │   ├── flaskapp.wsgi
    │   ├── static
    │   │   └── js
    │   │       └── dist
    │   │           ├── components
    │   └── templates
    │       ├── about.html
    │       ├── aboutme.JPG
    │       ├── detailed.html
    │       ├── index.html
    │       ├── stats.html
    │       └── tech.html
    ├── src
    │   ├── aws_s3_utils.py
    │   ├── copycatch_class.py
    │   ├── db_utils.py
    │   ├── image_compare.py
    │   └── main_spark_submit.py
    └── tools
        └── redis_db
           ├── create_label_db4.py
           ├── create_tag_db.py
           ├── create_taglevel_db.py
           ├── create_valid_id_tag_db.py

 
