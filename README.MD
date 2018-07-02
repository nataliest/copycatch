# CopyCatcher - efficient copy finder for large databases

Insight Data Engineering Project

CopyCatcher aims to solve:
- file duplication problem
- copyright infringement problem

After filtering almost two million images by tags in under a second, the app performs MSE-error calculation on the filtered images and the incoming image in parallel using Spark. The image metadata is stored in Redis.

<img src="./docs/CopyCatch_pipeline.png" alt="pipeline_pp">

Dataset:
Open Images Dataset: ~600GB of images
 
