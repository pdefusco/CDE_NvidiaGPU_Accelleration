# CDE Nvidia GPU Accelleration

# DEMO OUTLINE

# 0. Create data with custom runtime dbldatagen - save data to table

# Create credentials for job

cde credential create --name docker-creds --type docker-basic --docker-server hub.docker.com --docker-username pauldefusco

# Create CDE custom runtime with dbldatagen

cde resource create --name dex-spark-runtime-dbldatagen --image pauldefusco/dex-spark-runtime-3.2.3-7.2.15.8:1.20.0-b15-great-expectations-data-quality --image-engine spark3 --type custom-runtime-image

# Create CDE Files Resource for Jobs and Upload Files

cde resource create --name cde-gpu-demo
cde resource upload --name cde-gpu-demo --local-path files_resource/datagen.py --local-path files_resource/utils.py --local-path files_resource/sparkquery.py

# Create CDE Job with dbldatagen runtime and run it

cde job create --name create-data --type spark --mount-1-resource cde-gpu-demo --application-file datagen.py --runtime-image-resource-name dex-spark-runtime-dbldatagen

cde job run create-data --driver-cores 4 --driver-memory "4g" --executor-core 8 --executor-memory "8g"

# 1. CREATE AND RUN the CPU PERF job from notebook

(done in notebook via CDEPY)

Equivalent CDE CLI cmd: cde job create --name perf-query --type spark --mount-1-resource cde-gpu-demo --application-file perfquery.py

# 2. Use Nvidia QUAL tool to assess whether it should be a GPU job

Download Spark Event logs from CDE:

(done in notebook via CDEPY)

Equivalent CDE CLI cmd: cde run logs --id runId

# 3. Recreate the job via CDEPY and run it with GPU option

(done in notebook via CDEPY)

Equivalent CDE CLI cmd: cde job create --name perf-query --type spark --mount-1-resource cde-gpu-demo --application-file perfquery.py --conf cde.gpu.acceleration.enabled=True

# 4. Download logs and Compare results with nvidia PROFILING tool or look at CVS's

(done in notebook via CDEPY)

Equivalent CDE CLI cmd: cde run logs --id runId
