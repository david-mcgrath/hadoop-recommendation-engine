# hadoop-recommendation-engine
Recommendation engine for music using last.fm dataset, with Spark running on Hadoop
Changed to MovieLens dataset because it's a lot cleaner and easier to use. Small version of the MovieLens dataset included, the large one failed to upload and caused syncing issues.

**TO RUN:**
  Start Apache Spark instance on IBM Bluemix
  Create a linked Object Storage
  Copy the contents of the dataset folder to the Object Storage
  
  **EDIT CREDENTIALS:**
    vcap.json contains the Spark cluster credentials
    Start of run.py contains Object Storage credentials (if this isn't altered it will fail with a Null Pointer Exception)
  
  Run RUNTHIS.sh in bash, can be used with \*nix or the Windows 10 bash terminal
  This runs spark-submit.sh with the required commands to upload run.py and engine.py and execute run.py on the Spark cluster
  
  The bash terminal will poll the cluster and provide some information about progress of the task
  
  Outputs downloaded to the current folder as stdout\_\<numbers\>, along with stderr and Spark logs
  
**WILL RUN WITHOUT A NEW SPARK CLUSTER AND OBJECT STORAGE SET UP AS LONG AS THE CURRENT CLUSTER IS ACTIVE**

**File list:**
  dataset/movies_small.csv  *movie data, ids and name*
  dataset/ratings_small.csv *ratings: user_id, movie_id, rating, timestamp. 100k ratings total*
  logs/bash output          *outputs from bash for an example run of the task*
  logs/Interesting Logs     *failed with full dataset, appears to run out of resources*
  logs/stdout               *output, cleaned up slightly to be more readable with the test ratings included*
  bluemix_get_file.py       *old mostly pointless script for reading a file from Object Storage, used before credentials issues were sorted out*
  engine.py                 *RecommendationEngine class*
  key.ppk                   *unimportant*
  README.md                 *this file, obviously*
  run.py                    *main python script, trains the model and then adds test ratings for a new user (retraining), then gets recommendations*
  RUNTHIS.sh                *executes the task*
  spark-submit.sh           *script to interface with the Spark cluster, provided by IBM*
  vcap.json                 *Spark credentials*
