import os
from pyspark import SparkConf , SparkContext
from engine  import RecommendationEngine

from pyspark.mllib.recommendation import Rating

credentials = {
  "auth_url": "https://identity.open.softlayer.com",
  "project": "object_storage_ee9ca24f_81be_4ef0_a658_314e27fad09a",
  "project_id": "8b9bd11cb9294148b63b784fc254f34e",
  "region": "dallas",
  "user_id": "c0a11a5e37f94cb4a6778b1445761fc9",
  "username": "admin_ab95c493a010d6c326ad7e6d2e6dd047c44a4b54",
  "password": "r8#.b7~cVd).kXeR",
  "domain_id": "ed55f698827b43b59ebc0d2dddb36fba",
  "domain_name": "1154171",
  "role": "admin"
}

def set_credentials(sc,creds):
    prefix = "fs.swift.service." + 'TEST'
    hconf = sc._jsc.hadoopConfiguration()
    hconf.set(prefix + ".auth.url", creds['auth_url'] + '/v2.0/tokens')
    hconf.set(prefix + ".auth.endpoint.prefix", "endpoints")
    hconf.set(prefix + ".tenant", creds['project_id'])
    hconf.set(prefix + ".username", creds['user_id'])
    hconf.set(prefix + ".password", creds['password'])
    hconf.setInt(prefix + ".http.port", 8080)
    hconf.set(prefix + ".region", creds['region'])
    hconf.setBoolean(prefix + ".public", True)
    
def init_spark():
    conf = SparkConf().setAppName( 'movie-recommendations' )
    sc   = SparkContext( conf=conf )

    set_credentials( sc , credentials )

    return sc

def main():
    # Initialise spark context and set the data path
    sc        = init_spark()
    data_path = 'swift://notebooks.TEST'

    # Initialise the recommendation engine
    re        = RecommendationEngine( sc , data_path )

    # Run through a simple test, adding some ratings and then getting recommendations based on them
    new_ratings = [
        (0,260,4.0), # Star Wars
        (0,1,3.0),   # Toy Story
        (0,16,3.0),  # Casino
        (0,25,4.0),  # Leaving Las Vegas
        (0,32,4.0),  # Twelve Monkeys
        (0,335,1.0), # Flintstones, The
        (0,379,1.0), # Timecop
        (0,296,3.0), # Pulp Fiction
        (0,858,5.0), # Godfather, The
        (0,50,4.0)   # Usual Suspects, The
        ]

    re.add_ratings( new_ratings )

    print re.get_recommended( 0 , 25 )
    

if __name__ == "__main__":
    main()
