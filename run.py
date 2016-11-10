import os
from pyspark import SparkConf , SparkContext
from engine  import RecommendationEngine

from pyspark.mllib.recommendation import Rating

def init_spark():
    conf = SparkConf().setAppName( 'movie-recommendations' )
    sc   = SparkContext( conf=conf , pyFiles=['engine.py'] )

    return sc

def main():
    # Initialise spark context and set the data path
    sc        = init_spark()
    data_path = os.path.join( 'data' , 'idontreallyknow' )

    # Initialise the recommendation engine
    re        = RecommendationEngine( sc , dat_path )

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

    re.add_ratings( 0 , new_ratings )

    print re.get_recommended( 0 , 25 )
    

if __name__ == "__main__":
    main()
