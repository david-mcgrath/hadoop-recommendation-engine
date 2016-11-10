# STILL NEED TO MAKE SURE ALL OF THE INDICES ARE CORRECT!

import os
from pyspark.mllib.recommendation import ALS, Rating

# import bluemix_get_file as bm

# Recommendation engine class
class RecommendationEngine:

    # Trains the model
    def __train_model( self ):
        self.model = ALS.train( self.data , self.rank , seed=self.seed , iterations = self.iterations , lambda_=self.reg_para )

    # Adds new ratings to the existing RDD and retrains the model
    # Note that this should be done in batches, not just for every new play
    def add_ratings( self , ratings ):
        # Change to an RDD
        new_data  = self.sc.parallelize( ratings )

        # CHANGE IT TO SUM THE NUMBER OF PLAYS FOR EXISTING USERS
        self.data = self.data.union( new_data )

        # Retrain model
        self.__train_model()

        return ratings

    # Predicts ratings for a given set of user and artist pairs
    # Returns RDD ( artist_id , artist_name , rating )
    def __predict_ratings( self , unrated ):
        predicted             = self.model.predictAll( unrated )
        predicted_rating      = predicted.map( lambda x: (x.product,x.rating) )
        predicted_rating_name = predicted_rating.join( self.movies )

        return predicted_rating_name

    # Gets the top n recommended movies for the user
    def get_recommended( self , user_id , n ):
        # NEED A LIST OF ARTISTS!

        # Gets list of movies the user hasn't rated
        # codementor.io tutorial had this wrong I believe FIX IT LATER
        unrated = self.artists.filter( lambda plays: not plays[1]==user_id ).map( lambda x: (user_id,x[0]) )

        # Get predicted ratings
        ratings = self.__predict_ratings( unrated ).takeOrdered( n , key=lambda x: -x[1] )

        return ratings
        

    # Initiliase recommendation engine. sc is spark context
    def __init__( self , sc , data_path ):
        # Model parameters
        self.rank       = 8   # Number of implicit factors in the model
        self.seed       = 5L  # Seed
        self.iterations = 10  # Number of iterations for ALS
        self.reg_para   = 0.1 # Regularisation parameter. Would just call it lambda but... Python

        # Other parameters
        # SWITCHING OVER TO MOVIE DATASET (formatted in a nicer way)
        ratings_path = os.path.join( data_path , 'ratings.csv' )
        movies_path  = os.path.join( data_path , 'movies.csv' )

        # Spark context
        self.sc = sc

        # Load dataset
        raw    = sc.textFile( ratings_path ) # Load
        # raw    = sc.parallelize( bm.get_ratings() )
        header = raw.take(1)[0]           # Get header, to remove it later

        raw_m  = sc.textFile( movies_path ) # Load
        # raw_m  = sc.parallelize( bm.get_movies() )
        head_m = raw.take(1)[0]           # Get header, to remove it later
        
        # Parse into RDD. Remove header, then split at \t (tsv), take 0,1,3 (uid,artistid,plays). Finally cache.
        self.data   =   raw.filter( lambda l: l!=header ).map( lambda l: l.split(",") ).map( lambda tok: ( Rating(int(tok[0]),int(tok[1]),float(tok[3])) ) ).cache()
        # Do the same for artistid and artistname, and also remove any repeats
        self.movies = raw_m.filter( lambda l: l!=head_m ).map( lambda l: l.split(",") ).map( lambda tok: (int(tok[0]),tok[1]) ).cache()
        
        # Check the first few entries in the RDD (for debugging only) using data.take(10)

        # And finally, train the model
        self.__train_model()

    # Attach functions to class methods
    RecommendationEngine.add_ratings     = add_ratings
    RecommendationEngine.get_recommended = get_recommended
