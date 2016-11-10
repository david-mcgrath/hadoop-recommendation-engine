import os
from pyspark.mllib.recommendation import ALS

# Recommendation engine class
class RecommendationEngine:

    # Trains the model
    def __train_model( self ):
        self.model = ALS.trainImplicit( self.data , self.rank , seed=self.seed , iterations = self.iterations , lambda_=self.reg_para )

    # Adds new plays to the existing RDD and retrains the model
    # Note that this should be done in batches, not just for every new play
    def add_plays( self , plays ):
        # Change to an RDD
        new_data  = self.sc.parallelize( plays )

        # CHANGE IT TO SUM THE NUMBER OF PLAYS FOR EXISTING USERS
        self.data = self.data.union( new_data )

        # Retrain model
        self.__train_model()

        return plays

    # Gets the top n recommended artists for the user
    def get_recommended( self , user , n ):
        # NEED A LIST OF ARTISTS!

        # "New" artists are ones which the user doesn't have any recorded plays of their tracks
        new_artists = self.artists.filter( lambda plays: not plays[1]==user_id ).map( lambda x: (user_id,x[0]) )

        # Get predicted ratings
        ratings = self.__predict_ratings( new_artists ).takeOrdered( n , key=lambda x: -x[1] )

        return ratings
        

    # Initiliase recommendation engine. sc is spark context
    def __init__( self , sc , data_path ):
        # Model parameters
        self.rank       = 8   # Number of implicit factors in the model
        self.seed       = 5L  # Seed
        self.iterations = 10  # Number of iterations for ALS
        self.reg_para   = 0.1 # Regularisation parameter, would just call it lambda but... Python

        # Other parameters
        file_name       = os.path.join( data_path , 'userid-timestamp-artid-artname-traid-traname.tsv' )

        # Spark context
        self.sc = sc

        # Load dataset
        raw    = sc.textFile( view_path ) # Load
        header = raw.take(1)[0]           # Get header, to remove it later
        
        # Parse into RDD. Remove header, then split at \t (tsv), take 0 and 4 (uid & trackid). Finally cache.
        data   = raw.filter( lambda l: l!=header ).map( lambda l: l.split("\t") ).map( lambda tok: (tok[0],tok[4],tok[2]) ).cache()
        
        # Check the first few entries in the RDD (for debugging only)
        # data.take(10)

        self.__train_model()

    # Attach functions to class methods
    RecommendationEngine.add_plays       = add_plays
    RecommendationEngine.get_recommended = get_recommended
