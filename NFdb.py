# Copyright 2008 Luis Rei

import MySQLdb
import glob
import datetime
from configobj import ConfigObj
from math import sqrt

class NFdb:
    """The Netflix Database Class
    Contains all database related code.
    
    """
    
    def __init__(self, configFile):
        """Initializes de database connection.
        """
        self.config = ConfigObj(configFile)
        
        self.conn = MySQLdb.connect(
			host=self.config['DB']['host'],
			user=self.config['DB']['user'],
			passwd=self.config['DB']['passwd'],
			db=self.config['DB']['dbname'])
    
    def create(self):
        """Creates the Netflix Database structure (Tables)
        """
        c = self.conn.cursor()
        
        # create the main table (ratings)
        c.execute('''create table Ratings
         (MovieID INTEGER, CustomerID INTEGER, Rating INTEGER, RatingDate DATE,
          PRIMARY KEY (MovieID, CustomerID))''')
        
        # create the movie titles table (Movies)
        c.execute('''create table Movies
         (MovieID INTEGER PRIMARY KEY, Year INTEGER, Title TEXT)''')
        
        
        #create a table that holds statistics for each customer
        c.execute('''create table CustomerStats
            (CustomerID INTEGER PRIMARY KEY, Total INTEGER, Average FLOAT,
             StdDev FLOAT, AvgW FLOAT)''')
        
        #create a table that holds statistics for each movie
        c.execute('''create table MovieStats
             (MovieID INTEGER PRIMARY KEY, Total INTEGER, Average FLOAT,
              StdDev FLOAT, AvgW FLOAT)''')
            
        #create a table to hold the nearest neigboor of a given customer
        c.execute('''create table CNN
            (CustomerID INTEGER, NeighborID INTEGER, Freq INTEGER, Sim FLOAT,
             PRIMARY KEY(CustomerID, NeighborID))''')
        
        #create a table to hold the nearest neigboor of a given customer
        c.execute('''create table MNN
            (MovieID INTEGER, NeighborID INTEGER, Freq INTEGER, Sim FLOAT,
             PRIMARY KEY(MovieID, NeighborID))''')
    
        #@todo: add the indexes
        
        self.conn.commit()
    
    def loadData(self, dbTxtFiles):
        """Loads the Netflix ratings data from the downloaded text files into
        the database.
        """
        c = self.conn.cursor()
        
        
        for filename in glob.glob(dbTxtFiles):
            f = open(filename, 'r')
            str = f.readline()
            movieID = int(str[0:str.find(":")])
            
            for line in f:
                values = line.split(",")
                customerID = int(values[0])
                rating = int(values[1])
                ratingDate = datetime.datetime(
                  *[int(x) for x in values[2].split("-")])
                
                # create a tuple to store this data
                t = (movieID, customerID, rating, ratingDate)
                
                # put data into a row on the DB
                c.execute("""insert into ratings values (%s,%s,%s,%s)""", t)
                
        
        # commit import to the database
        self.conn.commit()
    
    def loadMovieTitles(self, moviefile):
        """Loads the movie information from a text file into the database.
        """
        c = self.conn.cursor()
        f = open(moviefile, 'r')
        for line in f:
            # MovieID,YearOfRelease,Title
            values = line.split(",")
            # The YearOFRelease can be "NULL"
            if values[1] == "NULL":
                values[1] = "0"
            t = (int(values[0]), int(values[1]), values[2].strip())
            c.execute("""insert into Movies values (%s,%s,%s)""", t)
        
        self.conn.commit()
    
    
    def dropTables(self):
        """Drop the tables from the database."""
        c = self.conn.cursor()
        
        c.execute("""DROP TABLE Ratings""")
        c.execute("""DROP TABLE Movies""")
        c.execute("""DROP TABLE MovieStats""")
        c.execute("""DROP TABLE CustomerStats""")
        c.execute("""DROP TABLE CNN""")
        c.execute("""DROP TABLE MNN""")
        
        self.conn.commit()
    
    #@todo:
    def createAndLoad(self):
        """Creates the database and loads all the data into it, including
           probe and customer and movie stats."""
        
        self.create()
        self.loadMovieTitles(dbTxtFiles)
        self.createProbeTable()
        self.loadProbeData(probeFile)
        
    def query(self, id):
        c = self.conn.cursor()
        
        c.execute("""SELECT * FROM Ratings WHERE id = %d""" % id)
        
        for row in c:
            print row
    
    def createProbeTable(self):
        """Create the table that will hold the probe data."""
        c = self.conn.cursor()
        
        # create the table with the probe data
        c.execute('''create table probe
         (MovieID INTEGER, CustomerID INTEGER,
          PRIMARY KEY (MovieID, CustomerID))''')

    def loadProbeData(self, probeFile):
        """Loads the probe data from a text file into the database.
        """
        c = self.conn.cursor()
        
        f = open(probeFile, 'r')
        
        for line in f:
            if line.find(":") != -1:
                movieID = int(line[0:line.find(":")])
            else:
                customerID = int(line)
                # create a tuple to store this data
                t = (movieID, customerID)
                
                # put data into a row on the DB
                c.execute("""insert into probe values (%s,%s)""", t)
        
        
        # commit import to the database
        self.conn.commit()
    
    def queryProbe(self, movID, CID):
        c = self.conn.cursor()
        
        c.execute("""SELECT * FROM probe
                     WHERE movieID = %d
                     AND customerID = %d""" % (movID, CID))
        
        for row in c:
            print row
    
    def getRating(self, CID, movieID):
        """Returns the Rating given to a movie by a customer."""
        c = self.conn.cursor()
        c.execute("""SELECT Rating FROM Ratings
                     WHERE customerID = %d
                     AND movieID= %d""" % (CID, movieID))
        
        t = c.fetchone()
        if t is None:
            return -1
        return t[0]
    
    def getRatingDate(self, CID, movieID):
        """Returns the date when a rating was given."""
        c = self.conn.cursor()
        c.execute("""SELECT RatingDate FROM Ratings
                     WHERE customerID = %d
                     AND movieID= %d""" % (CID, movieID))

        t = c.fetchone()
        if t is None:
            return -1
        return t[0]
    
    def getRatedMovies(self, CID):
        """Returns the MovieIDs, Ratings and RatingDate of the movies that
        a customer rated.
        """
        m = []
        c = self.conn.cursor()
        
        c.execute("""SELECT movieID, Rating, RatingDate FROM Ratings
                     WHERE customerID = %d""" % (CID))
        
        for row in c:
            m.append(row)
        
        return m
    
    def getRatedCustomers(self, MID):
        """Returns the CIDs, Ratings and RatingDate of the Customers that
        rated a movie.
        """
        m = []
        c = self.conn.cursor()

        c.execute("""SELECT customerID, Rating, RatingDate FROM Ratings
                     WHERE mvoieID = %d""" % (MID))

        for row in c:
            m.append(row)

        return m
    
    def getMovieRatingsByCID(self, CID):
        """Returns the MovieIDs and respective Ratings that a customer made.
        """
        m = []
        c = self.conn.cursor()
        
        c.execute("""SELECT movieID, Rating FROM Ratings
                     WHERE customerID = %d""" % (CID))
        
        for row in c:
            m.append(row)
        
        return m
         
         
    def getRatingsByCID(self, CID):
         """Returns all the Ratings a customer made without the MovieID."""
         m = []
         c = self.conn.cursor()

         c.execute("""SELECT Rating FROM Ratings
                      WHERE customerID = %d""" % (CID))

         for row in c:
             m.append(row[0])

         return m
    
    def getRatedMovieIDsByCID(self, CID):
          """Returns all the MovieIDs a customer rated."""
          m = []
          c = self.conn.cursor()

          c.execute("""SELECT MovieID FROM Ratings
                       WHERE customerID = %d""" % (CID))

          for row in c:
              m.append(row[0])

          return m
    
    def getRatedMoviesByDateInterval(self, CID, start, end):
        """Returns the movieID and Ratings of the movies that a customer rated
        between a given interval of dates.
        """
        m = []
        c = self.conn.cursor()

        c.execute("""SELECT movieID, Rating FROM Ratings
                     WHERE customerID = %d
                     AND RatingDate >= %s
                     AND RatingDate <= %s""" % (CID, start, end))

        for row in c:
            m.append(row)

        return m
    
    
    def getRatingsByMID(self, movieID):
        """Fetch all the Ratings of the movie.
        Args:
            movieID: an integer with the ID of the movie.
        Returns:
            A list with all the Ratings of the movie.
        """
        m = []
        c = self.conn.cursor()

        c.execute("""SELECT Rating FROM Ratings
                     WHERE movieID = %d""" % (movieID))

        for row in c:
            m.append(row[0])

        return m
        
    def getCIDsbyMovieIDRating(self, movieID, Rating):
        """Returns all the customer IDs that gave Rating to movieID"""
        m = []
        c = self.conn.cursor()
        
        c.execute("""SELECT customerID, RatingDate FROM Ratings
                     WHERE movieID = %d
                     AND Rating = %d""" % (movieID, Rating))
        
        for row in c:
            m.append(row)
        
        return m
    
    def getCIDsbyRatedMovies(self, RatedMovies):
        """Returns a list of (movieID, Rating) and returns all CIDs with the
        same ratings, including duplicates"""
        m = []
        
        for (movieID, Rating) in RatedMovies:
            t = self.getCIDbyMovieIDRating(movieID, Rating)
            m.extend(t)

        return m
    
    
    def getCIDsMovieIDsfromProbe(self):
        """Returns a list of (CID, MovieID) from the probe ordered by CID"""
        m = []
        c = self.conn.cursor()

        c.execute("""SELECT customerID, MovieID FROM probe
                     ORDER BY customerID""")

        for row in c:
            m.append(row)
        
        return m
        
    def setCustomerStats(self):
        """Sets the values in the CustomerStats table"""
        c = self.conn.cursor()
        d = self.conn.cursor()
        
        c.execute("""SELECT DISTINCT CustomerID FROM Ratings""")
        
        for row in c:
            d.execute("""SELECT AVG(Rating)
                         FROM Ratings WHERE CustomerID=%s""" % row[0])
            nrow = d.fetchone()
            
            avg = nrow[0]
            
            ratings = self.getRatingsByCID(row[0])
            total = len(ratings)
            
            if len(ratings) <= 1:
                s = 0
            else:
                t = sum([(x-avg)**2 for x in ratings])
                s = sqrt(float(t) / (len(ratings)-1))
            
            # @portability:
            # Using REPLACE instead of INSERT - MySQL only afaik
            d.execute("""REPLACE INTO CustomerStats VALUES (%s,%s,%s,%s,0)"""
                    % (row[0], total, avg, s))
            
            self.conn.commit()
    
    def setMovieStats(self):
        """Sets the values in the MovieStats table"""
        c = self.conn.cursor()
        d = self.conn.cursor()

        c.execute("""SELECT DISTINCT MovieID FROM Ratings""")

        for row in c:
            d.execute("""SELECT AVG(Rating)
                         FROM Ratings WHERE MovieID=%s""" % row[0])
            nrow = d.fetchone()

            avg = nrow[0]

            ratings = self.getRatingsByMID(row[0])
            total = len(ratings)

            if len(ratings) <= 1:
                s = 0
            else:
                t = sum([(x-avg)**2 for x in ratings])
                s = sqrt(float(t) / (len(ratings)-1))

            # @portability:
            # Using REPLACE instead of INSERT - MySQL only afaik
            d.execute("""REPLACE INTO MovieStats VALUES (%s,%s,%s,%s,0)"""
                    % (row[0], total, avg, s))

            self.conn.commit()
        
    
    def getStatsByCID(self, CID):
        """Returns statistics about a customer from the stats table"""
        c = self.conn.cursor()
        c.execute("""SELECT Total, Average, StdDev, AvgW FROM CustomerStats
                     WHERE customerID = %d""" % (CID))
        
        t = c.fetchone()
        if t is None:
            return -1
        return t
    
    def getNumberRatingsByCID(self, CID):
        """Returns the total number of ratings made by a customer from the
           stats table"""
        c = self.conn.cursor()
        c.execute("""SELECT Total FROM CustomerStats
                     WHERE customerID = %d""" % (CID))
        
        t = c.fetchone()
        if t is None:
            return -1
        return t[0]

    def getAverageByCID(self, CID):
        """Returns the average of the ratings made by a customer from the
           stats table"""
        c = self.conn.cursor()
        c.execute("""SELECT Average FROM CustomerStats
                     WHERE customerID = %d""" % (CID))

        t = c.fetchone()
        if t is None:
            return -1
        return t[0]

    def addCustomerNeighbor(self, CID, NID, freq, sim):
        c = self.conn.cursor()
        t = (CID, NID, freq, sim)
        c.execute("""INSERT INTO CNN values (%s,%s, %s, %s)""", t)
        
        self.conn.commit()
        
    def getCNN(self, CID):
        m = []
        c = self.conn.cursor()
        
        c.execute("""SELECT neighborID, freq FROM CNN
                     WHERE customerID = %d
                     ORDER BY freq DESC""" % (CID))
        
        for row in c:
            m.append(row)
        
        return m
    
    def getCustomerSimilarity(self, CID, NID):
        c = self.conn.cursor()
        t = (CID, NID)
        
        c.execute("""SELECT sim FROM CNN
                     WHERE customerID = %d
                     AND neighborID = %d""" % t)
        
        r = c.fetchone()
        return r[0]
    
    #@update:
    def export2csv(self, filename):
        c = self.conn.cursor()
        f = open(filename, 'w')
        
        c.execute("""SELECT * FROM NETFLIX""")
        for row in c:
            mname = str(row[3]).encode('utf8')
            rdt = str(row[6])
            rdate = rdt[0:rdt.find(" ")] # removing the anoying 0s from date
            
            f.write("%d,%d,%d,%s,%d,%d,%s\n" % (row[0], row[1], row[2], \
                mname, row[4], row[5], rdate))



