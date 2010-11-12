import NFdb
import cPickle
from math import sqrt
from configobj import ConfigObj
from nfaux import convtuple, sortDict, normalize

class nn:
    
    def __init__(self, configFile, create=False):
        
        self.config = ConfigObj(configFile)
        self.debug = int(self.config['NN']['debug'])
        dbTxtFiles = self.config['DATA']['dbfiles']
        mvfile = self.config['DATA']['mvfile']
        probeFile = self.config['DATA']['probefile']
        self.cacheDir = self.config['CACHE']['dir']
        self.cacheEnabled = bool(self.config['CACHE']['enabled'])
        
        self.testSetSize = int(self.config['NN']['testsize'])
        self.maxNN = int(self.config['NN']['maxnn'])
        self.k = int(self.config['NN']['k'])
        self.dinterval = int(self.config['NN']['dinterval'])
        self.norm = bool(self.config['NN']['normalize'])
        
        self.db = NFdb.NFdb(self.config['DATA']['dbconf'])
        

        if create is True:
            db.create()
            db.loadData(dbTxtFiles)
            db.loadMovieTitles(mvfile)
            db.createProbeTable()
            db.loadProbeData(probeFile)
        
    
         
    def dbCacheCNN(self, cid, nList, sList):
        """Cache a nearest neighbor list."""
        
        for ii in range(0, len(nList)):
            (nid, freq) = nList[ii]
            sim = sList[ii]
            self.db.addCustomerNeighbor(cid, nid, freq, sim)
        
    def dbGetCachedCNN(self, cid):
        return self.db.getCNN(cid)
    
    
    def CustomerSimilarity(self, cid, nid):
        """Determines the Similarity (Pearson's correlation factor)
           between two customers"""
        
        c = self.db.getRatedMovieIDsByCID(cid)
        n = self.db.getRatedMovieIDsByCID(nid)
        
        # movies both rated
        both = [x for x in c if x in n]
        length = len(both)
        if length < 2:
            return 0
        
        # get statistics
        (ctotal, cavg, cdev, cwavg) = self.db.getStatsByCID(cid)
        (ntotal, navg, ndev, nwavg) = self.db.getStatsByCID(nid)
        
        if cdev == 0 or ndev == 0:
            return 0
        
        temp = []
        for mid in both:
            crating = self.db.getRating(cid, mid)
            nrating = self.db.getRating(nid, mid)
            
            cval = float(crating - cavg) / cdev
            nval = float(nrating - navg) / ndev
            temp.append(cval*nval)
            
        sim = sum(temp)/(length-1)
        
        return sim
        
     
    def customerNearest(self, cid, excludeList=[]):
        """Creates a dictionary with the nearest neighboors for a given CID.
        Nearest neighbors are determined by the number of equal ratings.
        """
        # check if it has been cached
        data = self.dbGetCachedCNN(cid)
        if data != []:
            if self.debug > 4:
                print "Got it from cache"
                
            return data
            
        nn = dict()
        
        ratedList = self.db.getRatedMovies(cid)
        
        if self.debug > 3:
            num = len(ratedList)
            print "Rated %d Movies" % (num)
            count = 0
        
        for (mid, rating, rdate) in ratedList:
            if mid not in excludeList:
                # get all CIDs that rated the same movie
                new = self.db.getCIDsbyMovieIDRating(mid, rating)
                
                if self.debug > 3:
                    print "Count = %d / %d" % (count, num)
                    count += 1
                    numMatches = len(new)
                    print "Found %d Matches" % (numMatches)
            
                for x in new:
                    # ignore own id
                    if x[0] != cid:
                        # add to the dictionary 
                        if x[0] in nn:
                            nn[x[0]] = nn[x[0]] + 1
                        else:
                            nn[x[0]] = 1
                        
        
        # Order it (closest neighboors first)
        tempList = sortDict(nn)
        
        # limit to self.maxNN
        nList = []
        nList.extend(tempList[:self.maxNN])
        
        # calculate similarity for each neighbor
        sList = []
        for (nid, f) in nList:
            sim = self.CustomerSimilarity(cid, nid)
            sList.append(sim)
        
        # cache it
        if self.cacheEnabled is True:
            self.dbCacheCNN(cid, nList, sList)
        
        return nList

    #@TODO:
    def movieNearest(self, mid, excludeList=[]):
        """Creates a dictionary with the nearest neighboors for a given MID.
        Nearest neighbors are determined by the number of equal ratings.
        """
        # check if it has been cached
        data = self.dbGetCachedMNN(mid)
        if data != []:
            if self.debug > 4:
                print "Got it from cache"

            return data

        nn = dict()

        ratedList = self.db.getRatedCustomers(mid)

        if self.debug > 3:
            num = len(ratedList)
            print "Rated %d Movies" % (num)
            count = 0

        for (cid, rating, rdate) in ratedList:
            if cid not in excludeList:
                # get all MIDs that were rated by the same customer
                new = self.db.getMIDsbyCustomerIDRating(mid, rating)

                if self.debug > 3:
                    print "Count = %d / %d" % (count, num)
                    count += 1
                    numMatches = len(new)
                    print "Found %d Matches" % (numMatches)

                for x in new:
                    # ignore own id
                    if x[0] != cid:
                        # add to the dictionary 
                        if x[0] in nn:
                            nn[x[0]] = nn[x[0]] + 1
                        else:
                            nn[x[0]] = 1


        # Order it (closest neighboors first)
        tempList = sortDict(nn)

        # limit to self.maxNN
        nList = []
        nList.extend(tempList[:self.maxNN])

        # calculate similarity for each neighbor
        sList = []
        for (nid, f) in nList:
            sim = self.CustomerSimilarity(cid, nid)
            sList.append(sim)

        # cache it
        if self.cacheEnabled is True:
            self.dbCacheCNN(cid, nList, sList)

        return nList  
    
    
    def dateDistance(self, cid, date, nid, days=150):
        
        # get the ratings
        cRatings = self.db.getRatedMoviesByDateInterval(cid, date-days,
                                                        date+days)
        nRatings = self.db.getRatedMoviesByDateInterval(nid, date-days,
                                                        date+days)
        
        # convert the list of tuples into dictionarys
        nRatingsD = dict()
        filter(nRatingsD.update, map(convtuple, nRatings))
        
        n = 0
        total = 0
        distance = 0
        for (mid, rating) in cRatings:
            if mid in nRatingsD:
                distance += (rating - nRatingsD[mid])**2
                n += 1
                
            t1 += 1
        
        return sqrt(float(distance)/n)
    
    def dateDistances(self, cid, date, nid, movieID, days=150):
        """Compute average rating distance between a customer and his
        nearest neighbor on the dates each rated a given movieID.
        """
        
        # First determine proximity at the time the neighbor made the rating
        nDate = self.db.getRatingDate(nid, movieID)
        d1 = dateDistance(self, cid, nDate, nid, days)
        
        # Next the proximity at the time the prediction is being made
        d2 = dateDistance(self, cid, date, nid, days)
        
        return (d1, d2)
    
    def predictCustomerKNN(self, cid, mid, nList, k=30):
        """k-nearest neighbor based prediction using customer neighbors.
        """
        prediction = 0.0
        count = 0
        s = 0
        d = 0
        
        # customer stats
        stat = self.db.getStatsByCID(cid)
        (ctotal, cavg, cdev, cwavg) = stat
        
        
        # base predictor
        cbp = cavg
        
        
        for (nid, freq) in nList:
            # check if used k neighbors already
            if count > k:
                break
            
            rating = self.db.getRating(nid, mid)
            if rating > 0:
                # neighbor base predictor = average rating
                nbp = self.db.getAverageByCID(nid)
                sim = self.db.getCustomerSimilarity(cid, nid)
                
                s += sim * (rating - nbp)
                d += sim
                count += 1
        
        #@todo:
        # if count < k use another method? e.g. return -1?
                
        if d != 0:
            prediction = cbp + s/d
            
            if self.norm is True:
                return normalize(prediction)
            else:
                return prediction
        
        return -1
    
    
    def getTestSet(self):
        f =  self.cacheDir+"/"+"testset.pk"
        try:
            inp = open(f, 'rb')
            try:
                data = cPickle.load(inp)
                testSet = data[:self.testSetSize]
                if len(testSet) < self.testSetSize:
                    raise IOError
            finally:
                inp.close()
        except IOError:
            data = self.db.getCIDsMovieIDsfromProbe()
            data.sort()
            testSet = data[:self.testSetSize]
            out = open(f, 'wb')
            cPickle.dump(data, out, -1)
            out.close()
        
        return testSet
            
            
        
    def probeCustomerKNN(self):
        """Makes predictons for the probe values using KNN for customers"""
        result = []
        count = 0
        scid = 0
        probeList = self.getTestSet()
        
        for (cid, movieID) in probeList:
            count += 1
            if self.debug > 1:
                print "Customer KNN-Predicting for %d, %d (n=%d)" % (cid,
                movieID, count)
   
            
            if scid != cid:
                exclude = [movieID]    
                nn = self.customerNearest(cid, exclude)
                scid = cid
                
            prediction = self.predictCustomerKNN(cid, movieID, nn, self.k)
            actual = self.db.getRating(cid, movieID)
            nRatings = self.db.getNumberRatingsByCID(cid)
            t = (cid, movieID, nRatings, prediction, actual)
            
            if self.debug > 0:
                print "KNN-Result: ", t
                
            if prediction > 0:
                result.append(t)
        
        return result

                    
    
    def rmse(self, data):
        n = len(data)
        delta = [(x[0] - x[1])**2 for x in data]
        return sqrt(float(sum(delta))/n)
    
    
    def rmseProbeCustomerKNN(self):
        probe = self.probeCustomerKNN()
        z = zip(*probe)
        data = map(None, z[4], z[3])
        return self.rmse(data)
    
    
    #@update:
    def statistics(self, data):
        dist = dict()
        
        if self.debug > 1:
                count = 0
                total = len(data)
                print "Generating Statistics (%d entries)" % (total)
        
        #data = [(cid, movieID, prediction, actual)]
        for x in data:
            if self.debug > 1:
                if count % 100 == 0:
                    print "%d / %d" % (count, total)
                count += 1
            nRatings = self.getCachedTotalRatings(x[0])
            if nRatings is None:
                print "ERROR: No ratings for CID %d" % (x[0])
                exit(-1)
            if nRatings in dist:
                add = dist[nRatings]
                add.append(x)
            else:
                l = list()
                l.append(x)
                dist[nRatings] = l
        
        keys = dist.keys()
        keys.sort()

        stats = []
        for k in keys:
            d = dist[k]
            z = zip(*d)
            distData = map(None, z[4], z[3])
            r = solve.rmse(distData)
            stats.append((k,r))
        
        return stats
    
    
    def writeStats(self, filename, stats):
        if self.debug > 3:
            print "Writing Statistics to file (%s)" % (filename)
        
        try:
            out = open(filename, 'w')
            for (k, r) in stats:
                out.write("%d\t%f\n" % (k, r))
            out.close()
            return 1
        except IOError:
            return -1
