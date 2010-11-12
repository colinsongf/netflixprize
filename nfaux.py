import cPickle
from operator import itemgetter

def convtuple(t):
    return {t[0]:t[1]}
    
def sortDict(d):
    """Sort a dictionary based on the value (not the key).
    It is used by Nearest to order the dictionary based on frequency.
    (2,3000) > (1,9000)
    """
    items = d.items()
    items.sort(key = itemgetter(1), reverse=True)
    return items

def normalize(prediction):
    """Rounds predictions to an integer value or to .5"""
    x = int(prediction)
    y = prediction - x

    if y > 0.7:
        result = x + 1
    elif y < 0.3:
        result = x
    else:
        result = x + 0.5

    return result

    def diskCacheIt(self, what, did, data):
        """Deprecated"""
        f = self.cacheDir+"/"+what+"."+str(did)+".pk"
        out = open(f, 'wb')
        cPickle.dump(data, out, -1)
        out.close()
    
    def diskGetCached(self, what, did):
        """Deprecated"""
        f =  self.cacheDir+"/"+what+"."+str(did)+".pk"
        try:
            inp = open(f, 'rb')
            try:
                data = cPickle.load(inp)
            finally:
                inp.close()
        except IOError:
            return -1
        
        return data
