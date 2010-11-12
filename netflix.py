import NFdb
from nn import nn
from pynotifyx import XMPPNotify
import hotshot, hotshot.stats

def test():  
    db = NFdb.NFdb("config/db.conf")
    n = XMPPNotify("config/xmpp.conf")
    contact = "luis.rei@gmail.com"

    solve = nn("config/nn.conf")

    res = solve.rmseProbeCustomerKNN()
    msg = "RMSE for Customer KNN = %f" % (res)
    print msg
    n.notify(contact, msg)

# prof = hotshot.Profile("netflix.prof")
# prof.runcall(test)
# prof.close()
# stats = hotshot.stats.load("netflix.prof")
# stats.strip_dirs()
# stats.sort_stats('time', 'calls')
# stats.print_stats(20)

test()