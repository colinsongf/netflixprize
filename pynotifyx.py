import xmpp, time
from threading import Thread
from pythonutils import ConfigObj

class XMPPNotifyThread(Thread):
    
    def __init__(self, user, passwd, tojid, msg):
        Thread.__init__(self)
        self.status = 0
        self.user = user
        self.passwd = passwd
        self.tojid = tojid
        self.msg = msg
        
    def run(self):
        jid = xmpp.protocol.JID(self.user)
        cl = xmpp.Client(jid.getDomain(),debug=[])

        con = cl.connect()
        if not con:
            self.status = 1
        
        auth = cl.auth(jid.getNode(), self.passwd, resource=jid.getResource())
        if not auth:
            self.status = 2

        cl.send(xmpp.protocol.Message(self.tojid, self.msg))
        time.sleep(10)

class XMPPNotify:
    def __init__(self, configFile):
        config = ConfigObj(configFile)
        self.user = config['user']
        self.passwd = config['password']
        
    def notify(self, tojid, msg):
        t = XMPPNotifyThread(self.user, self.passwd, tojid, msg)
        t.start()
