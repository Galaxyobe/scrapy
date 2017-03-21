from twisted.enterprise import adbapi
from twisted.internet import reactor, task


class MysqlPing:
    def __init__(self, db_pointer, interval):
        self.dbpool = db_pointer
        self.dbping = task.LoopingCall(self.dbping)
        # 20 minutes = 1200 seconds; i found out that if MySQL socket is idled
        # for 20 minutes or longer, MySQL itself disconnects the session for
        # security reasons; i do believe you can change that in the
        # configuration of the database server itself but it may not be
        # recommended.
        self.interval = interval
        self.dbping.start(interval)
        self.reconnect = False

    def dbping(self):
        def ping(conn):
            # what happens here is that twisted allows us to access methods
            # from the MySQLdb module that python posesses; i chose to use the
            # native command instead of sending null commands to the database.
            conn.ping()

        pingdb = self.dbpool.runWithConnection(ping)
        pingdb.addCallback(self.dbactive)
        pingdb.addErrback(self.dbout)

    def dbactive(self, data):
        if data is None and self.reconnect:
            self.dbping.stop()
            self.reconnect = False
            self.dbping.start(self.interval)  # 20 minutes = 1200 seconds
            # print "Reconnected to database!"
        elif data is None:
            pass
            # print "database is active"

    def dbout(self, deferr):
        # print deferr
        if self.reconnect is False:
            self.dbreconnect()
        elif self.reconnect:
            pass
            # print "Unable to reconnect to database"
        # print "unable to ping MySQL database!"

    def dbreconnect(self, *data):
        self.dbping.stop()
        self.reconnect = True
        # self.dbping = task.LoopingCall(self.dbping)
        self.dbping.start(self.interval / 2)  # 60


if __name__ == "__main__":
    db = MysqlPing(
        adbapi.ConnectionPool(
            "MySQLdb", cp_reconnect=True, host="127.0.0.1", user="",
            passwd=""), 2)
    reactor.callLater(2, db.dbping)
    reactor.run()
