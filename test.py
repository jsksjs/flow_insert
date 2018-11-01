#MYSQL v 0.1, 10/25/2014
#Timothy Becker, UCONN/SOE/CSE Phd Student
#MYSQL Connection Factory, wraps up sophisticated functionality in
#an easy to use extensible class...

import mysql_connector as con
import time

# read cfg for host and database to connect to
with open("db.cfg") as f:
    host = f.readline().rstrip('\n')
    database = f.readline().rstrip('\n')

# read cfg for credentials (username and password to DB)
    # TODO: decrypt using a key found in another file
with open("cred.cfg") as f:
    usr = f.readline().rstrip('\n')
    pwd = f.readline().rstrip('\n')
    
# connect    
db = con.MYSQL(host, database, usr, pwd)
# time query
start = time.perf_counter()

# variable to be injected
id = '1'
# query that injects ID into %s
r = db.query("select * from user where UserID=%s;", [(id)], True)

# stop timing
stop = time.perf_counter()
# print the result and rounded query time
print(f'\n{r}\ncompleted query in {round(stop-start, 6)} sec')