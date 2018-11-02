import mysql_connector as con
import time
import csv

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
id = []
# query that injects ID into %s
query_base = "insert into deploy values "
query = ''
with open('Data/DeployDataClean.csv', mode='r') as infile:
    reader = csv.reader(infile)
    infile.readline()

    values = list(reader)
    buffer_size = 300
    i = 0
    for row in values:
        i = i + 1
        if i >= buffer_size:
            query = query[:-1] + ";"
            r = db.query(query, id, True)
            print('query results below:')
            print(r)
            i = 0
            id = []
            query = query_base

        # the indices of the columns whose types/formats need to be changed
        dates = [4, 5, 6]
        ints = [1]
        floats = [9, 10, 15]

        # replace null values with None and change types
        for j in range(0, len(row)):            
            if row[j].rstrip(' ') == '':
                row[j] = None
            elif j in dates:
                temp = row[j].split('/')
                year = temp[2]
                month = temp[0]
                day = temp[1]
                month.zfill(2)
                day.zfill(2)
                row[j] = year + '-' + month + '-' + day
            elif j in ints:
                row[j] = int(row[j])
            elif j in floats:
                row[j] = float(row[j])
        row.extend([None, None])
        query += '(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s),'
        id.extend(row)
    if i != 0:
        r = db.query(query, id, True)
        print('query results below:')
        print(r)

"""r = db.query("select * from user where UserID=%s;", [(id)], True)"""

# stop timing
stop = time.perf_counter()
# print the result and rounded query time
print(f'\n{r}\ncompleted query in {round(stop-start, 6)} sec')