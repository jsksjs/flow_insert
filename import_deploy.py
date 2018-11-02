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
id =[]
# query that injects ID into %s
query_base = "INSERT INTO flowlabel (\'Flag\', \'CameraNumber\', \'SiteID\', \'USDS\', \'DeployStart\', \'DeployEnd\', \'BatteryBrand\', \'BatteryType\', \'StartBatteryAmp\', \'EndBatteryAmp\', \'Infostrip\', \'MotionDetect\', \'QualitySettings\', \'TimeLapseSettings\', \'MemoryUsed\', \'USGSGage\', \'Comments\') VALUES "
query = ''
with open('C:\\Users\\Nick\\Desktop\\Working\\Flow_Insert\\Data\\DeployDataClean.csv', mode='r') as infile:
	reader = csv.reader(infile)
	infile.readline()
	
	values = list(reader)
	
	buffer_size = 300
	i = 0
	for row in values:
		i = i + 1
		if i == buffer_size:
			
			query = query + ";"
			r = db.query(query, id, True)
			print('query results below:')
			print(r)
			i = 0
			id = []
			query = query_base
		#replace null values with NULL
		for j in range(0, len(row)):
			if row[j] == '': 
				row[j] = 'NULL'
		
		#the indices of the columns whose dates need to be changed
		indices = [4,5,6] 
		#make date values be in the right format
		for i in indices:
			if row[i] != 'NULL':
				temp = row[i].split('/')
				year = temp[2]
				month = temp[0]
				day = temp[1]
				if len(month) < 2:
					month = '0' + temp[0]
				if len(day) < 2:
					day = '0' + temp[1]
				row[i] = year + '-' + month + '-' + day

		print(row)
		
		query = query + '(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
		temp = tuple(row)
		id.append(temp)

	if i != 0:
		r = db.query(query, id, True)
		print('query results below:')
		print(r)

"""r = db.query("select * from user where UserID=%s;", [(id)], True)"""

# stop timing
stop = time.perf_counter()
# print the result and rounded query time
print(f'\n{r}\ncompleted query in {round(stop-start, 6)} sec')