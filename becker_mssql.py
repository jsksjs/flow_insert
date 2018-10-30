#MSSQL v 0.1, 10/07/2013-10/17/2013
#Timothy Becker, UCONN/SOE/CSE/Graduate Research Assistant
#MSSQL Connection Factory, wraps up sophisticated functionality in
#an easy to use extensible class...

import os
import re
import sys
import getpass
import pyodbc

class MSSQL:
    
    #constructor
    def __init__(self,drv,srv,db,uid=False,pwd=False):
        #The MSSQL variables for injection safe connection strings
        self.drv = drv           #which driver to use(several come with pyodbc)
        self.srv = srv           #SQL server hostname to connect to
        self.db = db             #db name
        self.uid = uid
        self.pwd = pwd
        self.conn = self.start() #keeps the connection object
        self.SQL =  []           #list of Qs
        self.V   =  []           #list of values for SQL
        self.errors = ''
        
    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        with open(os.path.dirname(os.path.abspath(__file__))+'\\'+'errors.txt', 'a') as f:
            f.write(self.errors)
        #close up the connection
        try: self.conn.close()
        except RuntimeError:
            print('ER5.ODBC')
            self.errors += 'ER5.ODBC' + '\n'
            
    def start(self):
        conn = None
        if (not self.uid) and (not self.pwd):
            print('uid: '),
            self.uid = sys.stdin.readline().replace('\n','')                   
            self.pwd = getpass.getpass(prompt='pwd: ',stream=None)#was stream=sys.sdin
        try:#connection start
            if re.search('^DNS=',self.drv) is not None:
                #use a FreeTDS DNS registered connection
                self.drv = self.drv.replace('DNS=','')
                conn_string = "DNS=%s;UID=%s;PWD=%s" %(self.drv,self.uid,self.pwd)
                conn = pyodbc.connect(conn_string)
            else:
                conn_string = "DRIVER=%s;SERVER=%s;DATABASE=%s;UID=%s;PWD=%s"
                conn = pyodbc.connect(conn_string %(self.drv,self.srv,self.db,self.uid,self.pwd))
        except RuntimeError:
            print('ER1.ODBC')
            self.errors += 'ER1.ODBC' + '\n'
        return conn
        
    def query(self,sql,v,r):
        text = []
        try:  #execute one sql and v list
            if r:
                for row in self.conn.cursor().execute(sql,v): text.append(row)
            else:
                self.conn.cursor().execute(sql,v)
            self.conn.commit()
        except pyodbc.ProgrammingError:
            print('ER2.SQL_Malformed')
            self.errors += 'ER2.SQL_Malformed' + '\n'
        except pyodbc.DataError:
            print('ER3.GTFS_Insert_Not_Matching_Template')
            self.errors += 'ER3.GTFS_Insert_Not_Matching_Template' + '\n'
            print('SQL code:\n'+sql+'\nValue List:\n')
            self.errors += ('SQL code:\n'+sql+'\nValue List:\n')
            print(v)
            self.errors += str(v) + '\n'
        except pyodbc.IntegrityError:
            print('ER4.SQL_Constraint_Violation')
            self.errors += 'ER4.SQL_Constraint_Violation' + '\n'
            print('SQL code:\n'+sql+'\nValue List:\n')
            self.errors += ('SQL code:\n'+sql+'\nValue List:\n')
            print(v)
            self.errors += str(v) + '\n'
        return text
        



        
    

