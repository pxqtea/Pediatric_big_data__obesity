### test dates
#!/usr/bin/python
import psycopg2
from mailbox import linesep
import csv
import pandas as pd
from IPython.display import display
from configparser import ConfigParser
import psycopg2
from mailbox import linesep
from configparser import ConfigParser
import sklearn.model_selection # import ParameterGrid
import getpass
import matplotlib.pylab as plt

from astropy.visualization import astropy_mpl_style
from astropy.visualization import hist
plt.style.use(astropy_mpl_style)
from astropy.stats import histogram
from collections import Counter

class SQLConnection2(): 
    def config(self, filename='pdb_conect_local.ini', section='postgresql'):
        # create a parser
        parser = ConfigParser()
        # read config file
        parser.read(filename)

        #print("read parameter file")
        # get section, default to postgresql
        db = {}
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                db[param[0]] = param[1]
            #print("Please input your passwords for ", db['database'])
                #db['password'] = str(getpass.getpass())


            #print("Read parameters to connect to DB!")
        else:
            raise Exception('Section {0} not found in the {1} file'.format(section, filename))
            print("Cannot connect")
        return db

    def connect(self, fname='pdb_conect_local.ini'):
        conn = None
        try:    
            print('Connecting to the PBD database...')
            params = self.config(fname)
            # connect to the PostgreSQL server
            conn = psycopg2.connect(**params)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        return conn
    
    def close(self, conn):
        if conn is not None:
            conn.close()
            print(conn, 'Database connection closed.')

    def create_table2(self, sql1, sql2, sql3, concept_id, tname, conn_remote, conn_local):
        
        cur_remote = conn_remote.cursor()
        cur_local = conn_local.cursor()
        print("Pull", tname , "data from PBD_v24 measurement table")
        cur_remote.execute(sql1,(concept_id,) ) 
        outputs = cur_remote.fetchall()
        print("The total # of distinct ", tname, " measurement is:  ", cur_remote.rowcount, "\n")
        cur_local.execute(sql2 )
        print("Incerting data to ", tname, " table")
        cur_local.executemany(sql3, outputs) 

        conn_local.commit()  
        cur_local.close()
        cur_remote.close()
    
    
    def show_data(self, sqlquery, conn):
        """ Connect to the PostgreSQL database server """


        cur = conn.cursor()
        cur.execute(sqlquery) ##measurement_concept_id, measurement_date, measurement_result_date
        colnames = [desc[0] for desc in cur.description]
        outputs = cur.fetchall()
        print("The total # of measurements is:  ", cur.rowcount, "\n")
        ## read data to pandas data frame 
        sample_df = pd.DataFrame.from_records(outputs, columns = colnames)
        display(sample_df)
        cur.close()
        #except (Exception, fe_sendauth: no password supplied) as 


    def show_write_data(self, sqlquery, queryname, conn):
        """ Connect to the PostgreSQL database server """

        cur = conn.cursor()
        cur.execute(sqlquery) ##measurement_concept_id, measurement_date, measurement_result_date
        colnames = [desc[0] for desc in cur.description]
        outputs = cur.fetchall()

        print("The total # of measurements is:  ", cur.rowcount, "\n")

        ## read data to pandas data frame 
        sample_df = pd.DataFrame.from_records(outputs, columns = colnames)
        display(sample_df)
            
        with open(queryname + '.csv','w') as out:
            csv_out=csv.writer(out)
            csv_out.writerow(colnames) #['person_id', 'time_of_birth', 'year_of_birth', 'month_of_birth', 'day_of_birth', 'measurement_date', 'measurement_age_in_months'])
            for row in outputs:
                csv_out.writerow(row) 
        cur.close()


    def screen_data(self, sqlquery, queryname, gender, mtype, conn):
        """ Connect to the PostgreSQL database server """
        cur = conn.cursor()
        cur.execute(sqlquery, (gender, mtype, )) ##measurement_concept_id, measurement_date, measurement_result_date
        colnames = [desc[0] for desc in cur.description]
        outputs = cur.fetchall()
            
        print("The total # of measurements is:  ", cur.rowcount, "\n")
        ## read data to pandas data frame 
        sample_df = pd.DataFrame.from_records(outputs, columns = colnames)
        display(sample_df)

        with open(queryname + '_'+ str(gender) + '_' + str(mtype) + '.csv','w') as out:
            csv_out=csv.writer(out)
            csv_out.writerow(colnames) #['person_id', 'time_of_birth', 'year_of_birth', 'month_of_birth', 'day_of_birth', 'measurement_date', 'measurement_age_in_months'])
            for row in outputs:
                csv_out.writerow(row) 

        cur.close()

                
    def create_data(self, sqlquery, conn):
        """ Connect to the PostgreSQL database server """
        cur = conn.cursor()
        cur.execute(sqlquery) ##measurement_concept_id, measurement_date, measurement_result_date
        conn.commit()
        cur.close()
        



