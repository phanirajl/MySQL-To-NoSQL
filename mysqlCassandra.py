import findspark
findspark.init("/usr/local/spark/")
import pyspark
from pyspark.sql import SQLContext, SparkSession
from py4j.protocol import Py4JJavaError

class mysqlToCassandraTask(object):
    def __init__(self,host,database,userName,passwd,tableName):
        self.__host = host
        self.__database = database
        self.__userName = userName
        self.__passwd = passwd
        self.__tableName = tableName

    def storeDataIntoCassandra(self, keyspaceName, tableName):
        '''
        This funtion fetch data from MySQL and store into Cassandra Table.
        '''
        try:
            spark = SparkSession\
                .builder\
                .appName("MySQL To Cassandra")\
                .getOrCreate()

            # Read data from MySQL as dataframe
            dataframe_mysql = spark.read\
                .format("jdbc")\
                .option("url", "jdbc:mysql://{0}/{1}".format(self.__host,self.__database))\
                .option("driver", "com.mysql.jdbc.Driver")\
                .option("dbtable", self.__tableName).option("user", self.__userName)\
                .option("password", self.__passwd).load()
        

            # dataframe_mysql.show()
            # # print all columns data type
            # print(dataframe_mysql.dtypes,"\n")
            # # print schema of dataframe
            # dataframe_mysql.printSchema()

            # Store MySQL dataframe to Cassandra
            dataframe_mysql.write\
                .format("org.apache.spark.sql.cassandra")\
                .mode('append')\
                .options(table=tableName, keyspace=keyspaceName)\
                .save()
            
            print('''\n
            Task: Fetch data from MySQL and Store into Cassandra
            Successfully completed!!.
            \n''')
        
        except Py4JJavaError:
            print("""\n
            Something Worng!! Not concted with MySQL server.
            Please check MySQL server is runing or not. Also check the jar path correct or not.
            Also check your Credential, Database Name,Table Name.\n""")

class cassandraToMySQLTask(object):
    def __init__(self,host,database,userName,passwd,tableName):
        self.__host = host
        self.__database = database
        self.__userName = userName
        self.__passwd = passwd
        self.__tableName = tableName

    def storeDataIntoMySQL(self, keyspaceName,tableName):
        '''
        This funtion fetch data from Cassandra and store into MySQL Table.
        '''
        try:
            spark = SparkSession\
                .builder\
                .appName("Cassandra To MySQL")\
                .getOrCreate()

            dataframeCassandra = spark.read\
                .format("org.apache.spark.sql.cassandra")\
                .load(keyspace=keyspaceName, table=tableName)

            # dataframeCassandra.show()

            dataframeCassandra.write\
                .format("jdbc")\
                .option("url", "jdbc:mysql://{0}/{1}".format(self.__host,self.__database))\
                .option("driver", "com.mysql.jdbc.Driver")\
                .option("dbtable", self.__tableName).option("user", self.__userName)\
                .option("password", self.__passwd).mode('overwrite').save()
        
            print('''\n
            Task: Fetch data from Cassandra and store into MySQL.
            Successfully completed!!.
            \n''')

        except Py4JJavaError:
            print("""\n
            Something Worng!! Not concted with MySQL server.
            Please check MySQL server is runing or not. Also check the jar path correct or not.
            Also check your Credential, Database Name,Table Name.\n""")

def switchToFunction(objCassandra,objMySQL,keyspaceName,tableName):
    '''
    Create switch function to move perticular program
    '''
    print('''
    1. Fetch data from MySQL and Store into Cassandra.
    2. Fetch data from Cassandra and store into MySQL.
    ''')
    try:
        choice = int(input('Enter which program you want to run: '))
        switcher = {
            1 : lambda: objCassandra.storeDataIntoCassandra(keyspaceName="test",tableName="user"),
            2 : lambda: objMySQL.storeDataIntoMySQL(keyspaceName="test",tableName="user")
        }
        func = switcher.get(choice, lambda: print('\nInvalid choice please select correct options.'))
        func()
    except Exception as e:
        print("\n",e)


def main():
    keyspaceName = "test"
    tableName = "user"
    objCassandra = mysqlToCassandraTask(host="localhost",database="mysql",userName="root",passwd="admin",tableName="persons")
    objMySQL = cassandraToMySQLTask(host="localhost",database="mysql",userName="root",passwd="admin",tableName="persons")
    switchToFunction(objCassandra,objMySQL,keyspaceName,tableName)
    options = input('\nDo you want to continue?[y/n]: ')
    if options.lower() == 'y':
        main()
    else:
        exit()

if __name__ == "__main__":
    main()