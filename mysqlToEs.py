import findspark
findspark.init("/usr/local/spark/")
import requests
from pprint import pprint
import pyspark
from pyspark.sql import SparkSession
from py4j.protocol import Py4JJavaError

class sqlToESTask(object):
    def __init__(self,host,database,userName,passwd,tableName,elsticHost,port,indexName):
        self.__host = host
        self.__database = database
        self.__userName = userName
        self.__passwd = passwd
        self.__tableName = tableName
        self.__elsticHost = elsticHost
        self.__port = port
        self.__indexName = indexName

    def connection(self):
        '''
        This function check ElasticSearch server start or not.
        '''
        try:
            res = requests.get('http://{0}:{1}'.format(self.__elsticHost,self.__port))
            pprint(res.content)
        except:
            print("Connection failed! Check if Elasticsearch process has been started.")
            exit(1)

    def storeDataIntoES(self):
        '''
        Fetch data from MySQL server and store into ElasticSearch
        '''
        try:
            spark = SparkSession\
                .builder\
                .appName("MySQL To ES")\
                .getOrCreate()

            dataframe_mysql = spark.read\
                .format("jdbc")\
                .option("url", "jdbc:mysql://{0}/{1}".format(self.__host,self.__database))\
                .option("driver", "com.mysql.jdbc.Driver")\
                .option("dbtable", self.__tableName).option("user", self.__userName)\
                .option("password", self.__passwd).load()

            # show the top 20 rows in dataframe
            dataframe_mysql.show()
            # print all columns data type
            print(dataframe_mysql.dtypes,"\n")
            # print schema of dataframe
            dataframe_mysql.printSchema()

            # write data into elastic search
            dataframe_mysql.write.format(
                "org.elasticsearch.spark.sql"
            ).option(
                "es.resource", self.__indexName
            ).option(
                "es.nodes", self.__elsticHost
            ).option(
                "es.port", self.__port
            ).save()

            print("Successfully ingested data into Elasticsearch!")
        except Py4JJavaError:
            print("""\n
            Something Worng!! Not concted with MySQL server.
            Please check MySQL server is runing or not. Also check the jar path correct or not.
            Also check your Credential, Database Name,Table Name.\n""")

if __name__ == "__main__":
    obj = sqlToESTask(host="localhost",database="mysql",userName="root",passwd="admin",tableName="persons",elsticHost="localhost",port=9200,indexName="persons")
    obj.connection()
    obj.storeDataIntoES()
    