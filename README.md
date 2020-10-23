# MySQL-To-NoSQL
Fetch data from MySQL and push it in Cassandra using spark in local mode and vice-versa. Using pyspark to featch data from MySQL and store into Elasticsearch.

## MySQL to Elasticsearch

### How to start:-
1. Install [MySQL](https://www.sqlshack.com/how-to-install-mysql-on-ubuntu-18-04/).

2. Install [elasticsearch](https://phoenixnap.com/kb/install-elasticsearch-ubuntu) in local system and check port is running or not.
3. Install [spark](https://phoenixnap.com/kb/install-spark-on-ubuntu) in local system.

4. Clone the Repo.

    ```
    git clone https://github.com/shivamgupta7/MySQL-To-NoSQL.git 
    ```

5. Go into the file  and install all requirements.
    
    ```
    cd MySQL-To-NoSQL
    ```
    ```
    pip install -r requirements.txt
    ```
6. Edit ```mysqlToEs.py``` to give your host,database,userName,passwd,tableName,elsticHost,port,indexName.

7. Run ```mysqlToEs.py``` file
    ```
    python mysqlToEs.py
    ```

## MySQL to Cassandra and Vice-Versa.

### How to start:-
1. Install [MySQL](https://www.sqlshack.com/how-to-install-mysql-on-ubuntu-18-04/).

2. Install [Cassandra](https://phoenixnap.com/kb/install-cassandra-on-ubuntu)

3. Install [spark](https://phoenixnap.com/kb/install-spark-on-ubuntu) in local system.

4. Clone the Repo.

    ```
    git clone https://github.com/shivamgupta7/MySQL-To-NoSQL.git 
    ```

5. Go into the file  and install all requirements.
    
    ```
    cd MySQL-To-NoSQL
    ```
    ```
    pip install -r requirements.txt
    ```
6. Edit ```mysqlCassandr.py``` to give your host,database,userName,passwd,tableName,keyspaceName,tableName.

7. Run ```mysqlCassandr.py``` file
    ```
    python mysqlCassandra.py
    ```