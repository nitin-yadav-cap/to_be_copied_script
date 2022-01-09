import mysql.connector
from mysql.connector import Error
import re

config = {
    'user': 'workflowmanager-runner-IOho',
    'password': '9L3fEMRMX!wC',
    'host': 'reon-metadata-db-mysql-master',
    'database': 'reon_metadata_2021_10_05_02_54',
    'raise_on_warnings': True
}


# db_name = "nightly_mod"
# dynamic_db_name = "nightly_dynamic"
# ext_db_name = "nightly_external"
# dim_grouping = "dimension_grouping"
# data_location_db = "nightly_data_loc"


db_name = "reon_metadata"
dynamic_db_name = "reon_metadata_dynamic"
#db_name = "reon_metadata"
#dynamic_db_name = "reon_metadata_dynamic"
ext_db_name = "reon_metadata_external"
dim_grouping = "dimension_grouping"
data_location_db = "reon_data_location_info"

def dump_sql(stmt):
    print(stmt)
    file = open("id_mgr.sql", "a")
    file.write(stmt+";\n")
    file.close()


def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        #print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection


def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")


def read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as err:
        print(f"Error: '{err}'")
def read_conf(file_name):
    with open(file_name) as f:
        return f.readlines()
def find_between(s, first, last):
    try:
        start = s.index(first) + len(first)
        end = s.index(last, start)
        return s[start:end]
    except ValueError:
        return ""
def find_all_between(s, what_to):
    regex = ''
    if what_to == 'tableId':
        regex = r'"tableId":(.*?),'
    elif what_to == 'selectCols':
        regex = r'"selectCols":\[(.*?)\]'
    elif what_to == 'fk':
        regex = r'"fk":(.*?),'
    elif what_to == 'colId':
        regex = r'"colId":(.*?),'
    elif what_to == 'groupByCol':
        regex = r'"groupByCol":(.*?),'
    elif what_to == 'valueCol':
        regex = r'"valueCol":(.*?),'
    elif what_to == 'columnId':
        regex = r'"columnId":(.*?),'
    elif what_to == 'values':
        regex = r'"values":"(.*?)"'
    try:
        return re.findall(regex, s)
    except ValueError:
        return ""
#print(find_all_between(test_str, 'tableId'))