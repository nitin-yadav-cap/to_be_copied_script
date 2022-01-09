import mysql.connector
from mysql.connector import Error

import utils
from utils import config
from utils import create_db_connection
from utils import execute_query
from utils import read_conf

import json

# config = {
#     'user': 'root',
#     'password': 'root@123',
#     'host': '127.0.0.1',
#     'database': 'nightly_mod',
#     'raise_on_warnings': True
# }
#
#
# def create_db_connection(host_name, user_name, user_password, db_name):
#     connection = None
#     try:
#         connection = mysql.connector.connect(
#             host=host_name,
#             user=user_name,
#             passwd=user_password,
#             database=db_name
#         )
#         print("MySQL Database connection successful")
#     except Error as err:
#         print(f"Error: '{err}'")
#
#     return connection
#
#
# def execute_query(connection, query):
#     cursor = connection.cursor()
#     try:
#         cursor.execute(query)
#         connection.commit()
#         print("Query successful")
#     except Error as err:
#         print(f"Error: '{err}'")
#
#
# def read_query(connection, query):
#     cursor = connection.cursor()
#     result = None
#     try:
#         cursor.execute(query)
#         result = cursor.fetchall()
#         return result
#     except Error as err:
#         print(f"Error: '{err}'")
#
#
# def read_conf(file_name):
#     with open(file_name) as f:
#         return f.readlines()


db_name = utils.db_name
dynamic_db_name = utils.dynamic_db_name
ext_db_name = utils.ext_db_name

base_data = {}

base_tables = ['attribution_strategy', 'config_key_values', 'dimension', 'dimension_attribute_value_tables',
               'dimension_table', 'fact_table', 'mongo_collections', 'mongo_structs', 'organization_scopes',
               'org_context', 'provider_tables', 'source_schema_end_point_mapping', 'summary_table']
cnx = mysql.connector.connect(**config)
cursor = cnx.cursor(buffered=True, dictionary=True)


def get_new_id_for_table(table_name, old_id):
    if old_id in base_data[table_name]:
        return base_data[table_name][old_id]
    return "VALUE_MISSING_OR_DEACTIVATED"


def load_base_data():
    lines = read_conf('/mnt/data/nitin/base_tables.txt')
    for line in lines:
        sp = line.split('#')
        table_name = db_name + '.' + sp[0].strip()
        pk_col = sp[1].strip()
        base_data[table_name] = {}
        sel_query = "SELECT * FROM {}".format(table_name)
        cursor.execute(sel_query)
        temp = {}
        for row in cursor:
            temp[str(row[pk_col])] = str(row[pk_col + '_str'])
        base_data[table_name] = temp


def load_all_ids():
    lines = read_conf('/mnt/data/nitin/all_tables.txt')
    dynamic_tables = ['flat_dimension_columns', 'hierarchical_dimension_columns', 'provider_dimension_mapping', 'scd_dimension_columns']
    for line in lines:
        #print(line)
        sp = line.split('#')
        o_name = sp[0].strip()
        table_name = db_name + '.' + sp[0].strip()
        pk_col = sp[1].strip()
        base_data[table_name] = {}
        sel_query = "SELECT * FROM {}".format(table_name)
        cursor.execute(sel_query)
        temp = {}
        for row in cursor:
            temp[str(row[pk_col])] = str(row[pk_col + '_str'])
        if o_name in dynamic_tables:
            d_sel_query = "SELECT * FROM {}".format(dynamic_db_name + '.' + o_name)
            cu = cnx.cursor(buffered=True, dictionary=True)
            cu.execute(d_sel_query)
            for r in cu:
                temp[str(r[pk_col])] = str(r[pk_col + '_str'])
        base_data[table_name] = temp


def populate_fk():
    tab_pk_map = {}
    tab_comp_pk_map = {}
    tpk_lines = read_conf('/mnt/data/nitin/all_tables.txt')
    for l in tpk_lines:
        sp = l.split('#')
        tab_pk_map[sp[0].strip()] = sp[1].strip()

    w_ids = read_conf('/mnt/data/nitin/without_id.txt')
    for w_l in w_ids:
        sp = w_l.split(':')
        comp_pks = sp[1].strip().split(',')
        tab_comp_pk_map[sp[0].strip()] = comp_pks
    lines = read_conf('/mnt/data/nitin/fk_details.txt')
    sqls = []
    for line in lines:
        #print(line)
        sp = line.split(':')
        table_name = sp[0].strip()
        sp1 = sp[1].strip().split(',')
        pk_col = ""
        comp_pk_cols = []
        if table_name in tab_pk_map:
            pk_col = tab_pk_map[table_name]
        else:
            comp_pk_cols = tab_comp_pk_map[table_name]
        sel_query = "SELECT * FROM {}".format(db_name + '.'+table_name)
        cursor.execute(sel_query)
        for row in cursor:
            for x in sp1:
                temp = x.split('#')
                col_to_set = temp[0].strip()+"_str"
                v = row[temp[0]]
                where_clause = " 1 = 1 "
                if pk_col == "":
                    for col in comp_pk_cols:
                        where_clause += " AND {} = '{}'".format(col, row[col])
                else:
                    old_pk_val = row[pk_col]
                    where_clause += "AND {} = '{}'".format(pk_col, old_pk_val)
                if v is None or (v == 0) or (v == '0'):
                    continue
                new_fk_val = get_new_id_for_table(db_name + '.'+temp[1].strip(), str(row[temp[0].strip()]))
                if new_fk_val == 'VALUE_MISSING_OR_DEACTIVATED':
                    continue
                sql_str = "UPDATE {} SET {}='{}' WHERE {}".format(db_name + '.'+table_name, col_to_set, new_fk_val, where_clause)
                #print(sql_str)
                sqls.append(sql_str)
    for s in sqls:
        utils.dump_sql(s)
        cursor.execute(s)
    cnx.commit()
    cnx.close()