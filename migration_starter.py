import mysql.connector

import custom_logic
import data_location_migration
import dynamic_meta
import external_meta_migration
import utils
from base_data import load_all_ids
from base_data import load_base_data
from base_data import populate_fk
from utils import config
from utils import read_conf
from utils import dump_sql


db_name = utils.db_name


def update_ids(table_name, columns, pk_col):
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor(buffered=True, dictionary=True)
    f_tab_name = db_name + '.' + table_name
    sel_tab = "SELECT * FROM {}".format(f_tab_name)
    cursor.execute(sel_tab)
    sqls = []
    for row in cursor:
        # print(row)
        id_str = ''
        for c in columns:
            id_str = id_str + str(row[c]) + '_'
        id_str = id_str[:-1]
        # id_str = row['fact_column_link'] + '_' + str(row['scope_id'])
        pk_val = row[pk_col]
        sql_str = "UPDATE {} SET {}='{}' WHERE {}={}".format(f_tab_name, pk_col + '_str', id_str, pk_col, pk_val)
        #print(sql_str)
        sqls.append(sql_str)

    for s in sqls:
        dump_sql(s)
        cursor.execute(s)

    cnx.commit()
    cnx.close()


# get_new_id_for_table('nightly_mod.dimension', '1')
lines = read_conf('/mnt/data/nitin/test_conf.txt')
for line in lines:
    sp = line.split(':')
    table_name = sp[0].strip()
    columns_and_pk = sp[1].strip().split('#')
    columns = columns_and_pk[0].strip().split(',')
    pk = columns_and_pk[1].strip()
    for s in columns:
        s
    #print(line)
    #update_ids(table_name, columns, pk)

load_base_data()
load_all_ids()
#populate_fk()

dynamic_meta.update_id_str()
dynamic_meta.populate_fk()
dynamic_meta.handle_provider_dim_map()
#dynamic_meta.handle_transformation()

#custom_logic.handle_dimension_table()
#custom_logic.handle_fact_table()
#custom_logic.handle_fact_table_column()
#custom_logic.handle_fact_table_run_type()
#custom_logic.handle_fact_table_std_cond()
#custom_logic.handle_logical_summary_table()
#custom_logic.handle_mongo_collections()
#custom_logic.handle_provider_dim_map()
#custom_logic.handle_source_table()
#custom_logic.handle_source_target_dim_col_map()
#custom_logic.handle_source_target_table_map()
#custom_logic.handle_summary_condition()
#custom_logic.handle_summary_group_by()
#external_meta_migration.populate_fk()
#external_meta_migration.handle_dimension_groups()
#external_meta_migration.handle_ext_flat_dim_col()
#external_meta_migration.handle_pii_masked_col()
#external_meta_migration.handle_custom_table_fk()
#external_meta_migration.handle_banding()
#data_location_migration.handle_dim_grouping_run_details()
#data_location_migration.handle_dl_fact()
#data_location_migration.handle_dl_dimension()
#data_location_migration.handle_dl_source()
#data_location_migration.handle_dl_scd_dl()
#data_location_migration.handle_dl_dimension_attr()
#data_location_migration.handle_dl_summary_fact()
#custom_logic.handle_transformation()
#custom_logic.handle_transpose_columns()
# cnx = mysql.connector.connect(**config)
# cursor = cnx.cursor(buffered=True, dictionary=True)
# cursor.execute('SELECT * FROM nightly_mod.dimension')
# sqls = []
# for row in cursor:
#     # print(row)
#     # id_str = ''
#     id_str = row['fact_column_link'] + '_' + str(row['scope_id'])
#     id = row['dim_id']
#     sql_str = "UPDATE {} SET {}='{}' WHERE {}='{}'".format('nightly_mod.dimension', 'dim_id_str', id_str, 'dim_id', id)
#     print(sql_str)
#     sqls.append(sql_str)
#
# for s in sqls:
#     cursor.execute(s)
#
# cnx.commit()
# cnx.close()