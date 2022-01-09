import mysql.connector

import utils
from base_data import get_new_id_for_table
from utils import config
from utils import read_conf
from utils import find_between

cnx = mysql.connector.connect(**config)
cursor = cnx.cursor(buffered=True, dictionary=True)

db_name = utils.db_name
dynamic_db_name = utils.dynamic_db_name
ext_db_name = utils.ext_db_name
dim_grp_db = utils.dim_grouping
data_loc_db = utils.data_location_db


def handle_dim_grouping_run_details():
    table_name = dim_grp_db + '.' + "dimension_grouping_run_details"
    cols = ['dimension_id_str']
    sel_query = "SELECT * FROM {}".format(table_name)
    cursor.execute(sel_query)
    sqls = []
    for row in cursor:
        where_clause = "{} = '{}'".format('id', row['id'])
        for c in cols:
            if 'dimension_id_str' == c:
                col_value = row['dimension_id']
                new_val_str = get_new_id_for_table(db_name + '.dimension_table', str(col_value))
                sql_str = "UPDATE {} SET {}='{}' WHERE {}".format(table_name, c, new_val_str, where_clause)
                # print(sql_str)
                sqls.append(sql_str)
    for s in sqls:
        utils.dump_sql(s)
        cursor.execute(s)
    cnx.commit()


def handle_dl_dimension():
    table_name = data_loc_db + '.' + "data_location_dimension"
    cols = ['table_id_str']
    sel_query = "SELECT * FROM {}".format(table_name)
    cursor.execute(sel_query)
    sqls = []
    for row in cursor:
        where_clause = "{} = '{}'".format('id', row['id'])
        for c in cols:
            if 'table_id_str' == c:
                col_value = row['table_id']
                new_val_str = get_new_id_for_table(db_name + '.dimension_table', str(col_value))
                sql_str = "UPDATE {} SET {}='{}' WHERE {}".format(table_name, c, new_val_str, where_clause)
                # print(sql_str)
                sqls.append(sql_str)
    for s in sqls:
        utils.dump_sql(s)
        cursor.execute(s)
    cnx.commit()


def handle_dl_dimension_attr():
    table_name = data_loc_db + '.' + "data_location_dimension_attr"
    cols = ['table_id_str', 'column_id_str']
    sel_query = "SELECT * FROM {}".format(table_name)
    cursor.execute(sel_query)
    sqls = []
    for row in cursor:
        where_clause = "{} = '{}'".format('id', row['id'])
        for c in cols:
            if 'table_id_str' == c:
                col_value = row['table_id']
                new_val_str = get_new_id_for_table(db_name + '.dimension_table', str(col_value))
                sql_str = "UPDATE {} SET {}='{}' WHERE {}".format(table_name, c, new_val_str, where_clause)
                # print(sql_str)
                sqls.append(sql_str)
            elif 'column_id_str' == c:
                col_value = row['column_id']
                table_id = row['table_id']
                dim_select_sql = "SELECT * from {}.dimension_table where id = {}".format(db_name, table_id)
                cu = cnx.cursor(buffered=True, dictionary=True)
                cu.execute(dim_select_sql)
                r = cu.fetchone()
                if r is None:
                    continue
                st_type = r['structure_type']
                new_val_str = ""
                if st_type == 'HIERARCHICAL':
                    new_val_str = get_new_id_for_table(db_name + '.hierarchical_dimension_columns', str(col_value))
                elif st_type == 'FLAT':
                    new_val_str = get_new_id_for_table(db_name + '.flat_dimension_columns', str(col_value))
                sql_str = "UPDATE {} SET {}='{}' WHERE {}".format(table_name, c, new_val_str, where_clause)
                # print(sql_str)
                sqls.append(sql_str)

    for s in sqls:
        utils.dump_sql(s)
        cursor.execute(s)
    cnx.commit()


def handle_dl_fact():
    table_name = data_loc_db + '.' + "data_location_fact"
    cols = ['table_id_str']
    sel_query = "SELECT * FROM {}".format(table_name)
    cursor.execute(sel_query)
    sqls = []
    for row in cursor:
        where_clause = "{} = '{}'".format('id', row['id'])
        for c in cols:
            if 'table_id_str' == c:
                col_value = row['table_id']
                new_val_str = get_new_id_for_table(db_name + '.fact_table', str(col_value))
                sql_str = "UPDATE {} SET {}='{}' WHERE {}".format(table_name, c, new_val_str, where_clause)
                # print(sql_str)
                sqls.append(sql_str)
    for s in sqls:
        utils.dump_sql(s)
        cursor.execute(s)
    cnx.commit()


def handle_dl_source():
    table_name = data_loc_db + '.' + "data_location_source"
    cols = ['table_id_str','end_point_id_str']
    sel_query = "SELECT * FROM {}".format(table_name)
    cursor.execute(sel_query)
    sqls = []
    for row in cursor:
        where_clause = "{} = '{}'".format('id', row['id'])
        for c in cols:
            if 'table_id_str' == c:
                col_value = row['table_id']
                new_val_str = get_new_id_for_table(db_name + '.source_tables', str(col_value))
                sql_str = "UPDATE {} SET {}='{}' WHERE {}".format(table_name, c, new_val_str, where_clause)
                # print(sql_str)
                sqls.append(sql_str)
            elif 'end_point_id_str' == c:
                col_value = row['endpoint_id']
                new_val_str = get_new_id_for_table(db_name + '.source_schema_end_point_mapping', str(col_value))
                sql_str = "UPDATE {} SET {}='{}' WHERE {}".format(table_name, c, new_val_str, where_clause)
                # print(sql_str)
                sqls.append(sql_str)
    for s in sqls:
        utils.dump_sql(s)
        cursor.execute(s)
    cnx.commit()


def handle_dl_summary_fact():
    table_name = data_loc_db + '.' + "data_location_summary_fact"
    cols = ['table_id_str']
    sel_query = "SELECT * FROM {}".format(table_name)
    cursor.execute(sel_query)
    sqls = []
    for row in cursor:
        where_clause = "{} = '{}'".format('id', row['id'])
        for c in cols:
            if 'table_id_str' == c:
                col_value = row['table_id']
                new_val_str = get_new_id_for_table(db_name + '.summary_table', str(col_value))
                sql_str = "UPDATE {} SET {}='{}' WHERE {}".format(table_name, c, new_val_str, where_clause)
                # print(sql_str)
                sqls.append(sql_str)
    for s in sqls:
        utils.dump_sql(s)
        cursor.execute(s)
    cnx.commit()


def handle_dl_scd_dl():
    table_name = data_loc_db + '.' + "scd_data_locations"
    cols = ['dim_table_id_str','dim_column_id_str']
    sel_query = "SELECT * FROM {}".format(table_name)
    cursor.execute(sel_query)
    cnx.commit()
    sqls = []
    for row in cursor:
        where_clause = "{} = '{}'".format('id', row['id'])
        for c in cols:
            if 'dim_table_id_str' == c:
                col_value = row['dim_table_id']
                new_val_str = get_new_id_for_table(db_name + '.dimension_table', str(col_value))
                sql_str = "UPDATE {} SET {}='{}' WHERE {}".format(table_name, c, new_val_str, where_clause)
                # print(sql_str)
                sqls.append(sql_str)
            elif 'dim_column_id_str' == c:
                table_id = row['dim_table_id']
                col_value = row['dim_column_id']
                dim_select_sql = "SELECT * from {}.dimension_table where id = {}".format(db_name, table_id)
                cu = cnx.cursor(buffered=True, dictionary=True)
                cu.execute(dim_select_sql)
                r = cu.fetchone()
                if r is None:
                    continue
                st_type = r['structure_type']
                new_val_str = ""
                if st_type == 'HIERARCHICAL':
                    new_val_str = get_new_id_for_table(db_name + '.hierarchical_dimension_columns', str(col_value))
                elif st_type == 'FLAT':
                    dim_type = r['dim_type']
                    if dim_type == 'EXTERNAL_SRC':
                        sel_from_ext_src = "SELECT * from {}.{} where column_id = {}".format(ext_db_name,
                                                                                      'external_flat_dimension_columns',
                                                                                      str(col_value))
                        cu.execute(sel_from_ext_src)
                        data = cu.fetchone()
                        if data is None:
                            continue
                        new_val_str = data['column_id_str']
                    else:
                        new_val_str = get_new_id_for_table(db_name + '.flat_dimension_columns', str(col_value))
                sql_str = "UPDATE {} SET {}='{}' WHERE {}".format(table_name, c, new_val_str, where_clause)
                # print(sql_str)
                sqls.append(sql_str)
    for s in sqls:
        utils.dump_sql(s)
        cursor.execute(s)
    cnx.commit()