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

    lines = read_conf('/mnt/data/nitin/fk_details_dynamic.txt')
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

        sel_query = "SELECT * FROM {}".format(dynamic_db_name + '.'+table_name)
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
                sql_str = "UPDATE {} SET {}='{}' WHERE {}".format(dynamic_db_name + '.'+table_name, col_to_set, new_fk_val, where_clause)
                #print(sql_str)
                sqls.append(sql_str)

    for s in sqls:
        utils.dump_sql(s)
        cursor.execute(s)

    cnx.commit()


def handle_provider_dim_map():
    table_name = dynamic_db_name + '.provider_dimension_mapping'
    cols = ['provider_col_name_str', 'dimension_column_id_str']
    sel_query = "SELECT * FROM {}".format(table_name)
    cursor.execute(sel_query)
    sqls = []
    for row in cursor:
        where_clause = "{} = '{}'".format('id', row['id'])
        for c in cols:
            if 'provider_col_name_str' == c:
                # print(where_clause)
                col_value = row['provider_col_name']
                if col_value is None or col_value == "":
                    continue
                s_col = find_between(col_value, "_colId_", "_instance_")
                new_val_str = col_value.replace(s_col, get_new_id_for_table(db_name + '.source_table_column', str(s_col)))
                sql_str = "UPDATE {} SET {}='{}' WHERE {}".format(table_name, c, new_val_str, where_clause)
                #print(sql_str)
                sqls.append(sql_str)
            elif 'dimension_column_id_str' == c:
                col_value = row['dimension_column_id']
                dim_t_id = row['dim_table_id']
                if col_value is None or col_value == "":
                    continue
                dim_select_sql = "SELECT * from {}.dimension_table where id = {}".format(db_name, dim_t_id)
                cu = cnx.cursor(buffered=True, dictionary=True)
                cu.execute(dim_select_sql)
                r = cu.fetchone()
                st_type = r['structure_type']
                if st_type == 'HIERARCHICAL':
                    new_val_str = get_new_id_for_table(db_name + '.hierarchical_dimension_columns', str(col_value))
                elif st_type == 'FLAT':
                    new_val_str = get_new_id_for_table(db_name + '.flat_dimension_columns', str(col_value))
                sql_str = "UPDATE {} SET {}='{}' WHERE {}".format(table_name, c, new_val_str, where_clause)
                #print(sql_str)
                sqls.append(sql_str)
    for s in sqls:
        utils.dump_sql(s)
        cursor.execute(s)
    cnx.commit()


def handle_transformation():
    table_name = dynamic_db_name + '.transformations'
    cols = ['provider_table_id_str']
    sel_query = "SELECT * FROM {}".format(table_name)
    cursor.execute(sel_query)
    sqls = []
    for row in cursor:
        where_clause = "{} = '{}'".format('id', row['id'])
        for c in cols:
            if 'provider_table_id_str' == c:
                col_value = row['provider_table_id']
                new_val_str = get_new_id_for_table(db_name + '.provider_tables', str(col_value))
                sql_str = "UPDATE {} SET {}='{}' WHERE {}".format(table_name, c, new_val_str, where_clause)
                sqls.append(sql_str)
    for s in sqls:
        utils.dump_sql(s)
        cursor.execute(s)
    cnx.commit()
def update_ids(table_name, columns, pk_col):
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor(buffered=True, dictionary=True)
    f_tab_name = dynamic_db_name + '.' + table_name
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
        utils.dump_sql(s)
        cursor.execute(s)
    cnx.commit()
def update_id_str():
    lines = read_conf('/mnt/data/nitin/test_conf_dynamic.txt')
    for line in lines:
        sp = line.split(':')
        table_name = sp[0].strip()
        columns_and_pk = sp[1].strip().split('#')
        columns = columns_and_pk[0].strip().split(',')
        pk = columns_and_pk[1].strip()
        for s in columns:
            s
        #print(line)
        update_ids(table_name, columns, pk)