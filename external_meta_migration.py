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
    # tab_comp_pk_map = {}
    tpk_lines = read_conf('/mnt/data/nitin/all_external_meta_tables.txt')
    for l in tpk_lines:
        sp = l.split('#')
        tab_pk_map[sp[0].strip()] = sp[1].strip()

    # w_ids = read_conf('/Users/nitin/Documents/cap/id_mig/src/without_id.txt')
    # for w_l in w_ids:
    #     sp = w_l.split(':')
    #     comp_pks = sp[1].strip().split(',')
    #     tab_comp_pk_map[sp[0].strip()] = comp_pks

    lines = read_conf('/mnt/data/nitin/external_fk_details.txt')
    sqls = []
    for line in lines:
        #print(line)
        sp = line.split(':')
        table_name = sp[0].strip()
        sp1 = sp[1].strip().split(',')
        pk_col = ""
        comp_pk_cols = []
        pk_col = tab_pk_map[table_name]
        # if table_name in tab_pk_map:
        #     pk_col = tab_pk_map[table_name]
        # else:
        #     comp_pk_cols = tab_comp_pk_map[table_name]

        sel_query = "SELECT * FROM {}".format(ext_db_name + '.' + table_name)
        cursor.execute(sel_query)
        for row in cursor:
            for x in sp1:
                temp = x.split('#')
                col_to_set = temp[0].strip() + "_str"
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
                new_fk_val = get_new_id_for_table(db_name + '.' + temp[1].strip(), str(row[temp[0].strip()]))
                if new_fk_val == 'VALUE_MISSING_OR_DEACTIVATED':
                    continue
                sql_str = "UPDATE {} SET {}='{}' WHERE {}".format(ext_db_name + '.' + table_name, col_to_set,
                                                                  new_fk_val, where_clause)
                # print(sql_str)
                sqls.append(sql_str)

    for s in sqls:
        utils.dump_sql(s)
        cursor.execute(s)

    cnx.commit()
    #cnx.close()


def update_ids(table_name, columns, pk_col):
    f_tab_name = ext_db_name + '.' + table_name
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
    cnx.close()


def update_id_str():
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
    update_ids(table_name, columns, pk)


# custom handling


def handle_dimension_groups():
    table_name = ext_db_name + '.dimension_groups'
    cols = ['parent_attribute_column_id_str', 'parent_functional_dependency_column_str']
    sel_query = "SELECT * FROM {}".format(table_name)
    cursor.execute(sel_query)
    sqls = []
    for row in cursor:
        where_clause = "{} = '{}'".format('column_id', row['column_id'])
        st_type = row['parent_attribute_type']
        for c in cols:
            col_value = ''
            if 'parent_attribute_column_id_str' == c:
                col_value = row['parent_attribute_column_id']
            elif 'parent_functional_dependency_column_str' == c:
                col_value = row['parent_functional_dependency_column']
            if col_value is None or col_value == "" or col_value == 0 or col_value == '0':
                continue
            new_val_str = ''
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


def handle_ext_flat_dim_col():
    table_name = ext_db_name + '.external_flat_dimension_columns'
    sel_query = "SELECT * FROM {}".format(table_name)
    cursor.execute(sel_query)
    sqls = []
    for row in cursor:
        where_clause = "{} = '{}'".format('column_id', row['column_id'])
        dim_table_id_str = get_new_id_for_table(db_name + '.flat_dimension_columns', str(row['dim_table_id']))
        column_name = row['column_name']
        scope_id = row['scope_id']
        new_col_val = dim_table_id_str + '_' + column_name + '_' + str(scope_id)
        sql_str = "UPDATE {} SET {}='{}' WHERE {}".format(table_name, 'column_id_str', new_col_val, where_clause)
        #print(sql_str)
        sqls.append(sql_str)
    for s in sqls:
        utils.dump_sql(s)
        cursor.execute(s)
    cnx.commit()


def handle_pii_masked_col():
    table_name = ext_db_name + '.pii_masked_column'
    sel_query = "SELECT * FROM {}".format(table_name)
    cursor.execute(sel_query)
    sqls = []
    for row in cursor:
        where_clause = "{} = '{}'".format('id', row['id'])
        st_type = row['column_type']
        new_val_str = ''
        col_value = row['column_id']
        if st_type == 'HIERARCHICAL':
            new_val_str = get_new_id_for_table(db_name + '.hierarchical_dimension_columns', str(col_value))
        elif st_type == 'FLAT':
            new_val_str = get_new_id_for_table(db_name + '.flat_dimension_columns', str(col_value))
        sql_str = "UPDATE {} SET {}='{}' WHERE {}".format(table_name, 'column_id_str', new_val_str, where_clause)
        #print(sql_str)
        sqls.append(sql_str)
    for s in sqls:
        utils.dump_sql(s)
        cursor.execute(s)
    cnx.commit()


def handle_custom_table_fk():
    table_name = ext_db_name + '.custom_table_fk'
    cols = ['link_table_id_str', 'link_table_cols_str']
    sel_query = "SELECT * FROM {}".format(table_name)
    cursor.execute(sel_query)
    sqls = []
    for row in cursor:
        where_clause = "{} = '{}'".format('custom_table_id', row['custom_table_id'])
        ltt = row['link_table_type']
        for c in cols:
            if 'link_table_id_str' == c:
                col_value = row['link_table_id']
                if ltt == 'DIM':
                    new_val_str = get_new_id_for_table(db_name + '.dimension_table', str(col_value))
                    sql_str = "UPDATE {} SET {}='{}' WHERE {}".format(table_name, c, new_val_str, where_clause)
                    #print(sql_str)
                    sqls.append(sql_str)
                elif ltt == 'FACT':
                    new_val_str = get_new_id_for_table(db_name + '.fact_table', str(col_value))
                    sql_str = "UPDATE {} SET {}='{}' WHERE {}".format(table_name, c, new_val_str, where_clause)
                    #print(sql_str)
                    sqls.append(sql_str)
            elif 'link_table_cols_str' == c:
                table_id = row['link_table_id']
                col_value = row['link_table_cols']
                if ltt == 'DIM':
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
                    #print(sql_str)
                    sqls.append(sql_str)
                elif ltt == 'FACT':
                    new_val_str = get_new_id_for_table(db_name + '.fact_table_column', str(col_value))
                    sql_str = "UPDATE {} SET {}='{}' WHERE {}".format(table_name, c, new_val_str, where_clause)
                    #print(sql_str)
                    sqls.append(sql_str)
    for s in sqls:
        utils.dump_sql(s)
        cursor.execute(s)
    cnx.commit()
def handle_banding():
    table_name = ext_db_name + '.banding'
    cols = ['column_id_str']
    sel_query = "SELECT * FROM {}".format(table_name)
    cursor.execute(sel_query)
    sqls = []
    for row in cursor:
        where_clause = "{} = '{}'".format('id', row['id'])
        for c in cols:
            if 'column_id_str' == c:
                col_value = row['column_id']
                table_id = row['dim_table_id']
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
                #print(sql_str)
                sqls.append(sql_str)
    for s in sqls:
        utils.dump_sql(s)
        cursor.execute(s)
    cnx.commit()