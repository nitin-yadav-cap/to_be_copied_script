import mysql.connector

import utils
from base_data import get_new_id_for_table
from utils import config
from utils import find_between

cnx = mysql.connector.connect(**config)
cursor = cnx.cursor(buffered=True, dictionary=True)


db_name = utils.db_name
dynamic_db_name = utils.dynamic_db_name
ext_db_name = utils.ext_db_name

def handle_dimension_table():
    table_name = db_name + '.dimension_table'
    cols = ['auto_update_cols_str']
    sel_query = "SELECT * FROM {}".format(table_name)
    cursor.execute(sel_query)
    sqls = []
    for row in cursor:
        for c in cols:
            if 'auto_update_cols_str' == c:
                where_clause = "{} = '{}'".format('id', row['id'])
                # print(where_clause)
                cols_value = row['auto_update_cols']
                if cols_value is None or cols_value == "":
                    continue
                st_type = row['structure_type']
                if st_type == 'HIERARCHICAL':
                    vals = cols_value.split(',')
                    new_vals = []
                    for v in vals:
                        new_vals.append(get_new_id_for_table(db_name + '.hierarchical_dimension_columns', v))
                    new_val_str = ','.join(new_vals)
                elif st_type == 'FLAT':
                    vals = cols_value.split(',')
                    new_vals = []
                    for v in vals:
                        new_vals.append(get_new_id_for_table(db_name + '.flat_dimension_columns', v))
                    new_val_str = ','.join(new_vals)
                sql_str = "UPDATE {} SET {}='{}' WHERE {}".format(table_name, c, new_val_str, where_clause)
                #print(sql_str)
                sqls.append(sql_str)
    for s in sqls:
        utils.dump_sql(s)
        cursor.execute(s)
    cnx.commit()
    #cnx.close()


def handle_fact_table():
    table_name = db_name + '.fact_table'
    cols = ['parent_src_tables_str']
    sel_query = "SELECT * FROM {}".format(table_name)
    cursor.execute(sel_query)
    sqls = []
    for row in cursor:
        where_clause = "{} = '{}'".format('id', row['id'])
        for c in cols:
            if 'parent_src_tables_str' == c:
                # print(where_clause)
                cols_value = row['parent_src_tables']
                if cols_value is None or cols_value == "":
                    continue
                vals = cols_value.split(',')
                new_vals = []
                for v in vals:
                    new_vals.append(get_new_id_for_table(db_name + '.source_tables', v))
                new_val_str = ','.join(new_vals)
                sql_str = "UPDATE {} SET {}='{}' WHERE {}".format(table_name, c, new_val_str, where_clause)
                #print(sql_str)
                sqls.append(sql_str)
    for s in sqls:
        utils.dump_sql(s)
        cursor.execute(s)
    cnx.commit()
    #cnx.close()


def handle_fact_table_column():
    table_name = db_name + '.fact_table_column'
    cols = ['functional_dependency_keys_str']
    sel_query = "SELECT * FROM {}".format(table_name)
    cursor.execute(sel_query)
    sqls = []
    for row in cursor:
        for c in cols:
            if 'functional_dependency_keys_str' == c:
                where_clause = "{} = '{}'".format('id', row['id'])
                # print(where_clause)
                cols_value = row['functional_dependency_keys']
                if cols_value is None or cols_value == "":
                    continue
                vals = cols_value.split(',')
                new_vals = []
                for v in vals:
                    new_vals.append(get_new_id_for_table(db_name + '.fact_table_column', v))
                new_val_str = ','.join(new_vals)
                sql_str = "UPDATE {} SET {}='{}' WHERE {}".format(table_name, c, new_val_str, where_clause)
                #print(sql_str)
                sqls.append(sql_str)
    for s in sqls:
        utils.dump_sql(s)
        cursor.execute(s)
    cnx.commit()
    #cnx.close()


def handle_fact_table_run_type():
    table_name = db_name + '.fact_table_run_type'
    cols = ['table_id_str']
    sel_query = "SELECT * FROM {}".format(table_name)
    cursor.execute(sel_query)
    sqls = []
    for row in cursor:
        for c in cols:
            if 'table_id_str' == c:
                where_clause = "{} = '{}'".format('id', row['id'])
                # print(where_clause)
                col_value = row['table_id']
                tt = row['table_type']
                new_val_str = ""
                if tt == 'SUMMARY':
                    new_val_str = get_new_id_for_table(db_name + '.summary_table', str(col_value))
                elif tt == 'FACT':
                    new_val_str = get_new_id_for_table(db_name + '.fact_table', str(col_value))
                sql_str = "UPDATE {} SET {}='{}' WHERE {}".format(table_name, c, new_val_str, where_clause)
                #print(sql_str)
                sqls.append(sql_str)
    for s in sqls:
        utils.dump_sql(s)
        cursor.execute(s)
    cnx.commit()
    #cnx.close()


def handle_fact_table_std_cond():
    table_name = db_name + '.fact_table_standard_condition'
    cols = ['column_id_str']
    sel_query = "SELECT * FROM {}".format(table_name)
    cursor.execute(sel_query)
    sqls = []
    for row in cursor:
        for c in cols:
            if 'column_id_str' == c:
                where_clause = "{} = '{}'".format('id', row['id'])
                # print(where_clause)
                col_value = row['column_id']
                ref_table = row['refTableId']
                dim_select_sql = "SELECT * from {}.dimension_table where id = {}".format(db_name, ref_table)
                cu = cnx.cursor(buffered=True, dictionary=True)
                cu.execute(dim_select_sql)
                r = cu.fetchone()
                if r is None:
                    continue
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
    #cnx.close()


def handle_logical_summary_table():
    table_name = db_name + '.logical_summary_table'
    cols = ['group_by_cols_str']
    sel_query = "SELECT * FROM {}".format(table_name)
    cursor.execute(sel_query)
    sqls = []
    for row in cursor:
        for c in cols:
            if 'group_by_cols_str' == c:
                where_clause = "{} = '{}'".format('id', row['id'])
                # print(where_clause)
                cols_value = row['group_by_cols']
                if cols_value is None or cols_value == "":
                    continue
                vals = cols_value.split(',')
                new_vals = []
                for v in vals:
                    new_vals.append(get_new_id_for_table(db_name + '.summary_groupBy', v))
                new_val_str = ','.join(new_vals)
                sql_str = "UPDATE {} SET {}='{}' WHERE {}".format(table_name, c, new_val_str, where_clause)
                #print(sql_str)
                sqls.append(sql_str)
    for s in sqls:
        utils.dump_sql(s)
        cursor.execute(s)
    cnx.commit()
    #cnx.close()


def handle_mongo_collections():
    table_name = db_name + '.mongo_collections'
    cols = ['dependent_struct_ids_str']
    sel_query = "SELECT * FROM {}".format(table_name)
    cursor.execute(sel_query)
    sqls = []
    for row in cursor:
        for c in cols:
            if 'dependent_struct_ids_str' == c:
                where_clause = "{} = '{}'".format('id', row['id'])
                # print(where_clause)
                cols_value = row['dependent_struct_ids']
                if cols_value is None or cols_value == "":
                    continue
                vals = cols_value.split(',')
                new_vals = []
                for v in vals:
                    new_vals.append(get_new_id_for_table(db_name + '.mongo_structs', v))
                new_val_str = ','.join(new_vals)
                sql_str = "UPDATE {} SET {}='{}' WHERE {}".format(table_name, c, new_val_str, where_clause)
                #print(sql_str)
                sqls.append(sql_str)
    for s in sqls:
        utils.dump_sql(s)
        cursor.execute(s)
    cnx.commit()
    #cnx.close()


def handle_provider_dim_map():
    table_name = db_name + '.provider_dimension_mapping'
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
    #cnx.close()


def handle_source_table():
    table_name = db_name + '.source_tables'
    cols = ['pkey_columns_str']
    sel_query = "SELECT * FROM {}".format(table_name)
    cursor.execute(sel_query)
    sqls = []
    for row in cursor:
        for c in cols:
            if 'pkey_columns_str' == c:
                where_clause = "{} = '{}'".format('id', row['id'])
                # print(where_clause)
                cols_value = row['pkey_columns']
                if cols_value is None or cols_value == "":
                    continue
                vals = cols_value.split(',')
                new_vals = []
                for v in vals:
                    new_vals.append(get_new_id_for_table(db_name + '.source_table_column', v))
                new_val_str = ','.join(new_vals)
                sql_str = "UPDATE {} SET {}='{}' WHERE {}".format(table_name, c, new_val_str, where_clause)
                #print(sql_str)
                sqls.append(sql_str)
    for s in sqls:
        utils.dump_sql(s)
        cursor.execute(s)
    cnx.commit()
    #cnx.close()


def handle_source_target_dim_col_map():
    table_name = db_name + '.source_target_dimension_column_mapping'
    cols = ['join_col_id_str', 'select_col_id_str']
    sel_query = "SELECT * FROM {}".format(table_name)
    cursor.execute(sel_query)
    sqls = []
    for row in cursor:
        where_clause = "{} = '{}' AND {} = '{}' AND {} = '{}'".format('table_mapping_id', row['table_mapping_id'],
                                                                      'dimension_id', row['dimension_id'],
                                                                      'scope_id', row['scope_id'])
        dim_t_id = row['dim_table_id']
        dim_select_sql = "SELECT * from {}.dimension_table where id = {}".format(db_name, dim_t_id)
        cu = cnx.cursor(buffered=True, dictionary=True)
        cu.execute(dim_select_sql)
        r = cu.fetchone()
        if r is None:
            continue
        st_type = r['structure_type']
        for c in cols:
            if 'join_col_id_str' == c:
                # print(where_clause)
                col_value = row['join_col_id']
                if col_value is None or col_value == "":
                    continue
                new_val_str = ""
                if st_type == 'HIERARCHICAL':
                    new_val_str = get_new_id_for_table(db_name + '.hierarchical_dimension_columns', str(col_value))
                elif st_type == 'FLAT':
                    new_val_str = get_new_id_for_table(db_name + '.flat_dimension_columns', str(col_value))
                sql_str = "UPDATE {} SET {}='{}' WHERE {}".format(table_name, c, new_val_str, where_clause)
                #print(sql_str)
                sqls.append(sql_str)
            elif 'select_col_id_str' == c:
                col_value = row['select_col_id']
                if col_value is None or col_value == "":
                    continue
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
    #cnx.close()


def handle_source_target_table_map():
    table_name = db_name + '.source_target_table_mapping'
    cols = ['source_table_id_str', 'target_table_id_str']
    sel_query = "SELECT * FROM {}".format(table_name)
    cursor.execute(sel_query)
    sqls = []
    for row in cursor:
        where_clause = "{} = '{}'".format('id', row['id'])
        s_t_type = row['source_table_type']
        t_t_type = row['target_table_type']
        for c in cols:
            if 'source_table_id_str' == c:
                col_value = row['source_table_id']
                if s_t_type == 'LOGICAL':
                    sql_str = "UPDATE {} SET {}='{}' WHERE {}".format(table_name, c, col_value, where_clause)
                    #print(sql_str)
                    sqls.append(sql_str)
            elif 'target_table_id_str' == c:
                # print(where_clause)
                col_value = row['target_table_id']
                if t_t_type == 'LOGICAL':
                    sql_str = "UPDATE {} SET {}='{}' WHERE {}".format(table_name, c, col_value, where_clause)
                    #print(sql_str)
                    sqls.append(sql_str)
    for s in sqls:
        utils.dump_sql(s)
        cursor.execute(s)
    cnx.commit()
    #cnx.close()


def handle_summary_condition():
    table_name = db_name + '.summary_condition'
    cols = ['refTableId_str', 'column_id_str']
    sel_query = "SELECT * FROM {}".format(table_name)
    cursor.execute(sel_query)
    sqls = []
    for row in cursor:
        where_clause = "{} = '{}'".format('id', row['id'])
        t_type = row['tableType']
        for c in cols:
            if 'refTableId_str' == c:
                col_value = row['refTableId']
                if t_type == 'DIMENSION':
                    new_val_str = get_new_id_for_table(db_name + '.dimension_table', str(col_value))
                    sql_str = "UPDATE {} SET {}='{}' WHERE {}".format(table_name, c, new_val_str, where_clause)
                    #print(sql_str)
                    sqls.append(sql_str)
                elif t_type == 'FACT':
                    new_val_str = get_new_id_for_table(db_name + '.fact_table', str(col_value))
                    sql_str = "UPDATE {} SET {}='{}' WHERE {}".format(table_name, c, new_val_str, where_clause)
                    #print(sql_str)
                    sqls.append(sql_str)
            elif 'column_id_str' == c:
                # print(where_clause)
                table_id = row['refTableId']
                col_value = row['column_id']
                if t_type == 'DIMENSION':
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
                elif t_type == 'FACT':
                    new_val_str = get_new_id_for_table(db_name + '.fact_table_column', str(col_value))
                    sql_str = "UPDATE {} SET {}='{}' WHERE {}".format(table_name, c, new_val_str, where_clause)
                    #print(sql_str)
                    sqls.append(sql_str)
    for s in sqls:
        utils.dump_sql(s)
        cursor.execute(s)
    cnx.commit()
    #cnx.close()


def handle_summary_group_by():
    table_name = db_name + '.summary_groupBy'
    cols = ['refTableId_str', 'column_id_str']
    sel_query = "SELECT * FROM {}".format(table_name)
    cursor.execute(sel_query)
    sqls = []
    for row in cursor:
        where_clause = "{} = '{}'".format('id', row['id'])
        t_type = row['tableType']
        for c in cols:
            if 'refTableId_str' == c:
                col_value = row['refTableId']
                if t_type == 'DIMENSION':
                    new_val_str = get_new_id_for_table(db_name + '.dimension_table', str(col_value))
                    sql_str = "UPDATE {} SET {}='{}' WHERE {}".format(table_name, c, new_val_str, where_clause)
                    #print(sql_str)
                    sqls.append(sql_str)
                elif t_type == 'FACT':
                    new_val_str = get_new_id_for_table(db_name + '.fact_table', str(col_value))
                    sql_str = "UPDATE {} SET {}='{}' WHERE {}".format(table_name, c, new_val_str, where_clause)
                    #print(sql_str)
                    sqls.append(sql_str)
            elif 'column_id_str' == c:
                # print(where_clause)
                table_id = row['refTableId']
                col_value = row['column_id']
                if t_type == 'DIMENSION':
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
                elif t_type == 'FACT':
                    new_val_str = get_new_id_for_table(db_name + '.fact_table_column', str(col_value))
                    sql_str = "UPDATE {} SET {}='{}' WHERE {}".format(table_name, c, new_val_str, where_clause)
                    #print(sql_str)
                    sqls.append(sql_str)
    for s in sqls:
        utils.dump_sql(s)
        cursor.execute(s)
    cnx.commit()
    #cnx.close()


def handle_transformation():
    table_name = db_name + '.transformations'
    f_to_replace = ['tableId', 'selectCols', 'fk', 'colId', 'groupByCol', 'valueCol']
    sel_query = "SELECT * FROM {}".format(table_name)
    cursor.execute(sel_query)
    sqls = []
    for row in cursor:
        where_clause = "{} = '{}'".format('id', row['id'])
        # print(where_clause)
        template = row['template'].replace(" ", "")
        for f in f_to_replace:
            if f == 'tableId':
                vals = utils.find_all_between(template, f)
                if vals is not None:
                    for v in vals:
                        if v is None or v =="":
                            continue
                        old_vals_str = '"tableId":'+v+','
                        new_vals_str = '"tableId":"'+get_new_id_for_table(db_name + '.source_tables', v)+'",'
                        template = template.replace(old_vals_str, new_vals_str)
            elif f == 'selectCols':
                vals = utils.find_all_between(template, f)
                if vals is None:
                    continue
                for v in vals:
                    if v is None or v =="":
                        continue
                    all_val = v.split(',')
                    new_vals = []
                    for id in all_val:
                        new_vals.append('"'+get_new_id_for_table(db_name + '.source_table_column', id)+'"')
                    new_vals_str = '"selectCols":['+",".join(new_vals)+'],'
                    old_vals_str = '"selectCols":['+v+'],'
                    template = template.replace(old_vals_str, new_vals_str)
            elif f == 'fk':
                vals = utils.find_all_between(template, f)
                if vals is None:
                    continue
                for v in vals:
                    if v is None or v =="":
                        continue
                    v= v.replace("}", "")
                    old_vals_str = '"fk":'+v+','
                    new_vals_str = '"fk":"'+get_new_id_for_table(db_name + '.source_table_fk_constraints', v)+'",'
                    template = template.replace(old_vals_str, new_vals_str)
            elif f == 'colId':
                vals = utils.find_all_between(template, f)
                if vals is None:
                    continue
                for v in vals:
                    if v is None or v =="":
                        continue
                    old_vals_str = '"colId":'+v+','
                    new_vals_str = '"colId":"'+get_new_id_for_table(db_name + '.source_table_column', v)+'",'
                    template = template.replace(old_vals_str, new_vals_str)
            elif f == 'groupByCol':
                vals = utils.find_all_between(template, f)
                if vals is None:
                    continue
                for v in vals:
                    if v is None or v =="":
                        continue
                    old_vals_str = '"groupByCol":'+v+','
                    new_vals_str = '"groupByCol":"'+get_new_id_for_table(db_name + '.source_table_column', v)+'",'
                    template = template.replace(old_vals_str, new_vals_str)
            elif f == 'valueCol':
                vals = utils.find_all_between(template, f)
                if vals is None:
                    continue
                for v in vals:
                    if v is None or v =="":
                        continue
                    old_vals_str = '"valueCol":'+v+','
                    new_vals_str = '"valueCol":"'+get_new_id_for_table(db_name + '.source_table_column', v)+'",'
                    template = template.replace(old_vals_str, new_vals_str)
        sql_str = "UPDATE {} SET {}='{}' WHERE {}".format(table_name, 'template_str', template, where_clause)
        #print(sql_str)
        sqls.append(sql_str)

    for s in sqls:
        utils.dump_sql(s)
        cursor.execute(s)
    cnx.commit()
    cnx.close()

def handle_transpose_columns():
    table_name = db_name + '.transpose_columns'
    f_to_replace = ['columnId']
    sel_query = "SELECT * FROM {}".format(table_name)
    cursor.execute(sel_query)
    sqls = []
    for row in cursor:
        tc_id = row['transposed_column_id']
        scope_id = row['scope_id']
        if tc_id == 2227 and scope_id == -997:
            continue
        where_clause = "{} = '{}' AND {} = '{}'".format('transposed_column_id', tc_id,
                                                                      'scope_id', scope_id)
        cond = row['conditions']
        if cond is None:
            continue
        cond = cond.replace(" ", "")
        for f in f_to_replace:
            if f == 'columnId':
                vals = utils.find_all_between(cond, f)
                i =0
                col_vs = utils.find_all_between(cond, "values")
                for v in vals:
                    c_v = col_vs[i]
                    old_vals_str = '"columnId":'+v+','
                    v = v.replace('"', "")
                    new_vals_str = '"columnId":"'+get_new_id_for_table(db_name + '.source_table_column', v)+'",'
                    cond = cond.replace(old_vals_str, new_vals_str)
                    i= i+1
                    if c_v.replace("'", "").isnumeric():
                        cond = cond.replace("'", "\\'")
                    else:
                        # cond = [bytes(s, 'utf-8').decode('unicode_escape') for s in cond]
                        cond = cond.replace("'", "\\\'")
        sql_str = "UPDATE {} SET {}='{}' WHERE {};".format(table_name, 'conditions_str', cond, where_clause)
        #print(sql_str)
        sqls.append(sql_str)
    for s in sqls:
        utils.dump_sql(s)
        cursor.execute(s)
    cnx.commit()
    cnx.close()