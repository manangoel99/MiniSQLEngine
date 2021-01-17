import csv
import json
import sys
from collections import OrderedDict, defaultdict
from itertools import product

from moz_sql_parser import parse

from Database1 import Database
from Table1 import Table
from utils import check
import copy

aggregate_functions = {
    'max' : max,
    'min' : min,
    'sum' : sum,
    'average' : lambda x : sum(x) / len(x),
    'count' : len,
}

def read_metadata(filepath: str, database: Database):
    with open(f"{filepath}/metadata.txt") as f:
        metadata = f.readlines()
        i = 0
        while i < len(metadata):
            if "<begin_table>" in metadata[i].strip():
                i += 1
                table_name = metadata[i].strip()
                column_names = []
                while "<end_table>" not in metadata[i].strip():
                    i += 1
                    column_names.append(metadata[i].strip())
                column_names = column_names[:-1]
                table = Table(table_name, column_names, database)
                with open(f"{filepath}/{table_name}.csv", 'r') as f:
                    reader = csv.reader(f, delimiter=',')
                    for row in reader:
                        table.add_row(row)
                database.create_table(table)
            i += 1

def QueryDatabase(query: str):
    simple_query = True
    where_query = False
    distinct_query = False
    if query == "exit" or query == "quit":
        quit()
    if not query.endswith(';'):
        print("Query must end with ;")
        quit()
    try:
        parsed_query = parse(query)
    except:
        print("Query is Incorrect")
        return
    table_names = parsed_query['from']
    if type(table_names) != list:
        table_names = [table_names]
    
    table_rows = defaultdict(list)
    tables = {}
    all_col_names = []
    col_to_table = {}
    for table_name in table_names:
        table = database.get_table(table_name)
        if table == None:
            print("Table Does Not Exist")
            return -1
        all_col_names += [f"{table_name}.{i}" for i in table.get_column_names()]
        tables[table_name] = table
        for col_name in table.get_column_names():
            col_to_table[col_name] = table_name
        for idx in range(table.get_num_rows()):
            table_rows[table_name].append(table.get_row(idx))

    rows = list(product(*table_rows.values()))
    temp_table = Table(query, all_col_names)

    for idx, val in enumerate(rows):
        act_row = []
        for r in val:
            act_row.extend(r)
        rows[idx] = act_row
        temp_table.add_row(act_row)


    column_names = []
    print(parsed_query)
    if 'where' in parsed_query:
        where_query = True
    if parsed_query['select'] == "*":
        reqd_columns = []
        for col in all_col_names:
            reqd_columns.append({'value' : col.split(".")[1]})
    elif 'value' in parsed_query['select']:
        if 'distinct' in parsed_query['select']['value']:
            distinct_query = True
            simple_query = False
            # aggregation_query= False
            reqd_columns = parsed_query['select']['value']['distinct']
            if type(reqd_columns) != list:
                reqd_columns = [reqd_columns]
        if type(parsed_query['select']['value']) == str:
            print("YO")
            simple_query = True
            reqd_columns = [{'value' : parsed_query['select']['value']}]
    elif type(parsed_query['select']) != list:
        reqd_columns = [parsed_query['select']]
    else:
        reqd_columns = copy.copy(parsed_query['select'])
    col_exist = {}
    # print(reqd_columns)
    for idx, col in enumerate(reqd_columns):
        if type(col['value']) == dict:
            keys = list(col['value'].keys())[0]
            reqd_columns[idx] = {'value' : col['value'][keys]}
            col = reqd_columns[idx]
        col_exist[col['value']] = False

    for table in tables.values():
        cols = table.get_column_names()
        for reqd_col in reqd_columns:
            if reqd_col['value'] in cols:
                column_names.append(f"{table.tablename}.{reqd_col['value']}")
                col_exist[reqd_col['value']] = True
    
    if False in col_exist.values():
        print("Column Does Not Exist")
        return -1
    
    if where_query:
        multiple_conditions = False
        multi_cond_type = None
        if 'and' in parsed_query['where'] or 'or' in parsed_query['where']:
            multi_cond_type = list(parsed_query['where'].keys())[0]
            multiple_conditions = True
        if not multiple_conditions:
            reqd_rows = makeQuery(temp_table, parsed_query['where'], col_to_table)
        else:
            rows = []
            for condition in parsed_query['where'][multi_cond_type]:
                rows.append(makeQuery(temp_table, condition, col_to_table))
            # part1 = set(tuple(i) for i in rows[0])
            # part2 = set(tuple(i) for i in rows[1])
            if multi_cond_type == 'and':
                reqd = [x for x in rows[0] if x in rows[1]]
            else:
                reqd = [x for x in rows[0]]
                reqd += [x for x in rows[1] if x not in reqd]
        temp_table = Table(query, temp_table.get_column_names())
        for row in reqd_rows:
            temp_table.add_row(row)
    
    if 'groupby' in parsed_query:
        groupby_col = parsed_query['groupby']['value']
        col_name = f"{col_to_table[groupby_col]}.{groupby_col}"
        column_data = temp_table.get_column(col_name)

        distinct_vals = set(column_data)
        occurrences = defaultdict(list)
        for val in distinct_vals:
            occurrences[val] = [i for i, x in enumerate(column_data) if x == val]
        # print(occurrences)
        # print(parsed_query['select'])
        for column in parsed_query['select']:
            aggregation = "None"
            if type(column['val']) == dict:
                aggregation = list(column['value'].keys())[0]
                column_name = column['value'][aggregation]
            else:
                column_name = column['value']
            




    
    if distinct_query:
        distinct_cols = parsed_query['select']['value']['distinct']
        col_names = []
        if type(distinct_cols) != list:
            distinct_cols = [distinct_cols]
        for i in distinct_cols:
            col_names.append(i['value'])
        col_names = [f"{col_to_table[i]}.{i}" for i in col_names]
        cols = {}
        for col in col_names:
            cols[col] = temp_table.get_column(col)
        rows = set()
        for idx in range(temp_table.get_num_rows()):
            row = []
            for col in cols:
                row.append(cols[col][idx])
            row = tuple(row)
            rows.add(row)
        rows = list(list(i) for i in rows)
        temp_table = Table(query, col_names)
        for row in rows:
            temp_table.add_row(row)
        
    if 'orderby' in parsed_query:
        column_for_ordering = parsed_query['orderby']['value']
        ordering = 'asc'
        if 'sort' in parsed_query['orderby']:
            ordering = parsed_query['orderby']['sort']
        col_name = f"{col_to_table[column_for_ordering]}.{column_for_ordering}"
        if col_name not in temp_table.get_column_names():
            print("Column Does Not Exist")
            return -1
        if ordering == 'asc':
            indices = sorted(range(len(temp_table.data[col_name])), key=temp_table.data[col_name].__getitem__)
        elif ordering == 'desc':
            indices = sorted(range(len(temp_table.data[col_name])), key=temp_table.data[col_name].__getitem__, reverse=True)
        rows = []
        for idx in indices:
            rows.append(temp_table.get_row(idx))
        temp_table = Table(query, temp_table.get_column_names())
        for row in rows:
            temp_table.add_row(row)
    
    
    temp_table.print_table()
    

def makeQuery(table: Table, query: dict, col_to_table: dict):
    query_type = list(query.keys())[0]
    col_name, val = query[query_type]
    reqd_col = f"{col_to_table[col_name]}.{col_name}"
    cross_column = False
    if str(val).isalpha():
        cross_column = True
    rows = []
    rows_to_add = [False for _ in range(table.get_num_rows())]
    for idx in range(table.get_num_rows()):
        if not cross_column:
            if check(table.data[reqd_col][idx], query_type, val):
                rows_to_add[idx] = True
        else:
            second_col = f"{col_to_table[val]}.{val}"
            if check(table.data[reqd_col][idx], query_type, table.data[second_col][idx]):
                rows_to_add[idx] = True
    
    for idx in range(table.get_num_rows()):
        if rows_to_add[idx]:
            rows.append(table.get_row(idx))
    return rows

if __name__ == '__main__':
    database = Database()
    read_metadata('./files', database)
    query = sys.argv[1].strip()
    QueryDatabase(query)

    # main()