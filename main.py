import csv
from Table import Table
from Database import Database
from moz_sql_parser import parse
import json
from itertools import product
from collections import defaultdict, OrderedDict

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
                table = Table(table_name, column_names)
                with open(f"{filepath}/{table_name}.csv", 'r') as f:
                    reader = csv.reader(f, delimiter=',')
                    for row in reader:
                        table.add_row(row)
                database.create_table(table)
            i += 1


def main():
    while True:
        query = input("MiniSQL> ").strip()
        
        if query == "exit" or query == "quit":
            break
        if not query.endswith(';'):
            print("Query must end with ;")
        
        queries = query.split(';')
        queries = queries[:-1]
        
        for idx, query in enumerate(queries):
            distinct_query = False
            simple_query = True
            aggregation_query = False
            where_query = False
            try:
                parsed_query = parse(query)
            except:
                print("Only Select Queries are supported")
                break
            print(parsed_query)
            # Fetch Required Tables
            table_names = parsed_query['from']
            if type(table_names) != list:
                table_names = [table_names]
            tables = {}
            flag = False

            for table_name in table_names:
                tab = database.get_table(table_name)
                if tab == None:
                    print("Table does not exist")
                    flag = True
                    break
                tables[table_name] = tab

            if flag == True:
                break
                    
            column_names = []
            
            # Fetch required columns
            if 'where' in parsed_query:
                where_query = True
            if parsed_query['select'] == "*":
                reqd_columns = []
                for table in tables.values():
                    cols = table.get_column_names()
                    for col in cols:
                        reqd_columns.append({'value' : col})
            elif 'value' in parsed_query['select']:
                if 'distinct' in parsed_query['select']['value']:
                    distinct_query = True
                    simple_query = False
                    aggregation_query= False
                    reqd_columns = parsed_query['select']['value']['distinct']
                if type(parsed_query['select']['value']) == str:
                    simple_query = True
                    reqd_columns = [{'value' : parsed_query['select']['value']}]
                elif list(parsed_query['select']['value'].keys())[0] in aggregate_functions.keys():
                    val = list(parsed_query['select']['value'].keys())[0]
                    aggregation = aggregate_functions[val]
                    reqd_columns = [{'value' : parsed_query['select']['value'][val]}]
                    distinct_query = False
                    simple_query = False
                    aggregation_query = True

            elif type(parsed_query['select']) != list:
                reqd_columns = [parsed_query['select']]
            else:
                reqd_columns = parsed_query['select']
            col_exist = {}
            for col in reqd_columns:
                col_exist[col['value']] = False

            for table in tables.values():
                cols = table.get_column_names()
                for reqd_col in reqd_columns:
                    if reqd_col['value'] in cols:
                        column_names.append(f"{table.tablename}.{reqd_col['value']}")
                        col_exist[reqd_col['value']] = True
                
            if False in col_exist.values():
                print("Column Does Not Exist")
                break
            
            # Add fetched data to temporary table
        table = Table(query, column_names)
        cols = table.get_column_names()
            
        table_to_col = defaultdict(list)
        table_to_cond = defaultdict(list)

        for col in cols:
            tab_name, col_name = col.split('.')
            table_to_col[tab_name].append(col_name)
            if where_query == True:
                conditions = parsed_query['where']
                if 'and' in conditions:
                    conditions = parsed_query['where']['and']
                elif 'or' in conditions:
                    conditions = parsed_query['where']['or']
                else:
                    conditions = [conditions]
                for condition in conditions:
                    for key, val in condition.items():
                        if val[0] in tables[tab_name].get_column_names():
                            table_to_cond[tab_name].append([key, val])

        for key in table_to_cond.keys():
            table_to_cond[key] = [i for n, i in enumerate(table_to_cond[key]) if i not in table_to_cond[key][:n]]
            if len(table_to_cond[key]) > 1:
                table_to_cond[key].append(list(parsed_query['where'].keys())[0])
        table_data = defaultdict(list)
        print(table_to_cond)
        if where_query == True:
            for tab_name in table_to_col.keys():
                for tab, conditions in table_to_cond.items():
                    if tab == tab_name:
                        table_data[tab_name] = tables[tab_name].get_columns(table_to_col[tab_name], conditions)
                    else:
                        if len(table_data[tab_name]) == 0:
                            table_data[tab_name] = tables[tab_name].get_columns(table_to_col[tab_name])

        else:
            for tab_name in table_to_col.keys():
                table_data[tab_name] = tables[tab_name].get_columns(table_to_col[tab_name])

        rows = list(product(*table_data.values()))
        act_rows = []
        for row in rows:
            act_row = []
            for elem in row:
                act_row += elem
                # print(conditions)
            act_rows.append(act_row)
            
        if simple_query == True:
            for row in act_rows:
                table.add_row(row)
            table.print_table()

        if distinct_query == True:
            act_rows = list(set(tuple(row) for row in act_rows))
            for row in act_rows:
                table.add_row(row)
            table.print_table()
            
        if aggregation_query == True:
            # cols = [[] for _ in range(len(act_rows[0]))]
            cols = []
            for col_idx in range(len(act_rows[0])):
                cols.append([])
                for row_idx in range(len(act_rows)):
                    cols[col_idx].append(act_rows[row_idx][col_idx])
            table.add_row([aggregation(col) for col in cols])
            table.print_table()



if __name__ == '__main__':
    database = Database()
    read_metadata('./files', database)
    main()
    # table1 = database.get_table("table1")
    # table1.print_table()
