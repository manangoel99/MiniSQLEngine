import csv
from Table import Table
from Database import Database
from utils import read_metadata
from moz_sql_parser import parse
import json
from itertools import product
from collections import defaultdict

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
            try:
                parsed_query = parse(query)
            except:
                print("Only Select Queries are supported")
                break
            # Fetch Required Tables
            print(parsed_query)
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
            
            if parsed_query['select'] == "*":
                for table in tables.values():
                    cols = table.get_column_names()
                    for col in cols:
                        column_names.append(f"{table.tablename}.{col}")
            else:
                if type(parsed_query['select']) != list:
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
            
            table = Table(query, column_names)
            cols = table.get_column_names()
            
            table_to_col = defaultdict(list)
            for col in cols:
                tab_name, col_name = col.split('.')
                table_to_col[tab_name].append(col_name)
            
            table_data = defaultdict(list)

            for tab_name in table_to_col.keys():
                table_data[tab_name] = tables[tab_name].get_columns(table_to_col[tab_name])
            
            rows = list(product(*table_data.values()))
            for row in rows:
                act_row = []
                for elem in row:
                    act_row += elem
                table.add_row(act_row)
            table.print_table()



if __name__ == '__main__':
    database = Database()
    read_metadata('./files', database)
    main()
    # table1 = database.get_table("table1")
    # table1.print_table()
