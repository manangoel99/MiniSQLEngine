from Database import Database
from Table import Table
import csv

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
