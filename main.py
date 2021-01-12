import csv

class Table(object):
    def __init__(self, tablename: str, column_names: list):
        self.tablename = tablename
        self.num_columns = len(column_names)
        self.data = dict()
        for name in column_names:
            self.data[name] = []
        self.num_rows = 0

    def __str__(self):
        return self.tablename + " " + ",".join(self.data.keys())

    def add_row(self, data):
        assert self.num_columns == len(data), "Number of columns in the new row does not match number of columns in table"
        for idx, col in enumerate(self.data.keys()):
            self.data[col].append(int(data[idx]))
        self.num_rows += 1

    def print_table(self):
        row_format ="{:>15}" * (len(self.data.keys()) + 1)
        print(row_format.format("", *self.data.keys()))
        for i in range(self.num_rows):
            val = []
            for col in self.data.keys():
                val.append(self.data[col][i])
            print(row_format.format("", *val))

class Database(object):
    def __init__(self):
        self.tables = dict()
    
    def create_table(self, table: Table):
        self.tables[table.tablename] = table
    
    def get_table(self, name: str):
        return self.tables[name]

    def __str__(self):
        return "\n".join([self.tables[i].__str__() for i in self.tables])

def read_metadata(filepath: str):
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


if __name__ == '__main__':
    database = Database()
    read_metadata('./files')
    table1 = database.get_table("table1")
    table1.print_table()
