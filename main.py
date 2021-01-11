class Table(object):
    def __init__(self, tablename: str, column_names: list):
        self.tablename = tablename
        self.num_columns = len(column_names)
        self.data = dict()
        for name in column_names:
            self.data[name] = []

    def __str__(self):
        return self.tablename + " " + ",".join(self.data.keys())

class Database(object):
    def __init__(self):
        self.tables = dict()
    
    def create_table(self, table: Table):
        self.tables[table.tablename] = table
    
    def __str__(self):
        return "\n".join([self.tables[i].__str__() for i in self.tables])

def read_metadata(filepath: str):
    with open(filepath) as f:
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
                database.create_table(table)
            i += 1


if __name__ == '__main__':
    database = Database()
    read_metadata('./files/metadata.txt')
    print(database)
