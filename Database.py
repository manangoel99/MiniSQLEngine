from Table import Table


class Database(object):
    def __init__(self):
        self.tables = dict()

    def create_table(self, table: Table):
        self.tables[table.tablename] = table

    def get_table(self, name: str):
        try:
            return self.tables[name]
        except BaseException:
            return None

    def get_all_tables(self):
        return self.tables

    def __str__(self):
        return "\n".join([self.tables[i].__str__() for i in self.tables])
