from utils import check

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

    def add_row(self, data: list):
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
        
    def get_column_names(self):
        return self.data.keys()

    def get_num_rows(self):
        return self.num_rows

    def get_entry(self, col_name: str, row_idx: int):
        return self.data[col_name][row_idx]

    def get_row(self, idx: int):
        row = []
        for col in self.data.keys():
            row.append(self.data[col][idx])
        return row
    
    def get_columns(self, cols: list, conditions=None):
        rows = []
        if conditions is None:
            for idx in range(self.num_rows):
                rows.append([self.data[col][idx] for col in cols])
            return rows

        if len(conditions) == 1:
            conditions = conditions[0]
            cond_type = conditions[0]
            column, val = conditions[1]
            row_to_add = [False for i in range(self.num_rows)]

            for idx in range(self.num_rows):
                if check(self.data[column][idx], cond_type, val):
                    row_to_add[idx] = True
            for idx in range(self.num_rows):
                if row_to_add[idx] == True:
                    rows.append([self.data[col][idx] for col in cols])
            return rows
        else:
            join_type = conditions[-1]
            conditions = conditions[:-1]
            rows_prime = []
            for condition in conditions:
                print(condition)
                cond_type = condition[0]
                column, val = condition[1]
                row_to_add = [False for i in range(self.num_rows)]
                rows = []
                for idx in range(self.num_rows):
                    if check(self.data[column][idx], cond_type, val):
                        row_to_add[idx] = True
                for idx in range(self.num_rows):
                    if row_to_add[idx] == True:
                        rows.append([self.data[col][idx] for col in cols])
                rows_prime.append(set(tuple(i) for i in rows))
            if join_type == 'and':
                rows = rows_prime[0].intersection(rows_prime[1])
            if join_type == 'or':
                rows = rows_prime[0].union(rows_prime[1])
            print(list(list(i) for i in rows))
            return list(list(i) for i in rows)
        

    # def get_column_aggregate(self, cols: list, agg_func):
    #     row = []
    #     for col in cols:
    #         row.append(agg_func(self.data[col]))
    #     return row