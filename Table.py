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
    
    def get_columns(self, cols: list):
        rows = []
        for idx in range(self.num_rows):
            rows.append([self.data[col][idx] for col in cols])
        return rows

    # def get_column_aggregate(self, cols: list, agg_func):
    #     row = []
    #     for col in cols:
    #         row.append(agg_func(self.data[col]))
    #     return row