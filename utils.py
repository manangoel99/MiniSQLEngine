import csv

def check(lhs, op, rhs):
    if op == 'eq':
        return lhs == rhs
    elif op == 'gt':
        return lhs > rhs
    elif op == 'lt':
        return lhs < rhs
    elif op == 'gte':
        return lhs >= rhs
    elif op == 'lte':
        return lhs <= rhs
    elif op == '<>':
        return lhs != rhs
