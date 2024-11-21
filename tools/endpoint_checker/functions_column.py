import csv
import logging

def add_extra_column_mappings(column_path,additional_col_mappings):
     
    fieldnames = []
    rows = []
    with open(column_path) as f:
        dictreader = csv.DictReader(f)
        fieldnames = dictreader.fieldnames
        for row in dictreader:
            rows.append(row)
    
    #save the assignmeents
    with open(column_path, 'w',newline='') as f:
        writer = csv.DictWriter(f,fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
        writer.writerows(additional_col_mappings)

def add_extra_concats(concat_path,additional_concats):
    logging.warning('adding concats ran')
    fieldnames = []
    rows = []
    with open(concat_path) as f:
        dictreader = csv.DictReader(f)
        fieldnames = dictreader.fieldnames
        for row in dictreader:
            rows.append(row)
    logging.warning(fieldnames)
    #save the assignmeents
    with open(concat_path, 'w',newline='') as f:
        writer = csv.DictWriter(f,fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
        writer.writerows(additional_concats)