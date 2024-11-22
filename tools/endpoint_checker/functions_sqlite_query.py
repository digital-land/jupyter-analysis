import sqlite3
import pandas as pd

class DatasetSqlite:
    def __init__(self,sqlite_path):
        self.sqlite_path = sqlite_path

    def run_query(self, sql_query: str):
        """
        Receives a sql query and returns the results either in a pandas
        dataframe or just the first column as a set (this is useful to
        test presence or absence of items like tables, columns, etc).

        Note: connection is openned and closed at each query, but for use
        cases like the present one that would not offer big benefits and
        would mean having to dev thread-lcoal connection pools. For more
        info see: https://stackoverflow.com/a/14520670
        """
        with sqlite3.connect(self.sqlite_path) as con:
            cursor = con.execute(sql_query)
            cols = [column[0] for column in cursor.description]
            results = pd.DataFrame.from_records(data=cursor.fetchall(), columns=cols)

        return results
    
    def get_entities(self):
        sql = """
        select * from entity;
        """
        results = self.run_query(sql)
        return results
    
    def get_column_mappings(self):
        sql = """
        select * from column_field;
        """
        results = self.run_query(sql)
        return results
    
    def get_issues(self):
        sql = """
        select * from issue;
        """
        results = self.run_query(sql)
        return results
    
    def get_issues_by_type(self):
        sql = """
        select issue_type, COUNT(*) as count 
        from issue
        group by issue_type;
        """
        results = self.run_query(sql)
        return results