import sys
import mysql.connector
import logging


# Database connection
class Database(object):

    def __init__(self, db_local):
        self.db_local = db_local
        self.db_conn = None
        self.db_cursor = None

    def __enter__(self):
        try:
            self.db_conn = mysql.connector.connect(**self.db_local)
            self.db_cursor = self.db_conn.cursor()

        except Exception as e:
            logging.error('Database connection FAILED {}'.format(e))
            print('Database connection FAILED {}'.format(e), file=sys.stderr)

        return self

    def __exit__(self, exception_type, exception_val, trace):
        try:
            self.db_cursor.close()
            self.db_conn.close()

        except Exception as e:
            logging.error('Disconnect from database FAILED {}'.format(e))
            print('Disconnect from database FAILED {}'.format(e), file=sys.stderr)

    def insert_row(self, sql, data=None):
        try:
            self.db_cursor.execute(sql, data)
            self.db_conn.commit()
            logging.info('Insert row into database SUCCEEDED')
            print('Insert row into database SUCCEEDED', file=sys.stdout)
        except Exception as e:
            logging.error('Insert row into database FAILED {}'.format(e))
            print('Insert row into database FAILED {}'.format(e), file=sys.stderr)

    def sql_select(self, sql, data=None):
        try:
            self.db_cursor.execute(sql, data)
            result = self.db_cursor.fetchall()
            return result
        except Exception as e:
            logging.error('SQL SELECT FAILED {}'.format(e))
            print('SQL SELECT FAILED {}'.format(e), file=sys.stderr)
