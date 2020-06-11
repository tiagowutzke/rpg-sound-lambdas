import logging

from database.query import Query

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class Persistence:
    def __init__(
            self,
            conn=None,
            *args,
            **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.connection = conn

    def truncate_insert(self, table, **cols_values):
        query = Query()

        columns, _ = query.get_cols_param(cols_values.keys())
        values, _ = query.get_cols_param(cols_values.values())

        try:
            sql = f"""
                TRUNCATE TABLE {table};
                INSERT INTO {table} ({columns})
                VALUES ({values});
            """
            self.connection.commit_transaction(sql)
            return True

        except Exception as e:
            message = f'Error on insert values:\n{e}'
            logging.info(message)
            return False

    def update(self, table, column, value, where_col, col_value):
        try:
            sql = f"""
                UPDATE {table}
                SET {column} = '{value}'
            """
            self.connection.commit_transaction(sql)
        except Exception as e:
            message = f'Error on update:\n{e}'
            print(message)

    def update_config(self, text_id, voice_id):
        try:
            sql = f"""
                UPDATE config
                SET channel_voice_id = '{voice_id}',
                    channel_text_id =  '{text_id}'
            """
            self.connection.commit_transaction(sql)
            return True
        except Exception as e:
            message = f'Error on update config:\n{e}'
            print(message)
            return False

    def delete_by_id(self, table, id):
        try:
            sql = f"""
                DELETE FROM {table}
                WHERE id = {id}
            """
            self.connection.commit_transaction(sql)
        except Exception as e:
            message = f'Error on delete:\n{e}'
            print(message)
