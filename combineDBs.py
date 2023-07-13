import pandas as pd
import mysql.connector
from SQL_Auth import host, user, password, database


def load_database(datatable):
    connection = mysql.connector.connect(
        host=host,  # Server, usually localhost
        user=user,  # your username, e.g., root
        password=password,  # your password
        database=database  # Name of the database
    )

    cursor = connection.cursor()
    query = f"SELECT * FROM {datatable}"
    cursor.execute(query)
    columns = [column[0] for column in cursor.description]
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=columns)

    cursor.close()
    connection.close()
    return df


class Indicators:
    def __init__(self):
        self.bballref_df = load_database('baseball_reference_season_games')
        self.bovada_df = load_database('bovada_data')
        self.espn_df = load_database('espn_data')
        self.vsin_df = load_database('vsin_data')

    def link_all(self):
        bov_df = self.bovada_df.dropna(subset=['DC_Game_ID'])
        vsin_df = self.vsin_df.dropna(subset=['DC_Game_ID'])

        df = bov_df.merge(vsin_df, how='outer', on='DC_Game_ID')
        df.to_csv("combo_test.csv", index=False)


if __name__ == '__main__':
    ind = Indicators()
    ind.link_all()
