import pandas as pd
import sqlalchemy as sa
from datetime import datetime

########update cnxn with your server information########
#connect
cnxn_str = (r"Driver={SQL Server};"
            r"Server=ICF2110473\MSSQLSERVER01;"
            r"database=OpenFEMA Data Sets;"
            "Trusted_Connection=yes;")


engine = sa.create_engine('mssql+pyodbc:///?odbc_connect={}'.format(cnxn_str))

def createTable(connection):
    now = datetime.now()
    tables = connection.table_names()
    tables = [x for x in tables if 'Summary' not in x]
    summaryDf = pd.DataFrame(columns=['TableName', 'NumRows', 'RefreshDate'])
    tableLenList = []
    tableNames = []
    for table in tables:
        df = pd.read_sql(table, connection, chunksize=10_000)
        data = [x for x in df]
        lenData = 0
        for item in data:
            lenData += len(item)
        tableLenList.append(lenData)
        tableNames.append(table)
        print(table)
    summaryDf['TableName'] = tableNames
    summaryDf['NumRows'] = tableLenList
    summaryDf['RefreshDate'] = str(now)
    summaryDf.to_sql('Summary', connection, if_exists='append', index=False)


if __name__ == '__main__':

    createTable(engine)
    print("Done. Check SSMS to ensure update was successful.")