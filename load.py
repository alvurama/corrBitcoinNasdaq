from transform import tablon, today
import pyodbc

# TODO: Implement load from tablon to database
# 3.1 - Validar la operación en mercados durante el día del flow. Descartar los registros del tablón en caso de que no se haya operado en NASDAQ ese día.
# 3.2 - Crear tabla si no existe.
# 3.3 - Validar si existe el registro. Si existe descartamos los registros.
# 3.4 - Inserción de los registros de la tabla.

# 3.1
num_rows_tablon = len(tablon.index)
if num_rows_tablon != 1:
    print('Error: Hay conflicto en la actualzación de los datos. Probablemente no se haya operado en NASDAQ el día de hoy.')
    # return

# 3.2
nombre_cols_sql = ['[' + col + ']' for col in tablon.columns]
sql_create_valores_btc = '''
    IF NOT EXISTS ( SELECT name FROM sys.tables WHERE name = 'btcvalores')
        CREATE TABLE btcvalores (
            [fecha] DATE PRIMARY KEY,
            {} DECIMAL(20,2),
            {} DECIMAL(20,2),
            {} DECIMAL(20,2),
            {} VARCHAR,
            {} DECIMAL(20,2),
            {} DECIMAL(20,2),
            {} DECIMAL(20,2),
            {} VARCHAR,
            {} DECIMAL(20,2),
            {} DECIMAL(20,2),
            {} DECIMAL(20,2),
            {} VARCHAR,
            {} DECIMAL(20,2),
            {} DECIMAL(20,2),
            {} DECIMAL(20,2),
            {} VARCHAR,
            {} DECIMAL(20,2),
            {} DECIMAL(20,2),
            {} DECIMAL(20,2),
            {} VARCHAR,
            {} DECIMAL(20,2),
            {} DECIMAL(20,2),
            {} DECIMAL(20,2),
            {} VARCHAR,
            {} DECIMAL(20,2)

    )'''.format(*nombre_cols_sql)

server = 'ALVARO\SQLSERVER19'
database = 'caso_nasdaq_btc'
user = 'sa'
password = '1218'

cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' +
                      server+';DATABASE='+database+';UID='+user+';PWD=' + password)

cursor = cnxn.cursor()
cursor.execute(sql_create_valores_btc)
cnxn.commit()

# 3.3
sql_exists = "SELECT fecha FROM [dbo].[btcvalores] WHERE fecha = '{}'".format(
    str(today))
cursor.execute(sql_exists)
row = cursor.fetchone()
if row:
    print('Error: Ya existe un registro para el día de hoy.')
    # return

# 3.4
tablon.insert(0, 'fecha', today)
tablon['fecha'] = tablon.index

for index, row in tablon.iterrows():
    cursor.execute('INSERT INTO dbo.btcvalores VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, \
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )', row.tolist())
    cnxn.commit()
cursor.close()
cnxn.close()
