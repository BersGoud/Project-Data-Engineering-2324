from sqlalchemy import create_engine, types as sqlalchemytypes
import pandas as pd
from prefect import task, flow
import datetime as dt

@task
def lowercase_column_names(df):
    return df.rename(columns=lambda x: x.lower())

@task
def load_raw_data():
    engine = create_engine('postgresql://postgres:Newpassword@192.168.1.4:5432/postgres')
    table_names = ['aankomst', 'banen', 'klant', 'luchthavens', 'maatschappijen', 'planning', 'vertrek', 'vliegtuig', 'vliegtuigtype', 'vlucht', 'weer']
    dataframes = {}
    for table in table_names:
        query = f"SELECT * FROM raw.{table}"
        dataframes[table] = pd.read_sql(query, con=engine)
    return dataframes

@task
def process_data(dataframes):
    cleaning = {}
    archived_data = {}
    for table, df in dataframes.items():
        # Copy the data to the cleaning list
        cleaning[table] = df.copy()
        
        # Perform specific operations for each table
        if table == 'aankomst':
            # Select only the last duplicates based on the vluchtid column
            archived_data[table] = cleaning[table][cleaning[table].duplicated(subset=['vluchtid'], keep='last')]
            cleaning[table] = cleaning[table][~cleaning[table].duplicated(subset=['vluchtid'], keep='last')]
            # Replace null or NaN values in the 'bezetting' column with 0
            cleaning[table]['bezetting'] = cleaning[table]['bezetting'].fillna(0)

            # Archive rows where 'aankomsttijd' is null and remove them from the cleaning list
            aankomsttijd_null = cleaning[table][cleaning[table]['aankomsttijd'].isnull()]
            archived_data[table] = pd.concat([archived_data.get(table, pd.DataFrame()), aankomsttijd_null], ignore_index=True)
            cleaning[table] = cleaning[table][cleaning[table]['aankomsttijd'].notnull()]

        elif table == 'klant':
            # Select duplicates based on the vluchtid column and add them to archived_data
            archived_data[table] = cleaning[table][cleaning[table].duplicated(subset=['vluchtid'], keep='last')]
            cleaning[table] = cleaning[table][~cleaning[table].duplicated(subset=['vluchtid'], keep='last')]

            # Archive rows where 'shops' is null and remove them from the cleaning list
            shops_null = cleaning[table][cleaning[table]['shops'].isnull()]
            archived_data[table] = pd.concat([archived_data[table], shops_null], ignore_index=True)
            cleaning[table] = cleaning[table][cleaning[table]['shops'].notnull()]
        elif table == 'luchthavens':
            # Select only the last duplicates based on the airport column
            archived_data[table] = cleaning[table][cleaning[table].duplicated(subset=['airport'], keep='last')]
            # Remove selected rows from cleaning
            cleaning[table] = cleaning[table][~cleaning[table].duplicated(subset=['airport'], keep='last')]
        elif table == 'maatschappijen':
            # Filter the rows based on the given conditions
            filter_condition = (
                (cleaning[table]['iata'] == ';') |
                (cleaning[table]['iata'] == ';;') |
                (cleaning[table]['iata'] == '+') |
                (cleaning[table]['iata'] == '??') |
                (cleaning[table]['iata'] == '-+') |
                (cleaning[table]['iata'] == '--') |
                (cleaning[table]['iata'] == '^^') |
                (cleaning[table]['iata'] == '--+') |
                (cleaning[table]['icao'] == ';') |
                (cleaning[table]['icao'] == '+') |
                (cleaning[table]['icao'] == '-+') |
                (cleaning[table]['icao'] == '--') |
                (cleaning[table]['icao'] == '^^') |
                (cleaning[table]['icao'] == '--+') |
                (cleaning[table]['icao'] == '??') |
                (cleaning[table]['iata'] == '\\N') |
                (cleaning[table]['icao'] == '\\N')
            )
            archived_data[table] = cleaning[table][filter_condition]
            # Remove selected rows from cleaning
            cleaning[table] = cleaning[table][~filter_condition]
        elif table == 'planning':
            # Select only the last duplicates based on the vluchtnummer column
            archived_data[table] = cleaning[table][cleaning[table].duplicated(subset=['vluchtnr'], keep='last')]
            # Remove selected rows from cleaning
            cleaning[table] = cleaning[table][~cleaning[table].duplicated(subset=['vluchtnr'], keep='last')]

            # Archive rows where 'plantijd' is null and remove them from the cleaning list
            plantijd_null = cleaning[table][cleaning[table]['plantijd'].isnull()]
            archived_data[table] = pd.concat([archived_data[table], plantijd_null], ignore_index=True)
            cleaning[table] = cleaning[table][cleaning[table]['plantijd'].notnull()]
        elif table == 'vertrek':
            # Select only the last duplicates based on the vluchid column
            archived_data[table] = cleaning[table][cleaning[table].duplicated(subset=['vluchtid'], keep='last')]
            # Remove selected rows from cleaning
            cleaning[table] = cleaning[table][~cleaning[table].duplicated(subset=['vluchtid'], keep='last')]
            # Replace null or NaN values in the 'bezetting' column with 0
            cleaning[table]['bezetting'] = cleaning[table]['bezetting'].fillna(0)

            vertrektijd_null = cleaning[table][cleaning[table]['vertrektijd'].isnull()]
            archived_data[table] = pd.concat([archived_data.get(table, pd.DataFrame()), vertrektijd_null], ignore_index=True)
            cleaning[table] = cleaning[table][cleaning[table]['vertrektijd'].notnull()]
        elif table == 'vliegtuig':
            # Filter the rows where airlinecode is equal to "-"
            archived_data[table] = cleaning[table][cleaning[table]['airlinecode'] == '-']
            # Remove selected rows from cleaning
            cleaning[table] = cleaning[table][cleaning[table]['airlinecode'] != '-']
        elif table == 'weer':
            # Select only the last duplicates
            archived_data[table] = cleaning[table][cleaning[table].duplicated(keep='last')]
            # Remove selected rows from cleaning
            cleaning[table] = cleaning[table][~cleaning[table].duplicated(keep='last')]
        else:
            # For other tables, no specific operations
            archived_data[table] = pd.DataFrame()

    return archived_data, cleaning

@task
def convert_to_correct_types(cleaning):
        # Define the data types for each table
    data_types = {
        'aankomst': {
            'vluchtid': sqlalchemytypes.VARCHAR(10),
            'vliegtuigcode': sqlalchemytypes.VARCHAR(8),
            'terminal': sqlalchemytypes.CHAR(1),
            'gate': sqlalchemytypes.VARCHAR(2),
            'baan': sqlalchemytypes.CHAR(1),
            'bezetting': sqlalchemytypes.SMALLINT,
            'vracht': sqlalchemytypes.CHAR(3),
            'aankomsttijd': sqlalchemytypes.TIMESTAMP
        },
        'banen': {
            'baannummer': sqlalchemytypes.CHAR(1),
            'code': sqlalchemytypes.VARCHAR(7),
            'naam': sqlalchemytypes.VARCHAR(30),
            'lengte': sqlalchemytypes.SMALLINT
        },
        'luchthavens': {
            'airport': sqlalchemytypes.VARCHAR(150),
            'city': sqlalchemytypes.VARCHAR(150),
            'country': sqlalchemytypes.VARCHAR(150),
            'iata': sqlalchemytypes.CHAR(10),
            'icao': sqlalchemytypes.CHAR(10),
            'lat': sqlalchemytypes.FLOAT,
            'lon': sqlalchemytypes.FLOAT,
            'alt': sqlalchemytypes.SMALLINT,
            'tz': sqlalchemytypes.VARCHAR(15),
            'dst': sqlalchemytypes.CHAR(15),
            'tzname': sqlalchemytypes.VARCHAR(50)
        },
        'klant': {
            'vluchtid': sqlalchemytypes.VARCHAR(7),
            'operatie': sqlalchemytypes.DECIMAL(2, 1),
            'faciliteiten': sqlalchemytypes.DECIMAL(2, 1),
            'shops': sqlalchemytypes.DECIMAL(2, 1)
        },
        'maatschappijen': {
            'name': sqlalchemytypes.VARCHAR(50),
            'iata': sqlalchemytypes.VARCHAR(3),
            'icao': sqlalchemytypes.VARCHAR(3)
        },
        'planning': {
            'vluchtnr': sqlalchemytypes.VARCHAR(15),
            'airlinecode': sqlalchemytypes.CHAR(5),
            'destcode': sqlalchemytypes.CHAR(3),
            'planterminal': sqlalchemytypes.CHAR(1),
            'plangate': sqlalchemytypes.VARCHAR(5),
            'plantijd': sqlalchemytypes.TIME
        },
        'vertrek': {
            'vluchtid': sqlalchemytypes.VARCHAR(15),
            'vliegtuigcode': sqlalchemytypes.VARCHAR(15),
            'terminal': sqlalchemytypes.CHAR(5),
            'gate': sqlalchemytypes.VARCHAR(5),
            'baan': sqlalchemytypes.CHAR(5),
            'bezetting': sqlalchemytypes.SMALLINT,
            'vracht': sqlalchemytypes.CHAR(5),
            'vertrektijd': sqlalchemytypes.TIMESTAMP
        },
        'vliegtuig': {
            'airlinecode': sqlalchemytypes.CHAR(10),
            'vliegtuigcode': sqlalchemytypes.CHAR(10),
            'vliegtuigtype': sqlalchemytypes.CHAR(10),
            'bouwjaar': sqlalchemytypes.CHAR(10)
        },
        'vliegtuigtype': {
            'iata': sqlalchemytypes.CHAR(15),
            'icao': sqlalchemytypes.CHAR(15),
            'merk': sqlalchemytypes.CHAR(100),
            'type': sqlalchemytypes.CHAR(150),
            'wake': sqlalchemytypes.CHAR(20),
            'cat': sqlalchemytypes.CHAR(20),
            'capaciteit': sqlalchemytypes.CHAR(5),
            'vracht': sqlalchemytypes.CHAR(5)
        },
        'vlucht': {
            'vluchtid': sqlalchemytypes.CHAR(10),
            'vluchtnr': sqlalchemytypes.CHAR(10),
            'airlinecode': sqlalchemytypes.CHAR(5),
            'destcode': sqlalchemytypes.CHAR(5),
            'vliegtuigcode': sqlalchemytypes.CHAR(15),
            'datum': sqlalchemytypes.CHAR(10)
        },
        'weer': {
            'datum': sqlalchemytypes.CHAR(10),
            'ddvec': sqlalchemytypes.CHAR(10),
            'fhvec': sqlalchemytypes.CHAR(10),
            'fg': sqlalchemytypes.CHAR(10),
            'fhx': sqlalchemytypes.CHAR(10),
            'fhxh': sqlalchemytypes.CHAR(10),
            'fhn': sqlalchemytypes.CHAR(10),
            'fhnh': sqlalchemytypes.CHAR(10),
            'fxx': sqlalchemytypes.CHAR(10),
            'fxxh': sqlalchemytypes.CHAR(10),
            'tg': sqlalchemytypes.CHAR(10),
            'tn': sqlalchemytypes.CHAR(10),
            'tnh': sqlalchemytypes.CHAR(10),
            'tx': sqlalchemytypes.CHAR(10),
            'txh': sqlalchemytypes.CHAR(10),
            't10n': sqlalchemytypes.CHAR(10),
            't10nh': sqlalchemytypes.CHAR(10),
            'sq': sqlalchemytypes.CHAR(10),
            'sp': sqlalchemytypes.CHAR(10),
            'q': sqlalchemytypes.CHAR(10),
            'dr': sqlalchemytypes.CHAR(10),
            'rh': sqlalchemytypes.CHAR(10),
            'rhx': sqlalchemytypes.CHAR(10),
            'rhxh': sqlalchemytypes.CHAR(10),
            'pg': sqlalchemytypes.CHAR(10),
            'px': sqlalchemytypes.CHAR(10),
            'pxh': sqlalchemytypes.CHAR(10),
            'pn': sqlalchemytypes.CHAR(10),
            'pnh': sqlalchemytypes.CHAR(10),
            'vvn': sqlalchemytypes.CHAR(10),
            'vvnh': sqlalchemytypes.CHAR(10),
            'vvx': sqlalchemytypes.CHAR(10),
            'vvxh': sqlalchemytypes.CHAR(10),
            'ng': sqlalchemytypes.CHAR(10),
            'ug': sqlalchemytypes.CHAR(10),
            'ux': sqlalchemytypes.CHAR(10),
            'uxh': sqlalchemytypes.CHAR(10),
            'un': sqlalchemytypes.CHAR(10),
            'unh': sqlalchemytypes.CHAR(10),
            'ev2': sqlalchemytypes.CHAR(10)
        }
    }

    for table, df in cleaning.items():
        if table in data_types:
            dtype_mapping = data_types[table]
            for column, dtype in dtype_mapping.items():
                if dtype == sqlalchemytypes.CHAR:
                    df[column] = df[column].astype(str)
                elif dtype == sqlalchemytypes.VARCHAR:
                    df[column] = df[column].astype(str)
                elif dtype == sqlalchemytypes.SMALLINT:
                    df[column] = df[column].astype(int)
                elif dtype == sqlalchemytypes.FLOAT:
                    df[column] = df[column].astype(float)
                elif dtype == sqlalchemytypes.DECIMAL:
                    df[column] = df[column].astype(float)
                elif dtype == sqlalchemytypes.TIMESTAMP:
                    df[column] = pd.to_datetime(df[column])
                elif dtype == sqlalchemytypes.TIME:
                    df[column] = pd.to_datetime(df[column]).dt.time
    return cleaning

@task
def save_processed_data(archived_data, cleaning):
    engine = create_engine('postgresql://postgres:Newpassword@192.168.1.4:5432/postgres')
    for table, df in archived_data.items():
        df.to_sql(table, con=engine, schema='archived', if_exists='append', index=False)
    for table, df in cleaning.items():
        df.to_sql(table, con=engine, schema='cleansed', if_exists='append', index=False)

@flow
def preprocess_data_flow():
    dataframes = load_raw_data()
    archived_data, cleaning = process_data(dataframes)
    cleaning = convert_to_correct_types(cleaning)
    save_processed_data(archived_data, cleaning)
