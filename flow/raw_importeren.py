import psycopg2
from sqlalchemy import create_engine, types as sqlalchemytypes
import pandas as pd
from prefect import task, flow

@task
def instance_postgress_db():

    # Verbindingsgegevens
    host = "192.168.1.4"
    dbname = "postgres"
    user = "postgres"
    password = "Newpassword"
    port = "5432"  # Standaard PostgreSQL poort

    # Maak de verbinding
    conn = psycopg2.connect(
        host=host,
        dbname=dbname,
        user=user,
        password=password,
        port=port
    )

    # Maak een cursor aan
    cur = conn.cursor()
    # Open het SQL-bestand
    with open('./sql_scripts/raw.sql', 'r') as file:
        sql_script = file.read()
    cur.execute(sql_script)

    with open('./sql_scripts/archived.sql', 'r') as file:
        sql_script = file.read()
    cur.execute(sql_script)

    with open('./sql_scripts/cleansed.sql', 'r') as file:
        sql_script = file.read()
    cur.execute(sql_script)

    conn.commit()  # Vergeet niet te committeren als het script wijzigingen maakt


@task
# Functie om kolomnamen om te zetten naar kleine letters
def lowercase_column_names(df):
    return df.rename(columns=lambda x: x.lower())


@flow
def import_data_raw():
    instance_postgress_db()
    # Vervang 'username', 'password', 'host', 'port', en 'database' met jouw databasegegevens
    engine = create_engine('postgresql://postgres:Newpassword@192.168.1.4:5432/postgres')


    # Pas het pad naar je CSV-bestand aan
    df = pd.read_csv('./source_data/export_aankomst.txt', sep='\t', dtype=str)
    df = lowercase_column_names(df)
    df.to_sql('aankomst', con=engine, schema='raw', if_exists='append', index=False, dtype={
        "vluchtid": sqlalchemytypes.String,
        "vliegtuigcode": sqlalchemytypes.String,
        "terminal": sqlalchemytypes.String,
        "gate": sqlalchemytypes.String,
        "baan": sqlalchemytypes.String,
        "bezetting": sqlalchemytypes.String,
        "vracht": sqlalchemytypes.String,
        "aankomsttijd": sqlalchemytypes.String,
    })

    # Banen
    df_banen = pd.read_csv('./source_data/export_banen.csv', sep=';', dtype=str)
    df_banen = lowercase_column_names(df_banen)
    kolomtypes = {
        "baannummer": sqlalchemytypes.String,
        "code": sqlalchemytypes.String,
        "naam": sqlalchemytypes.String,
        "lengte": sqlalchemytypes.String
    }
    df_banen.to_sql('banen', con=engine, schema='raw', if_exists='append', index=False, dtype=kolomtypes)

    # Klant
    df_klant = pd.read_csv('./source_data/export_klant.csv', sep=';', dtype=str)
    df_klant = lowercase_column_names(df_klant)
    kolomtypes = {
        "vluchtid": sqlalchemytypes.String,
        "operatie": sqlalchemytypes.String,
        "faciliteiten": sqlalchemytypes.String,
        "shops": sqlalchemytypes.String
    }
    df_klant.to_sql('klant', con=engine, schema='raw', if_exists='append', index=False, dtype=kolomtypes)

    # Luchthavens
    df_luchthavens = pd.read_csv('./source_data/export_luchthavens.txt', sep='\t', dtype=str, encoding='windows-1252')
    df_luchthavens.rename(columns={'Tz': 'tzname'}, inplace=True)
    df_luchthavens = lowercase_column_names(df_luchthavens)
    kolomtypes = {
        "airport": sqlalchemytypes.String,
        "city": sqlalchemytypes.String,
        "country": sqlalchemytypes.String,
        "iata": sqlalchemytypes.String,
        "icao": sqlalchemytypes.String,
        "lat": sqlalchemytypes.String,
        "lon": sqlalchemytypes.String,
        "alt": sqlalchemytypes.String,
        "tz": sqlalchemytypes.String,
        "dst": sqlalchemytypes.String,
        "tzname": sqlalchemytypes.String
    }
    df_luchthavens.to_sql('luchthavens', con=engine, schema='raw', if_exists='append', index=False, dtype=kolomtypes)

    # Maatschappijen
    df_maatschappijen = pd.read_csv('./source_data/export_maatschappijen.txt', sep='\t', dtype=str, encoding='windows-1252')
    df_maatschappijen = lowercase_column_names(df_maatschappijen)
    kolomtypes = {
        "name": sqlalchemytypes.String,
        "iata": sqlalchemytypes.String,
        "icao": sqlalchemytypes.String
    }
    df_maatschappijen.to_sql('maatschappijen', con=engine, schema='raw', if_exists='append', index=False, dtype=kolomtypes)

    # Planning
    df_planning = pd.read_csv('./source_data/export_planning.txt', sep='\t', dtype=str)
    df_planning = lowercase_column_names(df_planning)
    kolomtypes = {
        "vluchtnr": sqlalchemytypes.String,
        "airlinecode": sqlalchemytypes.String,
        "destcode": sqlalchemytypes.String,
        "planterminal": sqlalchemytypes.String,
        "plangate": sqlalchemytypes.String,
        "plantijd": sqlalchemytypes.String
    }
    df_planning.to_sql('planning', con=engine, schema='raw', if_exists='append', index=False, dtype=kolomtypes)

    # Vetrek
    df_vertrek = pd.read_csv('./source_data/export_vertrek.txt', sep='\t', dtype=str)
    df_vertrek = lowercase_column_names(df_vertrek)
    kolomtypes = {
        "vluchtid": sqlalchemytypes.String,
        "vliegtuigcode": sqlalchemytypes.String,
        "verminal": sqlalchemytypes.String,
        "vate": sqlalchemytypes.String,
        "baan": sqlalchemytypes.String,
        "bezetting": sqlalchemytypes.String,
        "vracht": sqlalchemytypes.String,
        "vertrektijd": sqlalchemytypes.String
    }
    df_vertrek.to_sql('vertrek', con=engine, schema='raw', if_exists='append', index=False, dtype=kolomtypes)

    # Vliegtuig
    df_vliegtuig = pd.read_csv('./source_data/export_vliegtuig.txt', sep='\t', dtype=str)
    df_vliegtuig = lowercase_column_names(df_vliegtuig)
    kolomtypes = {
        "airlinecode": sqlalchemytypes.String,
        "vliegtuigcode": sqlalchemytypes.String,
        "vliegtuigtype": sqlalchemytypes.String,
        "bouwjaar": sqlalchemytypes.String
    }
    df_vliegtuig.to_sql('vliegtuig', con=engine, schema='raw', if_exists='append', index=False, dtype=kolomtypes)

    # Vliegtuigtype
    df_vliegtuigtype = pd.read_csv('./source_data/export_vliegtuigtype.csv', sep=';', dtype=str)
    df_vliegtuigtype = lowercase_column_names(df_vliegtuigtype)
    kolomtypes = {
        "iata": sqlalchemytypes.String,
        "icao": sqlalchemytypes.String,
        "merk": sqlalchemytypes.String,
        "type": sqlalchemytypes.String,
        "wake": sqlalchemytypes.String,
        "cat": sqlalchemytypes.String,
        "capaciteit": sqlalchemytypes.String,
        "vracht": sqlalchemytypes.String
    }
    df_vliegtuigtype.to_sql('vliegtuigtype', con=engine, schema='raw', if_exists='append', index=False, dtype=kolomtypes)

    # Vlucht
    df_vlucht = pd.read_csv('./source_data/export_vlucht.txt', sep='\t', dtype=str)
    df_vlucht = lowercase_column_names(df_vlucht)
    kolomtypes = {
        "vluchtid": sqlalchemytypes.String,
        "vluchtnr": sqlalchemytypes.String,
        "airlinecode": sqlalchemytypes.String,
        "destcode": sqlalchemytypes.String,
        "vliegtuigcode": sqlalchemytypes.String,
        "datum": sqlalchemytypes.String
    }
    df_vlucht.to_sql('vlucht', con=engine, schema='raw', if_exists='append', index=False, dtype=kolomtypes)

    # weer
    df_weer = pd.read_csv('./source_data/export_weer.txt', sep='\t', dtype=str)
    df_weer = lowercase_column_names(df_weer)
    kolomtypes = {
        "datum": sqlalchemytypes.String,
        "ddvec": sqlalchemytypes.String,
        "fhvec": sqlalchemytypes.String,
        "fg": sqlalchemytypes.String,
        "fhx": sqlalchemytypes.String,
        "fhxh": sqlalchemytypes.String,
        "fhn": sqlalchemytypes.String,
        "fhnh": sqlalchemytypes.String,
        "fxx": sqlalchemytypes.String,
        "fxxh": sqlalchemytypes.String,
        "tg": sqlalchemytypes.String,
        "tn": sqlalchemytypes.String,
        "tnh": sqlalchemytypes.String,
        "tx": sqlalchemytypes.String,
        "txh": sqlalchemytypes.String,
        "t10n": sqlalchemytypes.String,
        "t10nh": sqlalchemytypes.String,
        "sq": sqlalchemytypes.String,
        "sp": sqlalchemytypes.String,
        "q": sqlalchemytypes.String,
        "dr": sqlalchemytypes.String,
        "rh": sqlalchemytypes.String,
        "rhx": sqlalchemytypes.String,
        "rhxh": sqlalchemytypes.String,
        "pg": sqlalchemytypes.String,
        "px": sqlalchemytypes.String,
        "pxh": sqlalchemytypes.String,
        "pn": sqlalchemytypes.String,
        "pnh": sqlalchemytypes.String,
        "vvn": sqlalchemytypes.String,
        "vvnh": sqlalchemytypes.String,
        "vvx": sqlalchemytypes.String,
        "vvxh": sqlalchemytypes.String,
        "ng": sqlalchemytypes.String,
        "ug": sqlalchemytypes.String,
        "ux": sqlalchemytypes.String,
        "uxh": sqlalchemytypes.String,
        "un": sqlalchemytypes.String,
        "unh": sqlalchemytypes.String,
        "ev2": sqlalchemytypes.String
    }
    df_weer.to_sql('weer', con=engine, schema='raw', if_exists='append', index=False, dtype=kolomtypes)