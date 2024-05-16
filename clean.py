import psycopg2
from sqlalchemy import create_engine, types as sqlalchemytypes
import pandas as pd
# importeer database data van raw postegressql naar df om te cleanen

engine = create_engine('postgresql://postgres:Newpassword@192.168.1.4:5432/postgres')

df_aankomst = pd.read_sql_query('SELECT * FROM raw.aankomst', con=engine)
df_banen = pd.read_sql_query('SELECT * FROM raw.banen', con=engine)
df_klant = pd.read_sql_query('SELECT * FROM raw.klant', con=engine)
df_luchthavens = pd.read_sql_query('SELECT * FROM raw.luchthavens', con=engine)
df_maatschappijen = pd.read_sql_query('SELECT * FROM raw.maatschappijen', con=engine)
df_planning = pd.read_sql_query('SELECT * FROM raw.planning', con=engine)
df_vertrek = pd.read_sql_query('SELECT * FROM raw.vertrek', con=engine)
df_vliegtuig = pd.read_sql_query('SELECT * FROM raw.vliegtuig', con=engine)
df_vliegtuigtype = pd.read_sql_query('SELECT * FROM raw.vliegtuigtype', con=engine)
df_vlucht = pd.read_sql_query('SELECT * FROM raw.vlucht', con=engine)
df_weer = pd.read_sql_query('SELECT * FROM raw.weer', con=engine)
