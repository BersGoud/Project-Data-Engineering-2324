from sqlalchemy import create_engine, types as sqlalchemytypes, text
import pandas as pd
from prefect import task, flow

# Database connection string
DB_CONNECTION = 'postgresql://postgres:Newpassword@192.168.1.4:5432/postgres'

# Create SQLAlchemy engine
engine = create_engine(DB_CONNECTION)

@task
def load_data(schema, table_name, engine):
    """Load data from a specified table in a given schema."""
    query = f'SELECT * FROM {schema}.{table_name}'
    df = pd.read_sql(query, engine)
    return df

@task
def combine_and_clean_data(luchthavens, vliegtuig, vliegtuigtype, weer, maatschappijen, aankomst, vertrek, vlucht, klant):
    """Combine and clean data from the cleansed schema."""
    # Ensure the key columns exist
    if 'vliegtuigtype' not in vliegtuig.columns:
        raise KeyError("Column 'vliegtuigtype' not found in 'vliegtuig' table")
    if 'iata' not in vliegtuigtype.columns:
        raise KeyError("Column 'iata' not found in 'vliegtuigtype' table")

    # Strip any leading or trailing whitespace from the key columns
    vliegtuig['vliegtuigtype'] = vliegtuig['vliegtuigtype'].str.strip()
    vliegtuigtype['iata'] = vliegtuigtype['iata'].str.strip()
    maatschappijen['iata'] = maatschappijen['iata'].str.strip()
    vlucht['airlinecode'] = vlucht['airlinecode'].str.strip()

    # Combine vliegtuig and vliegtuigtype data
    vliegtuig_dim = vliegtuig.merge(vliegtuigtype, how='left', left_on='vliegtuigtype', right_on='iata')
    vliegtuig_dim = vliegtuig_dim[['airlinecode', 'vliegtuigcode', 'vliegtuigtype', 'bouwjaar', 
                                   'merk', 'type', 'wake', 'cat', 'capaciteit', 'vracht']].drop_duplicates(subset=['vliegtuigcode'])

    # Combine aankomst and vlucht data
    vlucht_fct = vlucht.copy()
    vlucht_fct['bezetting'] = vlucht_fct['vluchtid'].map(aankomst.set_index('vluchtid')['bezetting'])
    vlucht_fct['vracht'] = vlucht_fct['vluchtid'].map(aankomst.set_index('vluchtid')['vracht'])
    vlucht_fct['aankomsttijd'] = vlucht_fct['vluchtid'].map(aankomst.set_index('vluchtid')['aankomsttijd'])
    vlucht_fct['vertrektijd'] = vlucht_fct['vluchtid'].map(vertrek.set_index('vluchtid')['vertrektijd'])

    # Filter rows where airlinecode is exactly 2 characters long
    vlucht_fct = vlucht_fct[vlucht_fct['airlinecode'].str.len() == 2]

    # Filter rows where airlinecode is present in maatschappij_dim
    valid_airlinecodes = maatschappijen['iata'].tolist()
    vlucht_fct = vlucht_fct[vlucht_fct['airlinecode'].isin(valid_airlinecodes)]

    # Perform join between vlucht_fct and vliegtuigtype based on airlinecode and iata
    vlucht_fct = vlucht_fct.merge(vliegtuigtype, how='left', left_on='airlinecode', right_on='iata')

    # Rename columns to avoid conflicts and match the schema
    vlucht_fct.rename(columns={
        'airlinecode': 'maatschappij_id',
        'vliegtuigcode_x': 'vliegtuigcode',
        'vracht_x': 'vracht',
        'vracht_y': 'vracht_vertrek'
    }, inplace=True)

    # Select only necessary columns for vlucht_fct
    try:
        vlucht_fct = vlucht_fct[['vluchtid', 'vluchtnr', 'maatschappij_id', 'destcode', 'vliegtuigcode', 'bezetting', 'vracht', 'aankomsttijd', 'vertrektijd']]
    except KeyError as e:
        print("Available columns in vlucht_fct:", vlucht_fct.columns)
        raise e

    # Select only necessary columns and remove duplicates for luchthavens_dim
    luchthaven_dim = luchthavens[['airport', 'city', 'country', 'iata', 'icao', 'lat', 'lon', 'alt', 'tz', 'dst', 'tzname']].drop_duplicates(subset=['iata'])

    # Select only necessary columns for maatschappij_dim
    maatschappij_dim = maatschappijen[['name', 'iata', 'icao']].drop_duplicates(subset=['iata'])

    # Filter klant_dim to only include rows with vluchtid present in vlucht_fct
    valid_vluchtids = vlucht_fct['vluchtid'].tolist()
    klant_dim = klant[klant['vluchtid'].isin(valid_vluchtids)]

    return {
        'luchthaven_dim': luchthaven_dim,
        'vliegtuig_dim': vliegtuig_dim,
        'weer_dim': weer,
        'maatschappij_dim': maatschappij_dim,
        'vlucht_fct': vlucht_fct,
        'klant_dim': klant_dim
    }

@task
def remove_duplicates(df, subset):
    """Remove duplicate rows based on a subset of columns."""
    df_dedup = df.drop_duplicates(subset=subset)
    return df_dedup

@task
def write_to_dw(df, table_name, engine, dtype=None):
    """Write DataFrame to Data Warehouse."""
    # Ensure the DataFrame is not empty
    if df.empty:
        print(f"No data to write for table {table_name}")
        return
    
    try:
        # Clear the existing table
        with engine.connect() as connection:
            truncate_query = text(f"TRUNCATE TABLE dw.{table_name} RESTART IDENTITY CASCADE;")
            connection.execute(truncate_query)
        
        # Write the DataFrame to the database
        df.to_sql(table_name, engine, schema='dw', if_exists='append', index=False, dtype=dtype)
        print(f"Data successfully written to table {table_name}")

    except Exception as e:
        print(f"Failed to write data to table {table_name}: {e}")
        raise

@task
def save_first_row_to_csv(df, file_name):
    """Save the first row of a DataFrame to a CSV file."""
    first_row = df.head(1)
    first_row.to_csv(file_name, index=False)
    print(f"First row saved to {file_name}")

@flow
def etl_flow():
    # Extract data
    luchthavens = load_data('cleansed', 'luchthavens', engine)
    vliegtuig = load_data('cleansed', 'vliegtuig', engine)
    vliegtuigtype = load_data('cleansed', 'vliegtuigtype', engine)
    weer = load_data('cleansed', 'weer', engine)
    maatschappijen = load_data('cleansed', 'maatschappijen', engine)
    aankomst = load_data('cleansed', 'aankomst', engine)
    vertrek = load_data('cleansed', 'vertrek', engine)
    vlucht = load_data('cleansed', 'vlucht', engine)
    klant = load_data('cleansed', 'klant', engine)

    # Combine and clean data
    combined_data = combine_and_clean_data(luchthavens, vliegtuig, vliegtuigtype, weer, maatschappijen, aankomst, vertrek, vlucht, klant)

    # Remove duplicates from vliegtuig_dim
    deduplicated_vliegtuig_dim = remove_duplicates(combined_data['vliegtuig_dim'], subset=['vliegtuigcode'])
    combined_data['vliegtuig_dim'] = deduplicated_vliegtuig_dim

    # Print the cleaned and deduplicated DataFrame for verification
    print("vliegtuig_dim after deduplication:", combined_data['vliegtuig_dim'])

    # Save the first row of vlucht_fct to a CSV file
    save_first_row_to_csv(combined_data['vlucht_fct'], 'first_row_vlucht_fct.csv')

    # Load data into data warehouse
    write_to_dw(combined_data['luchthaven_dim'], 'luchthaven_dim', engine)
    write_to_dw(combined_data['vliegtuig_dim'], 'vliegtuig_dim', engine)
    write_to_dw(combined_data['weer_dim'], 'weer_dim', engine)
    write_to_dw(combined_data['maatschappij_dim'], 'maatschappij_dim', engine)
    write_to_dw(combined_data['vlucht_fct'], 'vlucht_fct', engine)
    write_to_dw(combined_data['klant_dim'], 'klant_dim', engine, dtype={
        'operatie': sqlalchemytypes.DECIMAL(2, 1),
        'faciliteiten': sqlalchemytypes.DECIMAL(2, 1),
        'shops': sqlalchemytypes.DECIMAL(2, 1)
    })

if __name__ == '__main__':
    etl_flow()
