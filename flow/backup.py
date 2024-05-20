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
def save_first_row_to_csv(df, file_name):
    """Save the first row of a DataFrame to a CSV file."""
    first_row = df
    first_row.to_csv(file_name, index=False)
    print(f"First row saved to {file_name}")

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
    vlucht['vluchtid'] = vlucht['vluchtid'].str.strip()
    aankomst['vluchtid'] = aankomst['vluchtid'].str.strip()
    vertrek['vluchtid'] = vertrek['vluchtid'].str.strip()

    # Combine vliegtuig and vliegtuigtype data
    vliegtuig_dim = vliegtuig.merge(vliegtuigtype, how='left', left_on='vliegtuigtype', right_on='iata')
    vliegtuig_dim = vliegtuig_dim[['airlinecode', 'vliegtuigcode', 'vliegtuigtype', 'bouwjaar', 
                                   'merk', 'type', 'wake', 'cat', 'capaciteit', 'vracht']].drop_duplicates(subset=['vliegtuigcode'])

    # Convert columns to correct dtypes
    vliegtuig_dim['capaciteit'] = pd.to_numeric(vliegtuig_dim['capaciteit'], errors='coerce')
    vliegtuig_dim['vracht'] = pd.to_numeric(vliegtuig_dim['vracht'], errors='coerce')
    
    # Copy the necessary columns from vlucht
    vlucht_fct = vlucht[['vluchtid', 'vluchtnr', 'airlinecode', 'destcode', 'vliegtuigcode']].copy()
    vlucht_fct.rename(columns={'airlinecode': 'maatschappij_id'}, inplace=True)

    # Filter rows where airlinecode is exactly 2 characters long
    vlucht_fct = vlucht_fct[vlucht_fct['maatschappij_id'].str.len() == 2]
    print("Print 1", vlucht_fct)
    # Filter rows where airlinecode is present in maatschappij_dim
    valid_airlinecodes = maatschappijen['iata'].tolist()
    vlucht_fct = vlucht_fct[vlucht_fct['maatschappij_id'].isin(valid_airlinecodes)]
    print("Print 2", vlucht_fct)

    # Debug: Check contents and types of aankomst DataFrame
    print("Contents of aankomst DataFrame:")
    print(aankomst.head())
    print("Data types of aankomst DataFrame:")
    print(aankomst.dtypes)

    # Merge aankomst with vlucht_fct on vluchtid
    vlucht_fct = vlucht_fct.merge(aankomst[['vluchtid', 'bezetting', 'aankomsttijd']], on='vluchtid', how='left')
    print("Print 3 aankomst merge", vlucht_fct)
    
    # Save first row of merged DataFrame to CSV
    save_first_row_to_csv(vlucht_fct, "first_row_vlucht_fct_after_aankomst_merge.csv")

    # Debug: Check contents and types of vertrek DataFrame
    print("Contents of vertrek DataFrame:")
    print(vertrek.head())
    print("Data types of vertrek DataFrame:")
    print(vertrek.dtypes)

    # Merge vertrek with vlucht_fct on vluchtid
    vlucht_fct = vlucht_fct.merge(vertrek[['vluchtid', 'vertrektijd']], on='vluchtid', how='left')
    print("Print 4 vertrek merge", vlucht_fct)
    
    # Save first row of merged DataFrame to CSV
    save_first_row_to_csv(vlucht_fct, "first_row_vlucht_fct_after_vertrek_merge.csv")

    # Fill missing aankomsttijd and bezetting when vertrektijd is not null
    vlucht_fct['aankomsttijd'] = vlucht_fct.apply(
        lambda row: pd.to_datetime(row['vertrektijd']).strftime('%Y-%m-%d') if pd.isnull(row['aankomsttijd']) and not pd.isnull(row['vertrektijd']) else row['aankomsttijd'],
        axis=1
    )
    vlucht_fct['bezetting'] = vlucht_fct.apply(
        lambda row: 0 if pd.isnull(row['bezetting']) and not pd.isnull(row['vertrektijd']) else row['bezetting'],
        axis=1
    )

    # Fill missing vertrektijd when aankomsttijd is not null
    vlucht_fct['vertrektijd'] = vlucht_fct.apply(
        lambda row: pd.to_datetime(row['aankomsttijd']).strftime('%Y-%m-%d') if pd.isnull(row['vertrektijd']) and not pd.isnull(row['aankomsttijd']) else row['vertrektijd'],
        axis=1
    )

    # Save first row of DataFrame after filling missing values
    save_first_row_to_csv(vlucht_fct, "first_row_vlucht_fct_after_filling_missing_values.csv")

    # Ensure aankomsttijd is in datetime format before converting to string for comparison
    vlucht_fct['aankomsttijd'] = pd.to_datetime(vlucht_fct['aankomsttijd'], errors='coerce')

    # Convert aankomsttijd to date string for comparison
    vlucht_fct['aankomstdatum'] = vlucht_fct['aankomsttijd'].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else None)

    # Convert weer datum to date string for comparison
    weer['datum'] = pd.to_datetime(weer['datum'], errors='coerce').dt.strftime('%Y-%m-%d')

    # Merge with weer to get weer_id
    vlucht_fct = vlucht_fct.merge(weer[['datum']], how='left', left_on='aankomstdatum', right_on='datum')
    print("Print 5 Merge with weer to get weer_id", vlucht_fct)
    vlucht_fct.rename(columns={'datum': 'weer_id'}, inplace=True)
    print("Print 6 Merge with weer to get weer_id", vlucht_fct)

    # Drop the helper columns used for merging
    vlucht_fct.drop(columns=['aankomstdatum'], inplace=True)

    # Save first row of DataFrame after adding weer_id
    save_first_row_to_csv(vlucht_fct, "first_row_vlucht_fct_after_adding_weer_id.csv")

    # Add dest_luchthaven_id to vlucht_fct
    vlucht_fct = vlucht_fct.merge(luchthavens[['iata']], how='left', left_on='destcode', right_on='iata')
    vlucht_fct.rename(columns={'iata': 'dest_luchthaven_id'}, inplace=True)
    print("Print 7 Added dest_luchthaven_id", vlucht_fct)

    # Save first row of DataFrame after adding dest_luchthaven_id
    save_first_row_to_csv(vlucht_fct, "first_row_vlucht_fct_after_adding_dest_luchthaven_id.csv")

    # Select only necessary columns and remove duplicates for luchthaven_dim
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