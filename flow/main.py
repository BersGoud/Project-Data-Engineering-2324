from raw_importeren import import_data_raw
from raw_to_archive_cleansed import preprocess_data_flow
from cleansed_to_dw import etl_flow
from datawarehouse_to_s3 import export_and_upload_tables

def run_project():
    import_data_raw()
    preprocess_data_flow()
    etl_flow()
    export_and_upload_tables()
    

if __name__ == '__main__':
    run_project()