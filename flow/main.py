from raw_importeren import import_data_raw
from raw_to_archive_cleansed import preprocess_data_flow
from cleansed_to_dw import etl_flow

def run_project():
    import_data_raw()
    preprocess_data_flow()
    etl_flow()
    

if __name__ == '__main__':
    run_project()