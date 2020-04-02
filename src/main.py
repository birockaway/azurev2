from azure.storage.blob import BlockBlobService, ContentSettings
from azure.storage.blob.baseblobservice import BaseBlobService
from keboola import docker  # pro komunikaci s parametry a input/output mapping
import os
from datetime import datetime
import csv
import re


def log(message, level="INFO"):
    timestamp = datetime.now().strftime("%Y%m%d %H:%M:%S.%f")
    print(f"{timestamp} - {level} - {message}")


def get_table_timestamp_from_data(table_path, date_column) -> str:
    """
    Extracts digits from timestamp, finds maximum.
    """
    with open(table_path, newline='\n', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        dates = {
            int(''.join(re.findall(r'\d+', row[date_column]))) for row in reader
        }
    max_date = max(dates)
    added_zeros = 14 - len(str(max_date)[:14])
    table_timestamp = str(max_date)[:14] + added_zeros * '0'
    return table_timestamp


def write_csv_to_azure(block_blob_service, azure_container, table_path,
                       timestamp_suffix, date_column):
    """
    Writes csv to azure bloc container.
    If specified, appends timestamp to the filename.
    """
    if timestamp_suffix and date_column is not None:
        timestamp = get_table_timestamp_from_data(table_path, date_column)

    elif timestamp_suffix:
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    else:
        timestamp = ''
    log(f'Adding timestamp: {timestamp}')

    # e.g. /data/in/tables/table_name.csv
    table_name = table_path.split('/')[-1].split('.')[0]

    block_blob_service.create_blob_from_path(
        azure_container,
        table_name + '-' + timestamp + '.csv',
        table_path,
        content_settings=ContentSettings(content_type='application/CSV'),
    )

    return None


if __name__ == '__main__':
    # get KBC parameters
    kbc_datadir = os.environ.get('KBC_DATADIR', '/data/')
    cfg = docker.Config(kbc_datadir)
    parameters = cfg.get_parameters()
    # loads application parameters - user defined
    account_key = parameters.get('#account_key')
    account_name = parameters.get('account_name')
    data_container = parameters.get('data_container')
    config_container = parameters.get('config_container')
    add_timestamp_suffix = parameters.get('add_timestamp_suffix')
    date_col = parameters.get('date_col')
    log('Parameters loaded.')

    block_bs = BlockBlobService(account_name=account_name, account_key=account_key)
    base_bs = BaseBlobService(account_name=account_name, account_key=account_key)
    log(f'Docker cointainer will try to connect to {account_name} account of BlockBlobService...')

    tables_to_write = [
        f'{kbc_datadir}in/tables/{path}' for path in
        os.listdir(f'{kbc_datadir}in/tables/') if path.endswith('.csv')
    ]

    log(f'Tables to be uploaded: {tables_to_write}')

    for tablepath in tables_to_write:
        log(f'Writing table {tablepath}')
        try:
            write_csv_to_azure(block_blob_service=block_bs, azure_container=data_container,
                               table_path=tablepath, timestamp_suffix=add_timestamp_suffix,
                               date_column=date_col)
            log(f'Table {tablepath} successfuly written.')
        except Exception as e:
            log(f'Failed to write table {tablepath}')

    log('Finished')
