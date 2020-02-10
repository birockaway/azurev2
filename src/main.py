from azure.storage.blob import BlockBlobService, ContentSettings
from azure.storage.blob.baseblobservice import BaseBlobService
from keboola import docker  # pro komunikaci s parametrama a input/output mapping
import os
from datetime import datetime
import pandas as pd
import json


def log(message, level="INFO"):
    timestamp = datetime.now().strftime("%Y%m%d %H:%M:%S.%f")
    print(f"{timestamp} - {level} - {message}")


def download_config(base_blob_service, config_container, table_name):
    config_name = table_name + '.config'
    config_path = out_data_dir + config_name
    try:
        base_blob_service.get_blob_to_path(config_container, config_name, config_path)
    except:
        brand_new_config = {'latest': '19700101'}
        with open(config_path, 'w') as outfile:
            json.dump(brand_new_config, outfile)


# Upload the CSV file to Azure cloud
def write_table(block_blob_service, data_container, tables_folder,
                table_name, table_name_suffix='', timestamp_suffix=False):
    """
    write the table to blob storage.
    """
    if str(timestamp_suffix).lower() == 'true':
        tmp_table = pd.read_csv(tables_folder + table_name + '.csv', usecols=[date_col], parse_dates=[date_col])
        max_timestamp = max(int(timeint) for timeint
                            in list(pd.unique(tmp_table[date_col].dt.strftime('%Y%m%d%H%M%S'))))
        table_name_suffix = f'{table_name_suffix}_ {max_timestamp}'

    block_blob_service.create_blob_from_path(
        data_container,
        table_name + table_name_suffix + '.csv',
        tables_folder + table_name + '.csv',
        content_settings=ContentSettings(content_type='application/CSV'),
    )


def write_new_config(block_blob_service, data_container, config_folder, table_name):
    """
	write the config to blob storage.
	"""
    block_blob_service.create_blob_from_path(
        data_container,
        table_name + '.config',
        config_folder + table_name + '.config',
        content_settings=ContentSettings(content_type='application/CSV'),
    )


def cut_table_by_dates(table_name, latest_date):
    result_files_dict = {}
    for chunk, data_df in enumerate(pd.read_csv(
            in_tables_dir + table_name + '.csv',
            chunksize=5_000_000,
            parse_dates=[date_col],
    )):
        log(f'Getting chunk No. {chunk}...')
        dates = list(pd.unique(data_df[date_col].dt.strftime('%Y%m%d')))

        new_dates = [date for date in dates if int(date) > int(latest_date)]

        for new_date in new_dates:
            table_name_and_date = f'{table_name}_{new_date}'
            if table_name_and_date not in result_files_dict.keys():
                result_files_dict[table_name_and_date] = open(f'{out_tables_dir}{table_name_and_date}.csv', 'a+')
            new_data_df = data_df[new_date == data_df[date_col].dt.strftime('%Y%m%d')]
            new_data_df.to_csv(result_files_dict[table_name_and_date], index=False,
                               header=result_files_dict[table_name_and_date].tell()==0)

        for conn in result_files_dict.values():
            conn.close()


def get_new_last_date(table_name, tables_dir):
    date_suffixes = [
        x.split('_')[-1].replace('.csv', '')
        for x in os.listdir(out_tables_dir)
        if x.endswith('.csv') and x.startswith(table_name)
    ]

    if len(date_suffixes) == 0:
        log(f'No new dates in table {table_name}.')
        return None
    max_date = max([int(s) for s in date_suffixes])
    return str(max_date)


def get_latest_date_from_config_file(file_path):
    with open(file_path) as f:
        config = json.load(f)

    return config['latest']


def update_config_file(file_path, new_last_date):
    with open(file_path) as f:
        config = json.load(f)
    config['latest'] = new_last_date

    with open(file_path, 'w') as outfile:
        json.dump(config, outfile)


def write_table_list_to_azure(block_blob_service, data_container,
                              tables_folder, table_name_list, add_timestamp_suffix=False):
    for table_name in table_name_list:
        try:
            write_table(block_blob_service, data_container, tables_folder, table_name,
                        timestamp_suffix=add_timestamp_suffix)
            log(
                f'Table {table_name} successfully uploaded to {data_container} storage container of BlockBlobService...'
            )
        except Exception as e:
            log(f'Something went wrong during {table_name} table upload...', level='WARNING')
            log(f'Exception: {str(e)}', level='WARNING')


# get KBC parameters
kbc_datadir = os.environ.get('KBC_DATADIR')
cfg = docker.Config(kbc_datadir)
parameters = cfg.get_parameters()
# loads application parameters - user defined
account_key = parameters.get('account_key')
account_name = parameters.get('account_name')
data_container = parameters.get('data_container')
config_container = parameters.get('config_container')
add_timestamp_suffix = parameters.get('add_timestamp_suffix')
upload_all = parameters.get('upload_all')
date_col = parameters.get('date_col')
log('Parameters loaded.')

# when date_col is not in params, set to default value
if not date_col:
    date_col = 'date'

tempdir = f'{kbc_datadir}temp_fileprep/'
# create directory to store temporary files
in_tables_dir = f'{kbc_datadir}in/tables/'
out_tables_dir = f'{tempdir}tables/'
out_data_dir = f'{tempdir}'

os.makedirs(tempdir)
os.makedirs(out_tables_dir)

block_blob_service = BlockBlobService(account_name=account_name, account_key=account_key)
base_blob_service = BaseBlobService(account_name=account_name, account_key=account_key)
log(f'Docker cointainer will try to connect to {account_name} account of BlockBlobService...')

# TODO:
# Create the container if it does not exist.
# block_blob_service.create_container(data_container)

in_tables_list = [os.path.splitext(i)[0] for i in
                  os.listdir(in_tables_dir) if i.endswith('.csv')
                  ]

log(f'Tables to be uploaded: {in_tables_list}')
log(f'Uploading tables {in_tables_list} to {data_container} storage container of BlockBlobService...')

if not config_container or str(upload_all) == 'true':
    write_table_list_to_azure(block_blob_service, data_container,
                              in_tables_dir, in_tables_list, add_timestamp_suffix=add_timestamp_suffix)
else:
    # Expand in tables & update config
    for table_name in in_tables_list:
        try:
            config_file_path = out_data_dir + table_name + '.config'
            download_config(base_blob_service, config_container, table_name)
            latest_date = get_latest_date_from_config_file(config_file_path)
            cut_table_by_dates(table_name, latest_date)
            new_last_date = get_new_last_date(table_name, tables_dir=out_tables_dir)
            log(f'New last date is: {new_last_date}')
            update_config_file(config_file_path, new_last_date)
            write_new_config(
                block_blob_service, config_container, out_data_dir, table_name)
            log(
                f'Config for {table_name} successfully uploaded to {config_container} storage container of BlockBlobService...'
                )
        except Exception as e:
            log(f'Something went wrong during {table_name} table upload...')
            log(f'Exception: {str(e)}')

    # Get out tables list
    out_tables_list = [
        os.path.splitext(i)[0]
        for i in os.listdir(out_tables_dir)
        if i.endswith('.csv')
    ]
    log(f'Tables to be written: {out_tables_list}')

    # Write expanded out tables to Azure
    write_table_list_to_azure(
        block_blob_service, data_container, out_tables_dir, out_tables_list
    )

log('Job finished.')
