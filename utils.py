#!/usr/local/bin/python
# -*- coding: utf-8 -*-
import csv
import datetime
import os
import random
import time
from datetime import datetime, timedelta
from glob import glob
from zipfile import ZipFile, error

import dask.dataframe as dd
import numpy as np
import pandas as pd
import requests
import scraperwiki
from bizdays import Calendar, load_holidays
from pandas.core.arrays.sparse import dtype
from sqlalchemy import create_engine
from tqdm import tqdm

from layout_b3 import LayoutB3

os.environ['SCRAPERWIKI_DATABASE_NAME'] = 'sqlite:///data.sqlite'


def load_useragents():
    uas = []
    with open("user-agents.txt", 'rb') as uaf:
        for ua in uaf.readlines():
            if ua:
                uas.append(ua.strip()[0:-1-0])
    random.shuffle(uas)
    return uas


def check_download(dt_referencia, file_name):
    if not isbizday(dt_referencia):
        print(dt_referencia, 'não é dia útil')
        return False
    if os.path.exists(file_name):
        print(file_name, 'arquivo já baixado')
        return False
    return True


def download(url, params, file_name):
    headers = {'User-Agent': random.choice(load_useragents())}
    response = requests.get(url, params=params, stream=True, headers=headers)
    if response.status_code != 200:
        'Nenhum arquivo encontrado nessa url'
        return False
    with open(file_name, "wb") as handle:
        for data in tqdm(response.iter_content()):
            handle.write(data)
    handle.close()


def get_ultima_data_disponivel_base(path_file_base):
    # verifica a última data disponível na base
    with open(path_file_base, 'r') as f:
        for row in reversed(list(csv.reader(f))):
            data = row[0].split(';')[0]
            if data == 'dt_referencia':
                return None
            data = row[0].split(';')[0]
            return datetime.datetime.strptime(data, '%Y-%m-%d').date()


def generate_xlsx_base(df, path_saida):
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pd.ExcelWriter(path_saida, engine='xlsxwriter')
    # Convert the dataframe to an XlsxWriter Excel object.
    df.to_excel(writer, sheet_name='Sheet1')
    # Close the Pandas Excel writer and output the Excel file.
    writer.save()


def xrange(x):
    return iter(range(x))


def datetime_range(start=None, end=None):
    span = end - start
    for i in xrange(span.days + 1):
        yield start + timedelta(days=i)


def remove_zero_files(folder_name):
    file_list = os.listdir(r"downloads/"+folder_name+"/")
    for file_name in file_list:
        if not file_name.endswith('.csv'):
            continue
        path_file = os.path.join('downloads', folder_name, file_name)
        with open(path_file, 'r', encoding='latin1') as f:
            first_line = f.readline()
            if 'Não há dados disponíveis' in first_line or 'error' in first_line or '<' in first_line:
                os.remove(path_file)


def generate_csv_base(path_file_base):
    # organizar o arquivo base por dt_referencia
    df = pd.read_csv(path_file_base, sep=';')
    df = df.sort_values('dt_referencia')
    # set the index
    df.set_index('dt_referencia', inplace=True)
    df.to_csv(path_file_base, sep=';')


def get_calendar():
    holidays = load_holidays(os.path.join('ANBIMA.txt'))
    return Calendar(holidays, ['Sunday', 'Saturday'])


def isbizday(dt_referencia):
    cal = get_calendar()
    return cal.isbizday(dt_referencia)


def get_ultima_data_base(path_file_base):
    ultima_data_base = get_ultima_data_disponivel_base(path_file_base)
    print('Última data base disponível:', ultima_data_base)
    if (ultima_data_base is None):
        return datetime.date(2015, 1, 1)
    return ultima_data_base


def prepare_download_folder(name_download_folder):
    path_download = os.path.join(name_download_folder)
    if not os.path.exists(path_download):
        os.makedirs(path_download)
    return path_download


def extract_file(path_file):
    with ZipFile(path_file, 'r') as zipObj:
        # Get a list of all archived file names from the zip
        list_of_file_names = zipObj.namelist()
        # Iterate over the file names
        for file_name in list_of_file_names:
            if file_name.startswith('COTAHIST.A'):
                print('Extracting', file_name)
                zipObj.extract(file_name, path='downloads')


def descompactar_arquivos_zip(download_path, extraidos_path):
    """
    Descompacta os arquivos zip.
    """
    for path_to_zip_file in sorted(glob('%s/*.zip' % download_path)):
        print('Extraindo zip files folder', path_to_zip_file)
        with ZipFile(path_to_zip_file, 'r') as zip_ref:
            zip_ref.extractall(extraidos_path)
        # os.remove(path_to_zip_file)


def gerar_arquivo_final(extraidos_path, base_path):
    layout = LayoutB3()

    for file_name in os.listdir(extraidos_path):
        file_path = os.path.join(extraidos_path, file_name)

        print('Importando arquivo', file_path)

        reader = pd.read_fwf(
            file_path,
            colspecs=layout.get_posicoes(),
            skiprows=1,
            names=layout.get_campos(),
            encoding='latin1',
            dtype={'PRAZOT': 'object'},
            low_memory=True,
            chunksize=75000
        )

        for df in reader:
            df['TIPREG'] = df['TIPREG']
            df['DATA'] = df['DATA']
            df['CODBDI'] = df['CODBDI'].astype(str)
            df['CODNEG'] = df['CODNEG'].astype(str)
            df['TPMERC'] = df['TPMERC']
            df['NOMRES'] = df['NOMRES'].astype(str)
            df['ESPECI'] = df['ESPECI'].astype(str)
            df['PRAZOT'] = df['PRAZOT'].astype(str)
            df['MODREF'] = df['MODREF'].astype(str)
            df['PREABE'] = df['PREABE'].astype(float)
            df['PREMAX'] = df['PREMAX'].astype(float)
            df['PREMIN'] = df['PREMIN'].astype(float)
            df['PREMED'] = df['PREMED'].astype(float)
            df['PREULT'] = df['PREULT'].astype(float)
            df['PREOFC'] = df['PREOFC'].astype(float)
            df['PREOFV'] = df['PREOFV'].astype(float)
            df['TOTNEG'] = df['TOTNEG']
            df['QUATOT'] = df['QUATOT']
            df['VOLTOT'] = df['VOLTOT'].astype(float)
            df['PREEXE'] = df['PREEXE'].astype(float)
            df['INDOPC'] = df['INDOPC']
            df['DATVEN'] = df['DATVEN'].astype(str)
            df['FATCOT'] = df['FATCOT']
            df['PTOEXE'] = df['PTOEXE'].astype(float)
            df['CODISI'] = df['CODISI'].astype(str)
            df['DISMES'] = df['DISMES']

            # Converte campo de data
            df['DATA'] = pd.to_datetime(
                df['DATA'], format='%Y%m%d', errors='coerce'
            ).dt.date

            # Converte campo de data
            df['DATVEN'] = pd.to_datetime(
                df['DATVEN'], format='%Y%m%d', errors='coerce'
            )

            # remove os registros de filler
            selecao = df['TIPREG'] == 99
            df = df[~selecao]

            print('Importando para a base scraperwiki')
            import_scraperwiki(df)


def import_scraperwiki(df):
    keys = [
        'TIPREG',
        'DATA',
        'CODBDI',
        'CODNEG',
        'TPMERC',
        'CODISI'
    ]

    print('Salvando csv de saída', len(df), 'registros')
    df.to_csv('base_completa.csv', index=False, mode='a')

    engine = create_engine('sqlite:///data.sqlite', echo=True)
    sqlite_connection = engine.connect()
    print('Importando usando pandas to_sql')
    df.to_sql(
        'swdata',
        sqlite_connection,
        if_exists='append',
        index=False
    )

    return True

    for row in enumerate(df.to_dict('records')):
        try:
            scraperwiki.sqlite.save(unique_keys=keys, data=row)
        except Exception as e:
            print("Error occurred:", e)


def reduce_mem_usage(props):
    start_mem_usg = props.memory_usage().sum() / 1024**2
    print("Memory usage of properties dataframe is :", start_mem_usg, " MB")
    NAlist = []  # Keeps track of columns that have missing values filled in.
    for col in props.columns:
        if props[col].dtype != object:  # Exclude strings

            # Print current column type
            print("******************************")
            print("Column: ", col)
            print("dtype before: ", props[col].dtype)

            # make variables for Int, max and min
            IsInt = False
            mx = props[col].max()
            mn = props[col].min()

            # Integer does not support NA, therefore, NA needs to be filled
            if not np.isfinite(props[col]).all():
                NAlist.append(col)
                props[col].fillna(mn-1, inplace=True)

            # test if column can be converted to an integer
            asint = props[col].fillna(0).astype(np.int64)
            result = (props[col] - asint)
            result = result.sum()
            if result > -0.01 and result < 0.01:
                IsInt = True

            # Make Integer/unsigned Integer datatypes
            if IsInt:
                if mn >= 0:
                    if mx < 255:
                        props[col] = props[col].astype(np.uint8)
                    elif mx < 65535:
                        props[col] = props[col].astype(np.uint16)
                    elif mx < 4294967295:
                        props[col] = props[col].astype(np.uint32)
                    else:
                        props[col] = props[col].astype(np.uint64)
                else:
                    if mn > np.iinfo(np.int8).min and mx < np.iinfo(np.int8).max:
                        props[col] = props[col].astype(np.int8)
                    elif mn > np.iinfo(np.int16).min and mx < np.iinfo(np.int16).max:
                        props[col] = props[col].astype(np.int16)
                    elif mn > np.iinfo(np.int32).min and mx < np.iinfo(np.int32).max:
                        props[col] = props[col].astype(np.int32)
                    elif mn > np.iinfo(np.int64).min and mx < np.iinfo(np.int64).max:
                        props[col] = props[col].astype(np.int64)

            # Make float datatypes 32 bit
            else:
                props[col] = props[col].astype(np.float32)

            # Print new column type
            print("dtype after: ", props[col].dtype)
            print("******************************")

    # Print final result
    print("___MEMORY USAGE AFTER COMPLETION:___")
    mem_usg = props.memory_usage().sum() / 1024**2
    print("Memory usage is: ", mem_usg, " MB")
    print("This is ", 100*mem_usg/start_mem_usg, "% of the initial size")
    return props, NAlist
