import os
import shutil
import time
from datetime import datetime
from itertools import islice

import zipfile
from dbfread import DBF

import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.keys import Keys


url = 'http://stat.customs.gov.ru/unload'

def init_driver(driver_path, url):
    '''
    Function that initializes the web-driver and set to TSVT unload page.
    '''
    driver = webdriver.Chrome(os.path.join(driver_path, 'chromedriver.exe'))
    driver.get(url)
    return driver


def time_filters():
    '''
    Function to let a user choose months to download.
    Returns dictionary with character and numeric representation of time filter.
    '''
    months_dict = {
        1: 'январь',
        2: 'февраль',
        3: 'март',
        4: 'апрель',
        5: 'май',
        6: 'июнь',
        7: 'июль',
        8: 'август',
        9: 'сентябрь',
        10: 'октябрь',
        11: 'ноябрь',
        12: 'декабрь',
    }

    all_years = [
        year for year in range(datetime.now().year - 3, datetime.now().year + 1)
    ]

    allm_dict = {
        str(month) + str(year):' '.join([months_dict[month], str(year), 'г.'])
        for year in all_years
            for month in months_dict
    }

    shortcut = int(input(
        '''
Setting filters for time period.
Please, enter the corresponding number to choose one of the options:
1 - download the last available month only
2 - enter time interval manually

        '''
    ))

    if shortcut == 1:
        if datetime.now().month > 2:
            m = str(datetime.now().month - 2) + str(datetime.now().year)
            return {m: allm_dict[m]}

        m = str(12 + datetime.now().month - 2) + str(datetime.now().year - 1)
        return {m: allm_dict[m]}

    elif shortcut == 2:
        period_input = input(
            '''
Please, enter months or time interval needed in the following form:
M(M).YYYY, M(M).YYYY
or
M(M).YYYY-M(M).YYYY

            '''
        )

        if '.' in period_input and len(period_input) in [6, 7]:
            m = ''.join(period_input.split('.'))
            return {m: allm_dict[m]}

        elif ',' in period_input:
            subset = [''.join(period.split('.')) for period in period_input.split(', ')]
            return {m_short: m_long for m_short, m_long in allm_dict.items() if m_short in subset}

        start, end = [''.join(period.split('.')) for period in period_input.split('-')]
        return dict(
            islice(
                allm_dict.items(),
                list(allm_dict.keys()).index(start),
                list(allm_dict.keys()).index(end) + 1
            )
        )

    return self.get_time_filters()


def download_stat(driver, month, downloads):
    '''
    Downloads the statistics from TSVT unload page using selenium webdriver.
    Sends periods recieved from time_filters function as filters.
    After unload renames an archive and moves it to the destination storage folder.
    '''
    level = driver.find_element_by_xpath('//div/input[@id="react-select-tnvedLevelsSelect-input"]')
    level.send_keys('10 знаков')
    level.send_keys(Keys.RETURN)

    time.sleep(1)

    period = driver.find_element_by_xpath('//div/input[@id="react-select-periodSelect-input"]')
    period.send_keys(month)
    period.send_keys(Keys.RETURN)

    time.sleep(1)

    while not os.path.exists(os.path.join(downloads, 'DATTSVT.dbf.zip')):
        submit_button = driver.find_element_by_xpath('//button[@type="submit"]')
        submit_button.click()
        time.sleep(10)

    remove_filters = driver.find_element_by_xpath(
        '//button[@class="ButtonLink__buttonLink' + \
        ' ButtonLink__buttonLink_color_green' + \
        ' buttonLink_size_normal buttonLink_theme_default"]'
    )
    remove_filters.click()


def move_unzip_rename(month, downloads, dest_folder):
    '''
    Function to move zip archive to the destination folder after downloading.
    Ten, unzips and renames it.
    '''
    file = os.path.join(downloads, 'DATTSVT.dbf.zip')
    new_file = f'TSVTdata_{month}.zip'

    shutil.move(
        os.path.join(downloads, file),
        os.path.join(dest_folder, file)
    )

    with zipfile.ZipFile(os.path.join(dest_folder, file), 'r') as zip_file:
        zip_info = zip_file.getinfo('DATTSVT.dbf')
        zip_info.filename = f'TSVTdata_{month}.dbf'
        zip_file.extract(zip_info, path=dest_folder)

    os.rename(
        os.path.join(dest_folder, file),
        os.path.join(dest_folder, new_file)
    )


def dbf_to_csv(dest_folder):
    '''
    Converts dbf from zip archive to csv.
    '''
    file = [file for file in os.listdir(dest_folder) if file.endswith('dbf')][0]
    new_file = '.'.join([file.split('.')[0], 'csv'])
    data = [row for row in DBF(os.path.join(dest_folder, file), encoding='cp866')]
    df = pd.DataFrame(data)
    df.to_csv(os.path.join(dest_folder, new_file), index=False, encoding='utf-8')

    to_remove = [file for file in os.listdir(dest_folder) if '.csv' not in file]
    for file in to_remove:
        os.remove(os.path.join(dest_folder, file))

    return df


def prepare_stat(df, dest_folder):
    '''
    Converts dtypes, aggregates by Russia.
    '''
    for col in ['Stoim', 'Netto', 'Kol']:
        df[col] = df[col].astype(str).str.replace(',', '.').astype(float)

    df = (
        df
        .groupby(['period', 'napr', 'strana', 'tnved'])
        .sum()
        .loc[:, ['Stoim', 'Netto', 'Kol']]
        .reset_index()
    )

    return df


def add_info_aggregate(df, dest_folder):
    '''
    Merges with country groups and branches.
    Adds group subtotal by 2, 4 and 6 digits to main dataframe.
    '''
    country_groups = pd.read_excel('ФТС блоки стран.xlsx', keep_default_na=False)
    branches = pd.read_excel('ФТС отрасли.xlsx')

    df['tnved'] = df.tnved.astype(str)
    df.loc[df.tnved.str.len() == 9, 'tnved'] = \
        df.loc[df.tnved.str.len() == 9, 'tnved'].apply(lambda x: '0' + x)

    df['tnved2'] = df.tnved.astype(str).apply(lambda x: x[:2])
    df['tnved4'] = df.tnved.astype(str).apply(lambda x: x[:4])
    df['tnved6'] = df.tnved.astype(str).apply(lambda x: x[:6])
    df = df.rename({'tnved': 'tnved10'}, axis=1)

    df = (
        df
        .merge(
            country_groups.loc[:, ['KOD', 'Блок_стран']],
            how='left',
            left_on='strana',
            right_on='KOD'
        )
        .merge(
            branches.loc[:, ['KOD4', 'GPB-BRANCH-NEW']],
            how='left',
            left_on='tnved4',
            right_on='KOD4'
        )
    ).rename({'Блок_стран': 'country_groups', 'GPB-BRANCH-NEW': 'branch'}, axis=1)
    df = df.drop([col for col in df.columns if 'KOD' in col], axis=1)

    groupby2 = df.groupby(
        ['period', 'napr', 'strana', 'country_groups', 'branch', 'tnved2']
    ).sum().loc[:, ['Stoim', 'Netto', 'Kol']].reset_index().rename({'tnved2': 'tnved'}, axis=1)

    groupby4 = df.groupby(
        ['period', 'napr', 'strana', 'country_groups', 'branch', 'tnved4']
    ).sum().loc[:, ['Stoim', 'Netto', 'Kol']].reset_index().rename({'tnved4': 'tnved'}, axis=1)

    groupby6 = df.groupby(
        ['period', 'napr', 'strana', 'country_groups', 'branch', 'tnved6']
    ).sum().loc[:, ['Stoim', 'Netto', 'Kol']].reset_index().rename({'tnved6': 'tnved'}, axis=1)

    df = df.drop(['tnved2', 'tnved4', 'tnved6'], axis=1).rename({'tnved10': 'tnved'}, axis=1)

    for grouped_df in [groupby2, groupby4, groupby6]:
        df = df.append(grouped_df).reset_index(drop=True)

    df['n_digits'] = df.tnved.str.len()

    return df


def encode_labels(df, dest_folder):
    '''
    Encodes character columns to codes with corresponding dictionaries.
    '''
    countries = pd.read_excel('ФТС блоки стран.xlsx', keep_default_na=False)
    country_dict = {
        row[1]['KOD']: row[1]['CTPAHA_CODE'] for row in countries.iterrows()
    }
    df['strana'] = df.strana.replace(country_dict)

    country_groups = pd.read_csv('country_groups.csv', keep_default_na=False)
    country_groups_dict = {
        row[1].country_group: row[1].group_code for row in country_groups.iterrows()
    }
    df['country_groups'] = df['country_groups'].replace(country_groups_dict)

    unique_branch = pd.read_csv('branches.csv', keep_default_na=False)
    unique_branch_dict = {
        row[1].branch: row[1].branch_code for row in unique_branch.iterrows()
    }
    df['branch'] = df['branch'].replace(unique_branch_dict)

    df['napr'] = df.napr.replace({'ИМ': 1, 'ЭК': 2})

    periods = pd.read_csv('periods.csv')
    periods_dict = {
        row[1].period: row[1].period_code for row in periods.iterrows()
    }
    df['period'] = df.period.replace(periods_dict)

    return df


def upload_to_file(df, path):
    '''
    Uploads processed dataframe to an existing file with all data.
    Creates a new one if there is no file for storage.
    '''
    if os.path.exists(os.path.join(path, 'TSVTdata.csv')):
        df.to_csv(os.path.join(path, 'TSVTdata.csv'), mode='a', index=False, header=False)
    else:
        df.to_csv(os.path.join(path, 'TSVTdata.csv'), index=False)
