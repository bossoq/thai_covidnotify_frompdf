import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import requests
import tabula
import minecart
from PyPDF2 import PdfFileReader
import os
import time
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from pandas.plotting import table

today = datetime.now().date()
file_date = str(today.day) + "{:02d}".format(today.month) + str(today.year + 543 - 2500)
file = 'https://media.thaigov.go.th/uploads/public_img/source/'+file_date+'.pdf'
cwd = os.path.dirname(os.path.abspath(__file__))

database = pd.read_csv(cwd+'/covid_data.csv')

def check_pdf_page(file):
    with open(cwd+'/tmp.pdf', 'wb') as f:
        f.write(requests.get(file).content)
    f = open(cwd+'/tmp.pdf', 'rb')
    doc = minecart.Document(f)
    target = []
    color_target = (0.816, 0.808, 0.808)
    pages = list(range(0, PdfFileReader(f).numPages,1))
    for page in pages:
        try:
            pdf = doc.get_page(page)
            rgb = pdf.shapes[2].fill.color.as_rgb()
            if rgb == color_target:
                target = target + [page]
        except Exception:
            pass
    read_page = str(target[0] + 1)+'-'+str(target[-1] + 1)
    return read_page

def retrieve_data(file):
    read_page = check_pdf_page(file)
    table = tabula.read_pdf(cwd+'/tmp.pdf', pages=read_page, guess=False, columns=[44,172,272,323,380,434,488,542,596,650], area=[37,0,392,720])
    now = today.day
    daterange = list(range(now - 6, now + 1))
    colname = ['Province', 'prev_data'] + daterange + ['Total']
    data = pd.DataFrame(columns=colname)
    for df in table:
        df = df.drop(df.columns[0], axis=1)
        df.columns = colname
        data = data.append(df)
    data = data.reset_index(drop=True).dropna().iloc[1:]
    data.iloc[:,1:] = data.iloc[:,1:].replace(',','', regex=True).replace('-', 0, regex=True).astype(int)
    prov_index = pd.read_csv(cwd+'/prov_index.csv')
    for name in list(prov_index.columns)[:-1]:
        rep = dict(zip(prov_index[name], prov_index['Province']))
        data['Province'] = data['Province'].replace(rep, regex=True)
    data = data.set_index('Province')
    prov_join = prov_index[['Province']].set_index('Province')
    data_full = prov_join.join(data)
    prov_join = prov_join.reset_index()
    data = data.reset_index()
    data_full = data_full.reset_index()
    data_full = data_full.sort_values(by='Total', ascending=False).reset_index(drop=True)
    #data_full = data_full.rename(columns={'Province_1': 'Province'})
    data_full = data_full.drop(columns=['prev_data', 'Total'])
    data_full = data_full.fillna(0)
    return data_full

def mergedb(data_full, database):
    if str(data_full.columns.to_list()[-1]) != database.columns.to_list()[-2].split('/')[0]:
        slice_data = data_full[['Province', data_full.columns.to_list()[-1]]].set_index('Province')
        slice_data = slice_data.rename(columns={data_full.columns.to_list()[-1]: today.strftime('%-d/%-m/%y')})
        update_db = database.iloc[:,:-1].set_index('Province')
        update_db = update_db.join(slice_data)
        update_db = update_db.reset_index()
        update_db.iloc[:,1:] = update_db.iloc[:,1:].astype(float)
        database = update_db
        database['Total'] = database.sum(axis=1)
        database = database.sort_values(by='Total', ascending=False)
        database.to_csv(cwd+'/covid_data.csv', index=False)
    return database

def top5create(database, timestamp_txt):
    top5_db = database.sort_values(by=timestamp_txt, ascending=False).reset_index(drop=True).loc[:4,['Province', timestamp_txt]]
    top5_db[timestamp_txt] = top5_db[timestamp_txt].astype(int)
    return top5_db

def plt_table(top5_db):
    fm.fontManager.addfont(cwd+'/THSarabunNew.ttf')
    plt.rcParams['font.family'] = 'TH Sarabun New'
    plt.rcParams['font.size'] = '20'
    fig, ax = plt.subplots(1,1)
    ax.axis('tight')
    ax.axis('off')
    tables = ax.table(cellText=top5_db.values, colLabels=top5_db.columns, loc='center', colColours=['yellow'] * 2)
    tables.scale(0.9, 2.5)
    for (row, col), cell in tables.get_celld().items():
        if (row == 0) or (col == -1):
            cell.set_text_props(fontproperties=fm.FontProperties(weight='bold'))
    plt.savefig(cwd+'/tmp.png')

def func_LineNotify(Message, image_file, Token):
    url = 'https://notify-api.line.me/api/notify'
    file = ({'imageFile': open(cwd+'/'+image_file, 'rb')})
    data = ({'message': Message})
    LINE_HEADERS = {'Authorization': 'Bearer ' + Token}
    session = requests.Session()
    response = session.post(url, headers=LINE_HEADERS, files=file, data=data)
    return response

while True:
    if requests.get(file).status_code != 404:
        data_full = retrieve_data(file)
        database = mergedb(data_full, database)
        timestamp_txt = today.strftime('%d/%-m/%y')
        newcases = "{:,}".format(int(database[timestamp_txt].sum(axis=0)))
        accumulated = "{:,}".format(int(database['Total'].sum(axis=0)))
        bkk_newcases = "{:,}".format(int(database[database['Province'] == 'กรุงเทพมหานคร'][timestamp_txt]))
        bkk_accumulated = "{:,}".format(int(database[database['Province'] == 'กรุงเทพมหานคร']['Total']))
        top5_db = top5create(database, timestamp_txt)
        plt_table(top5_db)
        break
    time.sleep(600)

line_token = # put your line_notify_token here

response = func_LineNotify('\nรายงานโควิด (Wave 3) ประจำวันที่ '+timestamp_txt+'\nจำนวนผู้ติดเชื้อรายใหม่ '+newcases+' คน'+'\nผู้ติดเชื้อสะสม '+accumulated+' คน'+'\n(กรุงเทพฯ จำนวนผู้ติดเชื้อรายใหม่ '+bkk_newcases+' คน'+'\nผู้ติดเชื้อสะสม '+bkk_accumulated+' คน)', 'tmp.png', line_token)
