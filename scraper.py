#!/usr/bin/env python
# coding: utf-8
import os
from datetime import datetime

import processa
import utils

url_base = 'http://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_A'

urls = [f'{url_base}{i}.zip' for i in range(2000, datetime.today().year+1)]

utils.prepare_download_folder('downloads')

for url in reversed(urls):
    print('Baixando', url, )
    file_path = os.path.join('downloads', url.split('/')[-1])

    # se for o arquivo do ano atual, faz o download
    if str(datetime.today().year) in url:
        utils.download(url, None, file_path)

    if os.path.exists(file_path) is False:
        utils.download(url, None, file_path)


processa.main()

# rename file
if os.path.exists('scraperwiki.sqlite'):
    os.rename('scraperwiki.sqlite', 'data.sqlite')
