#!/usr/bin/env python
# coding: utf-8
import os

import utils


def main():
    # extrai todos os arquivos baixados
    download_path = os.path.join('downloads')
    extraidos_path = os.path.join('extraidos')
    base_path = os.path.join('base')

    utils.descompactar_arquivos_zip(download_path, extraidos_path)

    print('Gerando arquivo final')
    utils.gerar_arquivo_final(extraidos_path, base_path)


if __name__ == '__main__':
    main()
