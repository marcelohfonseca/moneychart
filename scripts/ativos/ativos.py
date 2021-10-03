# Investing Go | Marcelo Henrique Fonseca => https://github.com/marcelohfonseca
# Consulte os arquivos README.md e LICENSE.md para detalhes.

# --------------------------------------------------
# IMPORTAR BIBLIOTECAS E PARAMETROS INICIAIS
# --------------------------------------------------

import investpy as inv

import json
import pandas as pd

from urllib.request import urlopen
from zipfile import ZipFile
import io
import numpy as np

import requests as r
from bs4 import BeautifulSoup as bs
from lxml import html

with open('../config.json') as config_file:
    config = json.load(config_file)

    """ 
    O arquivo "../json.config" serve para definir alguns parâmetros 
    importantes, como quais ativos serão utilizados para realização
    da busca de dados pelos scripts, nome do arquivo final e diretorio.

    """

    folder = config['pasta-dados'] + 'ativos/'
    file_name = 'ativos'
    country = config['pais']

    list_asset_type = {'acoes': config['filtrar-ativos']['acoes'], 
                       'criptomoedas': config['filtrar-ativos']['criptomoedas'], 
                       'etfs': config['filtrar-ativos']['etfs'], 
                       'fundos': config['filtrar-ativos']['fundos'], 
                       'indices': config['filtrar-ativos']['indices'], 
                       'moedas': config['filtrar-ativos']['moedas'], 
                       'tesouro': config['filtrar-ativos']['tesouro']}

# --------------------------------------------------
#  FORMAR DATAFRAME UNICO COM TODOS OS ATIVOS
# --------------------------------------------------

"""
Verificar o tipo de ativo para selecionar a função correta da
biblioteca "investpy". Para cada consulta, além dos campos padrões
retornados, será adicionado a coluna "cd_ativo" para que o retorno
dos dados seja concatenado com os demais ativos consultados.

Se o parâmetro "filtrar-ativos" estiver listando valores para cada
tipo de ativo, a consulta vai buscar apenas eles, senão, trará 
todos os ativos daquele "tipo" (Ex: todos os ativos de ações).

"""

dict_columns = {'symbol': 'cd_ativo',
                'name': 'nm_apelido',
                'full_name': 'nm_ativo', 
                'isin': 'cd_isin', 
                'currency': 'cd_moeda', 
                'api': 'cd_api'}

df_assets = pd.DataFrame(columns=dict_columns.keys())

# buscar o cadastro de ativos
print(f'INICIO: criando uma tabela com todos os ativos')

for asset_type in list_asset_type:
    if list_asset_type[asset_type] != False:

        # no caso de acoes
        if asset_type == 'acoes':
            df_base = pd.DataFrame(inv.stocks.get_stocks(country=country),
                                                         columns=dict_columns.keys())
            df_base = df_base.assign(api='investpy/stocks')

        # no caso de criptomoedas
        if asset_type == 'criptomoedas':
            df_base = pd.DataFrame(inv.crypto.get_cryptos(), columns=dict_columns.keys())
            df_base = df_base.assign(api='investpy/crypto', 
                                     full_name=df_base['name'])
        
        # no caso de etfs
        if asset_type == 'etfs':
            df_base = pd.DataFrame(inv.etfs.get_etfs(country=country),
                                                     columns=dict_columns.keys())
            df_base = df_base.assign(api='investpy/etfs')

        # no caso de fundos
        if asset_type == 'fundos':
            df_base = pd.DataFrame(inv.funds.get_funds(country=country),
                                                       columns=dict_columns.keys())
            df_base = df_base.assign(api='investpy/funds')

        # no caso de indices
        if asset_type == 'indices':
            df_base = pd.DataFrame(inv.indices.get_indices(country=country),
                                                           columns=dict_columns.keys())
            df_base = df_base.assign(api='investpy/indices')
        
        # no caso de moedas
        if asset_type == 'moedas':
            df_base = pd.DataFrame(inv.currency_crosses.get_currency_crosses(base='USD', 
                                                                             second='BRL'), 
                                                                             columns=dict_columns.keys())
            df_base = df_base.assign(api='investpy/currency_crosses', 
                                     currency='BRL', 
                                     symbol=df_base['name'])
        
        # no caso de tesouro
        if asset_type == 'tesouro':
            df_base = pd.read_csv('../de_para_tesouro.csv', sep=';')
            df_base = df_base.assign(api='tesouro-direto', 
                                     currency='BRL',                                       
                                     name=df_base['Tipo Titulo'], 
                                     full_name=df_base['Tipo Titulo'],
                                     symbol=df_base['Codigo'])
        
        # filtrar de acordo com o arquivo "../config.json"
        if len(list_asset_type[asset_type]) > 0:
            df_base = df_base.loc[df_base['symbol'].isin(list_asset_type[asset_type])]

        # concatenar para único dataframe compor todos os ativos
        df_assets = df_assets.append(df_base)

df_assets.reset_index(inplace=True)
df_assets = df_assets.rename(columns=dict_columns)

# separa "cd_isin" em "cd_pais", "cd_emissor", "cd_tipo_ativo" e "cd_especie"
df_assets['cd_pais'] = df_assets['cd_isin'].str[:2]
df_assets['cd_emissor'] = df_assets['cd_isin'].str[-10:].str[:4]
df_assets['cd_tipo_ativo'] = df_assets['cd_isin'].str[-6:].str[:3]
df_assets['cd_especie'] = df_assets['cd_isin'].str[-3:].str[:2]

df_assets = df_assets[['cd_ativo', 'nm_apelido', 'nm_ativo', 'cd_moeda', \
                       'cd_isin', 'cd_pais', 'cd_emissor', 'cd_tipo_ativo', \
                       'cd_especie', 'cd_api']]

# --------------------------------------------------
# CLASSIFICACAO DOS ATIVOS
# --------------------------------------------------

"""
Buscar as classificações das empresas listadas na B3.
Estes dados são fornecidos em planilha Excel não estruturada, 
portando é necessário realizar o download, tratar e preencher
as linhas vazias.

"""

url = 'http://www.b3.com.br/lumis/portal/file/fileDownload.jsp?fileId=8AA8D0975A2D7918015A3C81693D4CA4'
file = ZipFile(io.BytesIO(urlopen(url).read()))

df_b3 = pd.read_excel(file.open(file.filelist[0]))
df_b3 = df_b3[df_b3.columns[:4]]
df_b3.columns = ['nm_setor','nm_subsetor','nm_segmento','cd_emissor']

df_b3['nm_setor'] = df_b3['nm_setor'].str.replace(', ',' ')
df_b3['nm_subsetor'] = df_b3['nm_subsetor'].str.replace(', ',' ')
df_b3['nm_segmento'] = df_b3['nm_segmento'].str.replace(', ',' ')
df_b3['nm_classe'] = 'Ações'

df_b3['nm_segmento'] = np.where(df_b3['cd_emissor'].notna(), np.nan, df_b3['nm_segmento'])
df_b3['drop'] = np.where(df_b3['cd_emissor'].notna(), False, True)
df_b3 = df_b3.ffill(axis=0)
df_b3 = df_b3.loc[(df_b3['nm_setor'] != 'SETOR ECONÔMICO') 
                & (df_b3['cd_emissor'] != 'CÓDIGO') 
                & (df_b3['drop'] == False)]
df_b3 = df_b3.drop(columns='drop')

"""
Buscar as classificações de fundos de investimentos imobiliários
no site https://fiis.com.br/ pois a API investpy não possui
este tipo de informação complementar.

"""

url = 'https://fiis.com.br/lista-de-fundos-imobiliarios/'

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
            AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36'}

page = r.get(url, headers=headers)
content = bs(page.content, 'html.parser')
response = content.find_all('span', class_='ticker')

list_fiis = []
list_error = []
df_fiis = pd.DataFrame(columns=['cd_ativo','nm_classe','nm_setor','nm_subsetor','nm_segmento'])

for i in range(len(response)):
    list_fiis.append(response[i].text)

for fii in list_fiis:
    try:
        url = f'https://fiis.com.br/{fii}' 

        page = r.get(url) 
        content = html.fromstring(page.content)

        type = content.xpath('//*[@id="informations--basic"]/div[1]/div[2]/span[2]')
        segment = content.xpath('//*[@id="informations--basic"]/div[1]/div[3]/span[2]')

        df_base = pd.DataFrame(data={'cd_ativo': [fii], 
                                     'ds_tipo': [type[0].text], 
                                     'nm_segmento': [segment[0].text]})

        df_base['nm_setor'] = df_base['ds_tipo'].str.split(':').str[0]
        df_base['nm_subsetor'] = df_base['ds_tipo'].str.split(':').str[1]
        df_base['nm_classe'] = 'Fundos Imobiliários'
        df_base = df_base.drop(columns='ds_tipo')
        df_fiis = df_fiis.append(df_base)
    except:
        list_error.append(fii)

# imprime a lista de erros caso exista
if len(list_error) > 0:
    print(f'AVISO: Não foram encontrados dados para estes ativos: {list_error}')

# --------------------------------------------------
# EXPORTAR OS DADOS
# --------------------------------------------------

df_result_fii = pd.merge(left=df_assets.loc[df_assets['cd_ativo'].isin(list_fiis)],
                         right=df_fiis,
                         how='left',
                         on='cd_ativo')

df_result_stocks = pd.merge(left=df_assets.loc[~df_assets['cd_ativo'].isin(list_fiis)],
                            right=df_b3,
                            how='left',
                            on='cd_emissor')

df_result = pd.concat([df_result_fii, df_result_stocks])

# exportar para "csv"
df_result.to_csv(f'{folder}{file_name}.csv', index=False)
print('\n' + f'FIM: Arquivo salvo em "{folder}{file_name}.csv".')
