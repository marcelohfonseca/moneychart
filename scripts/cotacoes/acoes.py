from os import name
from pandas.io.stata import value_label_mismatch_doc
import investpy as inv
import json 
import pandas as pd
from datetime import date

# buscar as configuracoes
with open('../config.json') as config_file:
    config = json.load(config_file)

diretorio = '../../dados/cotacoes/acoes/'

data_inicial = date(config['periodo']['ano-inicial'], 1, 1)
data_final = date.today()
tipo_atualizacao = config['periodo']['incremental']
empresas = config['dados']['acoes']['empresas-br']

# buscar os dados
cotacoes = pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Currency', 'Company'])
for empresa in empresas:
    df_acoes = pd.DataFrame(inv.get_stock_historical_data(stock=empresa, country='Brazil', from_date=data_inicial.strftime('%d/%m/%Y'), to_date=data_final.strftime('%d/%m/%Y')))
    df_acoes = df_acoes.assign(Company=empresa)
    df_acoes.reset_index(inplace=True)
    cotacoes = cotacoes.append(df_acoes)

# renomear colunas, classificar e exportar os dados
cotacoes.reset_index(inplace=True, drop=True)
cotacoes = cotacoes.rename(columns ={'Date':'dt_cotacao', 'Open':'vl_abertura', 'High':'vl_alta', 'Low':'vl_baixa', 'Close':'vl_fechamento', 'Volume':'qt_volume', 'Currency':'cd_moeda', 'Company':'cd_empresa'})
cotacoes.sort_values(by=['dt_cotacao', 'cd_empresa'])
cotacoes.to_csv(f'{diretorio}acoes.csv', index=False)
print(f'Arquivo salvo na pasta "{diretorio}".')
