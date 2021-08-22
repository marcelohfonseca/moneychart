import investpy as inv
import json
import pandas as pd

# importar as configuracoes
with open('../config.json') as config_file:
    config = json.load(config_file)

diretorio = config['pasta-dados'] + 'ativos/'
nome_arquivo = 'ativos'

lista_ativos = {
    'acoes': config['filtrar-ativos']['acoes'], 
    'criptomoedas': config['filtrar-ativos']['criptomoedas'], 
    'etfs': config['filtrar-ativos']['etfs'], 
    'fundos': config['filtrar-ativos']['fundos'], 
    'indices': config['filtrar-ativos']['indices'], 
    'moedas': config['filtrar-ativos']['moedas'], 
    'tesouro': config['filtrar-ativos']['tesouro']}

# tabela base
colunas = ['name', 'full_name', 'isin', 'currency', 'symbol', 'cd_api']
ativos = pd.DataFrame(columns=colunas)

for tipo_ativo in lista_ativos:

    if lista_ativos[tipo_ativo] != False:

        # no caso de acoes
        if tipo_ativo == 'acoes':
            df_ativo = pd.DataFrame(inv.stocks.get_stocks(country='Brazil'), columns=colunas)
            df_ativo = df_ativo.assign(cd_api='investpy/stocks')

        # no caso de criptomoedas
        if tipo_ativo == 'criptomoedas':
            df_ativo = pd.DataFrame(inv.crypto.get_cryptos(), columns=colunas)
            df_ativo = df_ativo.assign(cd_api='investpy/crypto', full_name=df_ativo['name'])
        
        # no caso de etfs
        if tipo_ativo == 'etfs':
            df_ativo = pd.DataFrame(inv.etfs.get_etfs(country='Brazil'), columns=colunas)
            df_ativo = df_ativo.assign(cd_api='investpy/etfs')

        # no caso de fundos
        if tipo_ativo == 'fundos':
            df_ativo = pd.DataFrame(inv.funds.get_funds(country='Brazil'), columns=colunas)
            df_ativo = df_ativo.assign(cd_api='investpy/funds')

        # no caso de indices
        if tipo_ativo == 'indices':
            df_ativo = pd.DataFrame(inv.indices.get_indices(country='Brazil'), columns=colunas)
            df_ativo = df_ativo.assign(cd_api='investpy/indices')
        
        # no caso de moedas
        if tipo_ativo == 'moedas':
            df_ativo = pd.DataFrame(inv.currency_crosses.get_currency_crosses(base='USD', second='BRL'), columns=colunas)
            df_ativo = df_ativo.assign(cd_api='investpy/currency_crosses', currency='BRL', symbol=df_ativo['name'])
        
        # no caso de tesouro
        if tipo_ativo == 'tesouro':
            df_ativo = pd.read_csv('../de_para_tesouro.csv', sep=';')
            df_ativo = df_ativo.assign(cd_api='tesouro-direto', symbol=df_ativo['Codigo'], currency='BRL', name=df_ativo['Tipo Titulo'], full_name=df_ativo['Tipo Titulo'])
        
        # filtrar e concatenar todas as tabelas
        if len(lista_ativos[tipo_ativo]) > 0:
            df_ativo = df_ativo.loc[df_ativo['symbol'].isin(lista_ativos[tipo_ativo])]
    
        ativos = ativos.append(df_ativo)

# tratamento dos dados
ativos.reset_index(inplace = True)
ativos = ativos.rename(columns={'name':'nm_apelido', 'full_name':'nm_ativo', 'isin':'cd_isin', 'currency':'cd_moeda', 'symbol':'cd_ativo'})
ativos['cd_pais'] = ativos['cd_isin'].str[:2]
ativos['cd_emissor'] = ativos['cd_isin'].str[-10:].str[:4]
ativos['cd_tipo_ativo'] = ativos['cd_isin'].str[-6:].str[:3]
ativos['cd_especie'] = ativos['cd_isin'].str[-3:].str[:2]
ativos = ativos[['cd_ativo', 'nm_apelido', 'nm_ativo', 'cd_moeda', 'cd_isin', 'cd_pais', 'cd_emissor', 'cd_tipo_ativo', 'cd_especie', 'cd_api']]

# exportar os dados
ativos.to_csv(f'{diretorio}{nome_arquivo}.csv', index=False)
print(f'Arquivo salvo em "{diretorio}{nome_arquivo}.csv".')
