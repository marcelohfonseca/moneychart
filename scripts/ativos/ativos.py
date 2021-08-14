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

for ativo in lista_ativos:

    if lista_ativos[ativo] != False:

        # no caso de acoes
        if ativo == 'acoes':
            df_ativo = pd.DataFrame(inv.stocks.get_stocks(country='Brazil'), columns=colunas)
            df_ativo = df_ativo.assign(cd_api='stocks')

            if len(lista_ativos['acoes']) > 0:
                df_ativo = df_ativo.loc[df_ativo['symbol'].isin(lista_ativos['acoes'])]

        # no caso de criptomoedas
        if ativo == 'criptomoedas':
            df_ativo = pd.DataFrame(inv.crypto.get_cryptos(), columns=colunas)
            df_ativo = df_ativo.assign(cd_api='crypto', full_name=df_ativo['name'])
            
            if len(lista_ativos['criptomoedas']) > 0:
                df_ativo = df_ativo.loc[df_ativo['name'].isin(lista_ativos['criptomoedas'])]
        
        # no caso de etfs
        if ativo == 'etfs':
            df_ativo = pd.DataFrame(inv.etfs.get_etfs(country='Brazil'), columns=colunas)
            df_ativo = df_ativo.assign(cd_api='etfs')

            if len(lista_ativos['etfs']) > 0:
                df_ativo = df_ativo.loc[df_ativo['symbol'].isin(lista_ativos['etfs'])]

        # no caso de fundos
        if ativo == 'fundos':
            df_ativo = pd.DataFrame(inv.funds.get_funds(country='Brazil'), columns=colunas)
            df_ativo = df_ativo.assign(cd_api='funds')

            if len(lista_ativos['fundos']) > 0:
                df_ativo = df_ativo.loc[df_ativo['symbol'].isin(lista_ativos['fundos'])]

        # no caso de indices
        if ativo == 'indices':
            df_ativo = pd.DataFrame(inv.indices.get_indices(country='Brazil'), columns=colunas)
            df_ativo = df_ativo.assign(cd_api='indices')

            if len(lista_ativos['indices']) > 0:
                df_ativo = df_ativo.loc[df_ativo['symbol'].isin(lista_ativos['indices'])]
        
        # no caso de moedas
        if ativo == 'moedas':
            df_ativo = pd.DataFrame(inv.currency_crosses.get_currency_crosses(base='USD', second='BRL'), columns=colunas)
            df_ativo = df_ativo.assign(cd_api='currency_crosses', currency='BRL')

            if len(lista_ativos['moedas']) > 0:
                df_ativo = df_ativo.loc[df_ativo['name'].isin(lista_ativos['moedas'])]
        
        # concatenar todas as tabelas
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
