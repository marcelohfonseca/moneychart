import investpy as inv
import json
import pandas as pd
from datetime import date

# importar as configuracoes
with open('../config.json') as config_file:
    config = json.load(config_file)

diretorio = config['pasta-dados']
incremental = config['periodo']['incremental']
ano_inicial = config['periodo']['ano-inicial']
ano_final = date.today().year
periodo = list(range(int(ano_inicial), int(ano_final) + 1, 1))

filtro_acoes =  config['filtrar-ativos']['acoes']
filtro_fundos = config['filtrar-ativos']['fundos']
filtro_etfs = config['filtrar-ativos']['etfs']
filtro_indices = config['filtrar-ativos']['indices']
filtro_moedas = config['filtrar-ativos']['moedas']
filtro_criptos = config['filtrar-ativos']['criptomoedas']
filtro_tesouro = []

error_list = []

# buscar os dados
def buscar_dados(data_inicial, data_final):

    hist_cotacoes = pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Currency', 'Ticket'])

    for ativo in ativos:
        try:
            df_cotacoes = pd.DataFrame(inv.get_stock_historical_data(stock=ativo, country='Brazil', from_date=data_inicial, to_date=data_final))
            df_cotacoes = df_cotacoes.assign(Ticket=ativo)
            df_cotacoes.reset_index(inplace = True)
            hist_cotacoes = hist_cotacoes.append(df_cotacoes)
        except:
            print(f'Não encontrado nenhum dado para {ativo}')
            error_list.append(ativo)

    return hist_cotacoes

def salvar_arquivo(tabela):
    # renomear colunas, classificar e exportar os dados
    tabela.reset_index(inplace=True, drop=True)
    tabela = tabela.rename(columns={'Date':'dt_cotacao', 
                                    'Open':'vl_abertura', 
                                    'High':'vl_alta', 
                                    'Low':'vl_baixa', 
                                    'Close':'vl_fechamento', 
                                    'Volume':'qt_volume', 
                                    'Currency':'cd_moeda', 
                                    'Ticket':'cd_ativo'})                                            
    tabela.sort_values(by=['dt_cotacao', 'cd_ativo'])
    tabela.to_csv(f'{diretorio}{nome_arquivo}.csv', index=False)
    
    if len(error_list) > 0:
        print(f'Não foi encontrado dados para estes ativos: {error_list}')
    
    print(f'Arquivo salvo em "{diretorio}{nome_arquivo}.csv".')

# tabela base
colunas = ['name', 'full_name', 'isin', 'currency', 'symbol', 'cd_api']

# busca a lista de acoes
acoes = pd.DataFrame(inv.stocks.get_stocks(country='Brazil'), columns=colunas)
acoes = acoes.assign(cd_api='stocks')

if len(filtro_acoes) > 0:
    acoes.loc[acoes['symbol'].isin(filtro_acoes)]

# busca a lista de fundos
fundos = pd.DataFrame(inv.funds.get_funds(country='Brazil'), columns=colunas)
fundos = fundos.assign(cd_api='funds')

if len(filtro_fundos) > 0:
    fundos.loc[fundos['symbol'].isin(filtro_fundos)]

# busca a lista de etfs
etfs = pd.DataFrame(inv.etfs.get_etfs(country='Brazil'), columns=colunas)
etfs = etfs.assign(cd_api='etfs')

if len(filtro_etfs) > 0:
    etfs.loc[etfs['symbol'].isin(filtro_etfs)]

# busca a lista de indices
indices = pd.DataFrame(inv.indices.get_indices(country='Brazil'), columns=colunas)
indices = indices.assign(cd_api='indices')

if len(filtro_indices) > 0:
    indices.loc[indices['symbol'].isin(filtro_indices)]

# busca a lista de moedas
moedas = pd.DataFrame(inv.currency_crosses.get_currency_crosses(base='USD', second='BRL'), columns=colunas)
moedas = moedas.assign(cd_api='currency_crosses', currency='BRL')

if len(filtro_moedas) > 0:
    moedas.loc[moedas['symbol'].isin(filtro_moedas)]

# busca a lista de criptomoedas
criptos = pd.DataFrame(inv.crypto.get_cryptos(), columns=colunas)
criptos = criptos.assign(cd_api='crypto', full_name=criptos['name'])

if len(filtro_criptos) > 0:
    criptos.loc[criptos['symbol'].isin(filtro_criptos)]

# concatenar todas as tabelas
ativos = pd.DataFrame(columns=colunas)
ativos = ativos.append([acoes, fundos, etfs, indices, moedas, criptos], ignore_index=True)

# tratamento dos dados
ativos = ativos.rename(columns={'name':'nm_apelido', 'full_name':'nm_ativo', 'isin':'cd_isin', 'currency':'cd_moeda', 'symbol':'cd_ativo'})
ativos['cd_pais'] = ativos['cd_isin'].str[:2]
ativos['cd_emissor'] = ativos['cd_isin'].str[-10:].str[:4]
ativos['cd_tipo_ativo'] = ativos['cd_isin'].str[-6:].str[:3]
ativos['cd_especie'] = ativos['cd_isin'].str[-3:].str[:2]
ativos = ativos[['cd_ativo', 'nm_apelido', 'nm_ativo', 'cd_moeda', 'cd_isin', 'cd_pais', 'cd_emissor', 'cd_tipo_ativo', 'cd_especie', 'cd_api']]

# exportar os dados
ativos.to_csv(f'{diretorio}ativos/ativos.csv', index=False)
print(f'Arquivo salvo em "{diretorio}ativos/ativos.csv".')
