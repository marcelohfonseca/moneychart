import investpy as inv
import json 
import pandas as pd
from datetime import date

# buscar as configuracoes
with open('../config.json') as config_file:
    config = json.load(config_file)

diretorio = '../../dados/cotacoes/acoes/'
empresas = config['dados']['acoes']['empresas-br']
incremental = config['periodo']['incremental']
ano_inicial = config['periodo']['ano-inicial']
ano_final = date.today().year
periodo = list(range(int(ano_inicial), int(ano_final) + 1, 1))

# buscar os dados
def buscar_dados(data_inicial, data_final):
    cotacoes = pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Currency', 'Company'])
    for empresa in empresas:
        df_acoes = pd.DataFrame(inv.get_stock_historical_data(stock=empresa, country='Brazil', from_date=data_inicial, to_date=data_final))
        df_acoes = df_acoes.assign(Company=empresa)
        df_acoes.reset_index(inplace=True)
        cotacoes = cotacoes.append(df_acoes)
    return cotacoes

def salvar_arquivo(data_frame):
    # renomear colunas, classificar e exportar os dados
    data_frame.reset_index(inplace=True, drop=True)
    data_frame = data_frame.rename(columns ={'Date':'dt_cotacao', 'Open':'vl_abertura', 'High':'vl_alta', 'Low':'vl_baixa', 'Close':'vl_fechamento', 'Volume':'qt_volume', 'Currency':'cd_moeda', 'Company':'cd_empresa'})
    data_frame.sort_values(by=['dt_cotacao', 'cd_empresa'])
    data_frame.to_csv(f'{diretorio}{nome_arquivo}.csv', index=False)
    print(f'Arquivo salvo em "{diretorio}{nome_arquivo}.csv".')

if incremental == True:
    data_inicial = date(ano_final, 1, 1).strftime('%d/%m/%Y')
    data_final = date.today().strftime('%d/%m/%Y')
    nome_arquivo = f'acoes_{ano_final}'
    salvar_arquivo(buscar_dados(data_inicial, data_final))

else:
    for ano in periodo:
        data_inicial = date(ano, 1, 1).strftime('%d/%m/%Y')
        data_final = date(ano, 12, 31).strftime('%d/%m/%Y')
        nome_arquivo = f'acoes_{ano}'
        salvar_arquivo(buscar_dados(data_inicial, data_final))
