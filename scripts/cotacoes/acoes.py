import investpy as inv
import json
import pandas as pd
from datetime import date

# buscar as configuracoes
with open('../config.json') as config_file:
    config = json.load(config_file)

diretorio = config['salvar-dados'] + 'cotacoes/acoes/'
incremental = config['periodo']['incremental']
ano_inicial = config['periodo']['ano-inicial']
ano_final = date.today().year
periodo = list(range(int(ano_inicial), int(ano_final) + 1, 1))
ativos = config['coletar-dados']['acoes']['brasil']
error_list = []

if len(ativos) == 0:
    ativos = inv.stocks.get_stocks_list(country='Brazil')

# buscar os dados
def buscar_dados(data_inicial, data_final):
    hist_cotacoes = pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Currency', 'Ticket'])
    for ativo in ativos:
        try:
            df_cotacoes = pd.DataFrame(inv.get_stock_historical_data(stock=ativo, country='Brazil', from_date=data_inicial, to_date=data_final))
            df_cotacoes = df_cotacoes.assign(Company=ativo)
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
