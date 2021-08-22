import investpy as inv
import json
import pandas as pd
from datetime import date

# buscar as configuracoes
with open('../config.json') as config_file:
    config = json.load(config_file)

ano_inicial = config['periodo']['ano-inicial']
ano_final = config['periodo']['ano-final']
incremental = config['periodo']['incremental']
intervalo = config['periodo']['intervalo']

lista_ativos = {
    'acoes': config['filtrar-ativos']['acoes'], 
    'criptomoedas': config['filtrar-ativos']['criptomoedas'], 
    'etfs': config['filtrar-ativos']['etfs'], 
    'fundos': config['filtrar-ativos']['fundos'], 
    'indices': config['filtrar-ativos']['indices'], 
    'moedas': config['filtrar-ativos']['moedas']}

class Cotacoes:
    # tabela base para o historico de cotacoes

    def __init__(self, tipo, ativos, dt_inicial, dt_final):
        self.tipo = tipo
        self.ativos = ativos
        self.dt_inicial = dt_inicial
        self.dt_final = dt_final
        self.dict_columns = {'Date':'dt_cotacao', 'Open':'vl_abertura', 'High':'vl_alta', 'Low':'vl_baixa', 'Close':'vl_fechamento', 'Volume':'qt_volume', 'Currency':'cd_moeda', 'Ticket':'cd_ativo'}
        self.df_cotacoes = pd.DataFrame(columns=self.dict_columns.keys())
        self.lista_erros = []

    # buscar os dados de cotacoes
    def cotacoes(self):

        # no caso de acoes
        if self.tipo == 'acoes':
            
            if len(self.ativos) == 0 or self.ativos == True:
                self.ativos = inv.get_stocks_list(country='Brazil')
            
            for ativo in self.ativos:
                try:                    
                    df_historico = pd.DataFrame(inv.get_stock_historical_data(stock=ativo, country='Brazil', from_date=self.dt_inicial, to_date=self.dt_final, interval=intervalo))
                    df_historico = df_historico.assign(Ticket=ativo)
                    df_historico.reset_index(inplace = True)
                    self.df_cotacoes = self.df_cotacoes.append(df_historico)
                except:
                    self.lista_erros.append(ativo)

        # no caso de criptomoedas
        if self.tipo == 'criptomoedas':           

            # faz um de-para entre o nome e código
            dict_base = inv.get_cryptos_dict(columns=['name','symbol'])
            dict_ativos = {}
            
            for i in range(len(dict_base)):
                dict_ativos[dict_base[i].get('symbol')] = dict_base[i].get('name')

            if len(self.ativos) == 0 or self.ativos == True:
                self.ativos = dict_ativos.values()
            else:
                self.ativos = list({k:v for k,v in dict_ativos.items() if k in self.ativos}.values())
            
            for ativo in self.ativos:                           
                try:
                    cd_ativo = list({k:v for k,v in dict_ativos.items() if v == ativo}.keys())[0]
                    df_historico = pd.DataFrame(inv.get_crypto_historical_data(crypto=ativo, from_date=self.dt_inicial, to_date=self.dt_final, interval=intervalo))
                    df_historico = df_historico.assign(Ticket=cd_ativo)
                    df_historico.reset_index(inplace = True)
                    self.df_cotacoes = self.df_cotacoes.append(df_historico)
                except:
                    self.lista_erros.append(ativo)
        
        # no caso de etfs
        if self.tipo == 'etfs':

            # faz um de-para entre o nome e código
            dict_base = inv.get_etfs_dict(country='Brazil', columns=['name','symbol'])
            dict_ativos = {}
            
            for i in range(len(dict_base)):
                dict_ativos[dict_base[i].get('symbol')] = dict_base[i].get('name')
            
            if len(self.ativos) == 0 or self.ativos == True:
                self.ativos = dict_ativos.values()
            else:
                self.ativos = list({k:v for k,v in dict_ativos.items() if k in self.ativos}.values())
            
            for ativo in self.ativos:
                try:
                    cd_ativo = list({k:v for k,v in dict_ativos.items() if v == ativo}.keys())[0]
                    df_historico = pd.DataFrame(inv.get_etf_historical_data(etf=ativo, country='Brazil', from_date=self.dt_inicial, to_date=self.dt_final, interval=intervalo))
                    df_historico = df_historico.assign(Ticket=cd_ativo).drop(columns=['Exchange'])
                    df_historico.reset_index(inplace = True)
                    self.df_cotacoes = self.df_cotacoes.append(df_historico)
                except:
                    self.lista_erros.append(ativo)
        
        # no caso de fundos
        if self.tipo == 'fundos':

            # faz um de-para entre o nome e código
            dict_base = inv.get_funds_dict(country='Brazil', columns=['name','symbol'])
            dict_ativos = {}
            
            for i in range(len(dict_base)):
                dict_ativos[dict_base[i].get('symbol')] = dict_base[i].get('name')

            if len(self.ativos) == 0 or self.ativos == True:
                self.ativos = dict_ativos.values()
            else:
                self.ativos = list({k:v for k,v in dict_ativos.items() if k in self.ativos}.values())
            
            for ativo in self.ativos:
                try:
                    cd_ativo = list({k:v for k,v in dict_ativos.items() if v == ativo}.keys())[0]
                    df_historico = pd.DataFrame(inv.get_fund_historical_data(fund=ativo, country='Brazil', from_date=self.dt_inicial, to_date=self.dt_final, interval=intervalo))
                    df_historico = df_historico.assign(Ticket=cd_ativo)
                    df_historico.reset_index(inplace = True)
                    self.df_cotacoes = self.df_cotacoes.append(df_historico)
                except:
                    self.lista_erros.append(ativo)
        
        # no caso de indices
        if self.tipo == 'indices':

            # faz um de-para entre o nome e código
            dict_base = inv.get_indices_dict(country='Brazil', columns=['name','symbol'])
            dict_ativos = {}
            
            for i in range(len(dict_base)):
                dict_ativos[dict_base[i].get('symbol')] = dict_base[i].get('name')
            
            if len(self.ativos) == 0 or self.ativos == True:
                self.ativos = dict_ativos.values()
            else:
                self.ativos = list({k:v for k,v in dict_ativos.items() if k in self.ativos}.values())
            
            for ativo in self.ativos:
                try:
                    cd_ativo = list({k:v for k,v in dict_ativos.items() if v == ativo}.keys())[0]
                    df_historico = pd.DataFrame(inv.get_index_historical_data(index=ativo, country='Brazil', from_date=self.dt_inicial, to_date=self.dt_final, interval=intervalo))
                    df_historico = df_historico.assign(Ticket=cd_ativo)
                    df_historico.reset_index(inplace = True)
                    self.df_cotacoes = self.df_cotacoes.append(df_historico)
                except:
                    self.lista_erros.append(ativo)
        
        # no caso de moedas
        if self.tipo == 'moedas':

            if len(self.ativos) == 0 or self.ativos == True:
                self.ativos = inv.get_currency_crosses_list(second='BRL')
            
            for ativo in self.ativos:
                try:
                    df_historico = pd.DataFrame(inv.get_currency_cross_historical_data(currency_cross=ativo, from_date=self.dt_inicial, to_date=self.dt_final, interval=intervalo))
                    df_historico = df_historico.assign(Ticket=ativo)
                    df_historico.reset_index(inplace = True)
                    self.df_cotacoes = self.df_cotacoes.append(df_historico)
                except:
                    self.lista_erros.append(ativo)

        # imprime a lista de erros
        if len(self.lista_erros) > 0:
            print(f'Não foi encontrado dados entre {self.dt_inicial} e {self.dt_final} para estes ativos: {self.lista_erros}')

        return Cotacoes.salvar_arquivo(self.df_cotacoes, self.dict_columns)
        
    # renomear colunas, classificar e exportar os dados  
    def salvar_arquivo(df_cotacoes, dict_columns):
        df_cotacoes.reset_index(inplace=True, drop=True)  
        df_cotacoes = df_cotacoes.rename(columns=dict_columns)
        df_cotacoes.sort_values(by=['dt_cotacao', 'cd_ativo'])        
        df_cotacoes.to_csv(f'{diretorio}{nome_arquivo}.csv', index=False, decimal=',')
        print(f'Arquivo salvo em "{diretorio}{nome_arquivo}.csv".')

for ativo in lista_ativos:

    if lista_ativos[ativo] != False:
        # buscar as cotacoes historicas
        print(f'Buscando dados do tipo de ativo {ativo}')

        diretorio = config['pasta-dados'] + f'cotacoes/{ativo}/'

        if ano_final == False:
            ano_final = date.today().year

        if incremental == True:
            # redefinir variaveis
            dt_inicial = date(date.today().year, 1, 1).strftime('%d/%m/%Y')
            dt_final = date.today().strftime('%d/%m/%Y')
            nome_arquivo = f'{ativo}_{ano_final}'
            
            # buscar os dados apenas do último ano
            Cotacoes(ativo, lista_ativos[ativo], dt_inicial, dt_final).cotacoes()            

        else:
            periodo = list(range(int(ano_inicial), int(ano_final) + 1, 1))
            
            for ano in periodo:
                dt_inicial = date(ano, 1, 1).strftime('%d/%m/%Y')
                dt_final = date(ano, 12, 31).strftime('%d/%m/%Y')
                nome_arquivo = f'{ativo}_{ano}'

                # buscar os dados de forma incremental
                Cotacoes(ativo, lista_ativos[ativo], dt_inicial, dt_final).cotacoes() 
    else:
        print(f'Desconsiderar o tipo de ativo {ativo}')
    