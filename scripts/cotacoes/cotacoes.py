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
            'moedas': config['filtrar-ativos']['moedas'], 
            'tesouro': config['filtrar-ativos']['tesouro']}

lista_erros = []

class Cotacoes:
    # tabela base para o historico de cotacoes

    def __init__(self, tipo, ativos, dt_inicial, dt_final):
        self.tipo = tipo
        self.ativos = ativos
        self.dt_inicial = dt_inicial
        self.dt_final = dt_final
        self.df_cotacoes = pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Currency', 'Ticket'])

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
                    lista_erros.append(ativo)

        # no caso de criptomoedas
        if self.tipo == 'criptomoedas':           

            # faz um de-para entre o nome e código da criptomoeda
            cripto = inv.get_cryptos_dict(columns=['name','symbol'])
            cripto_nome = []
            cripto_ticket = []
            
            for i in range(len(cripto)):
                cripto_nome.append(cripto[i].get('name'))
                cripto_ticket.append(cripto[i].get('symbol'))
            
            df_cripto = pd.DataFrame({'nome': cripto_nome, 'cd_ativo': cripto_ticket}, index=cripto_nome)

            if len(self.ativos) == 0 or self.ativos == True:
                self.ativos = inv.get_cryptos_list()
            
            for ativo in self.ativos:
                try:
                    df_historico = pd.DataFrame(inv.get_crypto_historical_data(crypto=ativo, from_date=self.dt_inicial, to_date=self.dt_final, interval=intervalo))
                    df_historico = df_historico.assign(Ticket=df_cripto.loc[ativo,'cd_ativo'])
                    df_historico.reset_index(inplace = True)
                    self.df_cotacoes = self.df_cotacoes.append(df_historico)       
                except:
                    lista_erros.append(ativo)
        
        # no caso de etfs
        if self.tipo == 'etfs':
            
            if len(self.ativos) == 0 or self.ativos == True:
                self.ativos = inv.get_etfs_list(country='Brazil')
            
            for ativo in self.ativos:
                try:
                    df_historico = pd.DataFrame(inv.get_etf_historical_data(crypto=ativo, country='Brazil', from_date=self.dt_inicial, to_date=self.dt_final, interval=intervalo))
                    df_historico = df_historico.assign(Ticket=ativo)
                    df_historico.reset_index(inplace = True)
                    self.df_cotacoes = self.df_cotacoes.append(df_historico)       
                except:
                    lista_erros.append(ativo)
        
        # no caso de fundos
        if self.tipo == 'fundos':

            if len(self.ativos) == 0 or self.ativos == True:
                self.ativos = inv.get_funds_list(country='Brazil')
            
            for ativo in self.ativos:
                try:
                    df_historico = pd.DataFrame(inv.get_fund_historical_data(crypto=ativo, country='Brazil', from_date=self.dt_inicial, to_date=self.dt_final, interval=intervalo))
                    df_historico = df_historico.assign(Ticket=ativo)
                    df_historico.reset_index(inplace = True)
                    self.df_cotacoes = self.df_cotacoes.append(df_historico)       
                except:
                    lista_erros.append(ativo)
        
        # no caso de indices
        if self.tipo == 'indices':
            
            if len(self.ativos) == 0 or self.ativos == True:
                self.ativos = inv.get_indices_list(country='Brazil')
            
            for ativo in self.ativos:
                try:
                    df_historico = pd.DataFrame(inv.get_index_historical_data(crypto=ativo, country='Brazil', from_date=self.dt_inicial, to_date=self.dt_final, interval=intervalo))
                    df_historico = df_historico.assign(Ticket=ativo)
                    df_historico.reset_index(inplace = True)
                    self.df_cotacoes = self.df_cotacoes.append(df_historico)       
                except:
                    lista_erros.append(ativo)
        
        # no caso de moedas
        if self.tipo == 'moedas':

            if len(self.ativos) == 0 or self.ativos == True:
                self.ativos = inv.get_currency_crosses_list()
            
            for ativo in self.ativos:
                try:
                    df_historico = pd.DataFrame(inv.get_currency_cross_historical_data(currency_cross=ativo, from_date=self.dt_inicial, to_date=self.dt_final, interval=intervalo))
                    df_historico = df_historico.assign(Ticket=ativo)
                    df_historico.reset_index(inplace = True)
                    self.df_cotacoes = self.df_cotacoes.append(df_historico)       
                except:
                    lista_erros.append(ativo)
        
        # no caso de tesouro
        if self.tipo == 'tesouro':
            print('Funcao ainda nao implementada')

        return Cotacoes.salvar_arquivo(self.df_cotacoes)

     # renomear colunas, classificar e exportar os dados  
    def salvar_arquivo(tabela):        
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

        if len(lista_erros) > 0:
            print(f'Não foi encontrado dados para estes ativos: {lista_erros}')

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
    