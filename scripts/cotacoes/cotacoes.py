# Investing Go | Marcelo Henrique Fonseca => https://github.com/marcelohfonseca
# Consulte os arquivos README.md e LICENSE.md para detalhes.

# --------------------------------------------------
# IMPORTAR BIBLIOTECAS E PARAMETROS INICIAIS
# --------------------------------------------------

import investpy as inv

import json
import pandas as pd
import numpy as np
from datetime import date
from tqdm import tqdm

# buscar as configuracoes
with open('../config.json') as config_file:
    config = json.load(config_file)

    """ 
    O arquivo "../json.config" serve para definir alguns parâmetros 
    importantes, como quais ativos serão utilizados para realização
    da busca de dados pelos scripts, nome do arquivo final e diretorio.

    """

    year_start = config['periodo']['ano-inicial']
    year_end = config['periodo']['ano-final']
    incremental = config['periodo']['incremental']
    interval = config['periodo']['intervalo']
    country = config['pais']

    list_asset_type = {'acoes': config['filtrar-ativos']['acoes'], 
                       'criptomoedas': config['filtrar-ativos']['criptomoedas'], 
                       'etfs': config['filtrar-ativos']['etfs'], 
                       'fundos': config['filtrar-ativos']['fundos'], 
                       'indices': config['filtrar-ativos']['indices'], 
                       'moedas': config['filtrar-ativos']['moedas']}

# --------------------------------------------------
# FUNCOES DE CONSULTA E TRATAMENTO DOS DADOS
# --------------------------------------------------

class Quotes:

    """
    Esta classe/função (Quotes => quotes) busca o histórico de cotações
    através da biblioteca "investpy", para os tipos de ativos
    ações, criptomoedas, etfs, fundos, índices e moedas. São passados
    os parâmetros de "ticker", período e país do ativo.

    O retorno dos dados segue o seguinte padrão:

    Date | Open | High | Low | Close | Volume | Currency
    -----|------|------|-----|-------|--------|----------
    xxxx | xxxx | xxxx | xxx | xxxxx | xxxxxx | xxxxxxxx

    """

    def __init__(self, type, assets, dt_start, dt_end):
        self.asset_type = type
        self.list_symbol = assets
        self.dt_start = dt_start
        self.dt_end = dt_end
        self.dict_columns = {'Date':'dt_cotacao', 
                             'Open':'vl_abertura', 
                             'High':'vl_alta', 
                             'Low':'vl_baixa', 
                             'Close':'vl_fechamento', 
                             'Volume':'qt_volume', 
                             'Currency':'cd_moeda', 
                             'Symbol':'cd_ativo'}

        self.df_quotes = pd.DataFrame(columns=self.dict_columns.keys())
        self.list_error = []

    # buscar os dados de cotacoes
    def quotes(self):

        """
        Verificar o tipo de ativo para selecionar a função correta da
        biblioteca "investpy". Para cada consulta, além dos campos padrões
        retornados, será adicionado a coluna "cd_ativo" para que o retorno
        dos dados seja concatenado com os demais ativos consultados.

        Se o parâmetro "filtrar-ativos" estiver listando valores para cada
        tipo de ativo, a consulta vai buscar apenas eles, senão, trará 
        todos os ativos daquele "tipo" (Ex: todos os ativos de ações).

        Caso não seja encontrado o ativo pela consulta, o Ticker (código
        do ativo) será inserido em uma lista de erros, para ser listado
        ao fim da execução desta função.

        """

        # no caso de acoes
        if self.asset_type == 'acoes':
            
            if len(self.list_symbol) == 0 or self.list_symbol == True:
                self.list_symbol = inv.get_stocks_list(country=country)
            
            for symbol in tqdm(self.list_symbol):
                try:                    
                    df_history = pd.DataFrame(inv.get_stock_historical_data(stock=symbol, 
                                                                            country=country,
                                                                            from_date=self.dt_start, 
                                                                            to_date=self.dt_end, 
                                                                            interval=interval))
                    df_history = df_history.assign(Symbol=symbol)
                    df_history.reset_index(inplace = True)
                    self.df_quotes = self.df_quotes.append(df_history)
                except:
                    self.list_error.append(symbol)

        # no caso de criptomoedas
        if self.asset_type == 'criptomoedas':           

            # faz um de-para entre o nome e código
            dict_base = inv.get_cryptos_dict(columns=['name','symbol'])
            dict_assets = {}
            
            for i in range(len(dict_base)):
                dict_assets[dict_base[i].get('symbol')] = dict_base[i].get('name')

            if len(self.list_symbol) == 0 or self.list_symbol == True:
                self.list_symbol = dict_assets.values()
            else:
                self.list_symbol = list({k:v for k,v in dict_assets.items() if k in self.list_symbol}.values())
            
            for symbol_name in tqdm(self.list_symbol):
                try:
                    symbol = list({k:v for k,v in dict_assets.items() if v == symbol_name}.keys())[0]
                    df_history = pd.DataFrame(inv.get_crypto_historical_data(crypto=symbol_name, 
                                                                             from_date=self.dt_start, 
                                                                             to_date=self.dt_end, 
                                                                             interval=interval))
                    df_history = df_history.assign(Symbol=symbol)
                    df_history.reset_index(inplace = True)
                    self.df_quotes = self.df_quotes.append(df_history)
                except:
                    self.list_error.append(symbol)
        
        # no caso de etfs
        if self.asset_type == 'etfs':

            # faz um de-para entre o nome e código
            dict_base = inv.get_etfs_dict(country=country, columns=['name','symbol'])
            dict_assets = {}
            
            for i in range(len(dict_base)):
                dict_assets[dict_base[i].get('symbol')] = dict_base[i].get('name')
            
            if len(self.list_symbol) == 0 or self.list_symbol == True:
                self.list_symbol = dict_assets.values()
            else:
                self.list_symbol = list({k:v for k,v in dict_assets.items() if k in self.list_symbol}.values())
            
            for symbol_name in tqdm(self.list_symbol):
                try:
                    symbol = list({k:v for k,v in dict_assets.items() if v == symbol_name}.keys())[0]
                    df_history = pd.DataFrame(inv.get_etf_historical_data(etf=symbol_name, 
                                                                          country=country, 
                                                                          from_date=self.dt_start, 
                                                                          to_date=self.dt_end, 
                                                                          interval=interval))
                    df_history = df_history.assign(Symbol=symbol).drop(columns=['Exchange'])
                    df_history.reset_index(inplace = True)
                    self.df_quotes = self.df_quotes.append(df_history)
                except:
                    self.list_error.append(symbol)
        
        # no caso de fundos
        if self.asset_type == 'fundos':

            # faz um de-para entre o nome e código
            dict_base = inv.get_funds_dict(country=country, columns=['name','symbol'])
            dict_assets = {}
            
            for i in range(len(dict_base)):
                dict_assets[dict_base[i].get('symbol')] = dict_base[i].get('name')

            if len(self.list_symbol) == 0 or self.list_symbol == True:
                self.list_symbol = dict_assets.values()
            else:
                self.list_symbol = list({k:v for k,v in dict_assets.items() if k in self.list_symbol}.values())
            
            for symbol_name in tqdm(self.list_symbol):
                try:
                    symbol = list({k:v for k,v in dict_assets.items() if v == symbol_name}.keys())[0]
                    df_history = pd.DataFrame(inv.get_fund_historical_data(fund=symbol_name, 
                                                                           country=country, 
                                                                           from_date=self.dt_start, 
                                                                           to_date=self.dt_end, 
                                                                           interval=interval))
                    df_history = df_history.assign(Symbol=symbol)
                    df_history.reset_index(inplace = True)
                    self.df_quotes = self.df_quotes.append(df_history)
                except:
                    self.list_error.append(symbol)
        
        # no caso de indices
        if self.asset_type == 'indices':

            # faz um de-para entre o nome e código
            dict_base = inv.get_indices_dict(country=country, columns=['name','symbol'])
            dict_assets = {}
            
            for i in range(len(dict_base)):
                dict_assets[dict_base[i].get('symbol')] = dict_base[i].get('name')
            
            if len(self.list_symbol) == 0 or self.list_symbol == True:
                self.list_symbol = dict_assets.values()
            else:
                self.list_symbol = list({k:v for k,v in dict_assets.items() if k in self.list_symbol}.values())
            
            for symbol_name in tqdm(self.list_symbol):
                try:
                    symbol = list({k:v for k,v in dict_assets.items() if v == symbol_name}.keys())[0]
                    df_history = pd.DataFrame(inv.get_index_historical_data(index=symbol_name, 
                                                                            country=country, 
                                                                            from_date=self.dt_start, 
                                                                            to_date=self.dt_end, 
                                                                            interval=interval))
                    df_history = df_history.assign(Symbol=symbol)
                    df_history.reset_index(inplace = True)
                    self.df_quotes = self.df_quotes.append(df_history)
                except:
                    self.list_error.append(symbol)
        
        # no caso de moedas
        if self.asset_type == 'moedas':

            if len(self.list_symbol) == 0 or self.list_symbol == True:
                self.list_symbol = inv.get_currency_crosses_list(second='BRL')
            
            for symbol in tqdm(self.list_symbol):
                try:
                    df_history = pd.DataFrame(inv.get_currency_cross_historical_data(currency_cross=symbol, 
                                                                                     from_date=self.dt_start, 
                                                                                     to_date=self.dt_end, 
                                                                                     interval=interval))
                    df_history = df_history.assign(Symbol=symbol)
                    df_history.reset_index(inplace = True)
                    self.df_quotes = self.df_quotes.append(df_history)
                except:
                    self.list_error.append(symbol)

        # imprime a lista de erros caso exista
        if len(self.list_error) > 0:
            print(f'AVISO: Sem dados entre {self.dt_start} - {self.dt_end} para: {self.list_error}')

        return Quotes.save_file(self.df_quotes, self.dict_columns)
        
    # renomear colunas, classificar e exportar os dados  
    def save_file(df_quotes, dict_columns):

        """
        Essa função tem o objetivo de pegar o dataframe retornado pela
        função "quotes" e tratar, renomeando as colunas, classificando
        os dados e também adicionando as colunas de variação do preço
        de fechamento, em valor e percentual.

        O dataframe final deve ficar como:

        dt_cotacao | vl_abertura | vl_alta | vl_vaixa | vl_fechamento | =>
        -----------|-------------|---------|----------|---------------|
        xxxxxxxxxx | xxxxxxxxxxx | xxxxxxx | xxxxxxxx | xxxxxxxxxxxxx | 
        
        qt_volume | cd_moeda | cd_ativo | vl_var | pr_var | cd_var |
        ----------|----------|----------|--------|--------|--------|
        xxxxxxxxx | xxxxxxxx | xxxxxxxx | xxxxxx | xxxxxx | xxxxxx |

        Onde, as novas colunas representam:
            vl_var => Valor da variação de preço entre o dia atual e anterior
            pr_var => % da variação de preço entre o dia atual e anterior
            cd_var => símbolo de "+" ou "-" referente a variação de preço

        """

        df_quotes.reset_index(inplace=True, drop=True)  
        df_quotes = df_quotes.rename(columns=dict_columns)
        df_quotes = df_quotes.sort_values(by=['cd_ativo','dt_cotacao'])
        df_quotes['vl_var'] = df_quotes.vl_fechamento.diff()
        df_quotes['pr_var'] = df_quotes.vl_fechamento.pct_change()*100
        df_quotes['cd_var'] = np.where(df_quotes['vl_var'] > 0, '+', '-')

        # exportar para "csv"
        df_quotes.to_csv(f'{folder}{file_name}.csv', index=False, decimal=',')
        print('\n' + f'FIM: Arquivo salvo em "{folder}{file_name}.csv".')

# --------------------------------------------------
# CHAMAR AS FUNCOES
# --------------------------------------------------

for asset_type in list_asset_type:

    """
    Seguindo os parâmetros do arquivo "config.json", caso o item "incremental"
    esteja marcado como "True", trazer os dados do último ano vigente (atual).
    Se estiver "False", percorrer a listagem de anos entre os parâmetros 
    "ano-inicial" e "ano_final", salvando um arquivo para cada ano.

    """
    
    if list_asset_type[asset_type] != False:
        # buscar as cotacoes historicas
        print(f'INICIO: Buscando dados do tipo de ativo "{asset_type}"')

        folder = config['pasta-dados'] + f'cotacoes/{asset_type}/'

        if year_end == False:
            year_end = date.today().year

        if incremental == True:
            # redefinir variaveis
            dt_start = date(date.today().year, 1, 1).strftime('%d/%m/%Y')
            dt_end = date.today().strftime('%d/%m/%Y')
            file_name = f'{asset_type}_{year_end}'
            
            # buscar os dados apenas do último ano
            Quotes(asset_type, list_asset_type[asset_type], dt_start, dt_end).quotes()  

        else:
            period = list(range(int(year_start), int(year_end) + 1, 1))
            
            for year in period:
                dt_start = date(year, 1, 1).strftime('%d/%m/%Y')
                dt_end = date(year, 12, 31).strftime('%d/%m/%Y')
                file_name = f'{asset_type}_{year}'

                # buscar os dados de forma incremental
                Quotes(asset_type, list_asset_type[asset_type], dt_start, dt_end).quotes()
    else:
        print(f'AVISO: Desconsiderar o tipo de ativo "{asset_type}"')
    