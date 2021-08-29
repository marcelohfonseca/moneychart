# Investing Go | Marcelo Henrique Fonseca => https://github.com/marcelohfonseca
# Consulte os arquivos README.md e LICENSE.md para detalhes.

# --------------------------------------------------
# IMPORTAR BIBLIOTECAS E PARAMETROS INICIAIS
# --------------------------------------------------

import json
import pandas as pd
import numpy as np
from datetime import date
    
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
    list_symbol = config['filtrar-ativos']['tesouro']

# --------------------------------------------------
# FUNCOES DE CONSULTA E TRATAMENTO DOS DADOS
# --------------------------------------------------

class Tesouro:

    """
    O site do tesouro direto fornece um único arquivo para download, 
    contendo todos os títulos nos últimos anos, seus preços e taxas.
    O script faz o download e separa o "csv" em um arquivo 
    para cada ano.

    O arquivo ontém os seguintes dados:

    Tipo Titulo | Data Vcto | Data Base | Taxa Compra | Taxa Venda | =>
    ------------|-----------|-----------|-------------|------------|
    xxxxxxxxxxx | xxxxxxxxx | xxxxxxxxx | xxxxxxxxxxx | xxxxxxxxxx | 

    PU Compra | PU Venda | PU Base |
    ----------|----------|---------|
    xxxxxxxxx | xxxxxxxx | xxxxxxx |

    """

    def __init__(self, dt_start, dt_end):
        self.dt_start = dt_start
        self.dt_end = dt_end
        self.dict_columns = {'Data Base': 'dt_cotacao', 
                             'Data Vencimento': 'dt_vencimento', 
                             'Codigo': 'cd_ativo', 
                             'Taxa Compra Manha': 'pr_compra', 
                             'Taxa Venda Manha': 'pr_venda',
                             'PU Compra Manha': 'vl_compra',
                             'PU Venda Manha': 'vl_venda',
                             'PU Base Manha': 'vl_base'}
        self.df_quotes = pd.DataFrame(columns=self.dict_columns.values())
        self.df_titulos = pd.read_csv('../de_para_tesouro.csv', sep=';')
        self.lista_erros = []

    # buscar o historico de precos e taxas
    def prices(self):

        """
        Para cada consulta, além dos campos padrões retornados,  será 
        adicionado a coluna "cd_ativo", concatenando o código do ativo,
        que virá do arquivo "de_para_tesouro.csv" + "ano de vencimento".

        Caso não seja encontrado o ativo pela consulta, o Ticker (código
        do ativo) será inserido em uma lista de erros, para ser listado
        ao fim da execução desta função.

        """
        
        url = 'https://www.tesourotransparente.gov.br/ckan/dataset/df56aa42-484a-4a59-8184-7676580c81e3/resource/796d2059-14e9-44e3-80c9-2d9e30b405c1/download/PrecoTaxaTesouroDireto.csv'

        try:
            df_history = pd.read_csv(url, sep=';', decimal=',')
            df_history = df_history.merge(self.df_titulos, )
            df_history['Codigo'] = df_history['Codigo'] + df_history['Data Vencimento'].str[-4:]
            df_history['Data Vencimento'] = pd.to_datetime(df_history['Data Vencimento'], dayfirst=True)
            df_history['Data Base'] = pd.to_datetime(df_history['Data Base'], dayfirst=True)            
            df_history = df_history.rename(columns=self.dict_columns).drop(columns='Tipo Titulo')
            self.df_quotes = self.df_quotes.append(df_history)
        except:
            self.lista_erros.append(self.dict_columns.items())

        # imprime a lista de erros caso exista
        if len(self.lista_erros) > 0:
            print(f'Não foi encontrado dados entre {self.dt_start} e {self.dt_end} para estes ativos: {self.lista_erros}')

        return Tesouro.save_file(self.df_quotes, self.dt_start, self.dt_end)
        
    # renomear colunas, classificar e exportar os dados  
    def save_file(df_quotes, dt_start, dt_end):

        """
        Essa função tem o objetivo de pegar o dataframe retornado pela
        função "quotes" e tratar, renomeando as colunas, classificando
        os dados e também adicionando as colunas de variação do preço
        de fechamento, em valor e percentual.

        O dataframe final deve ficar como:

        dt_cotacao | dt_vencimento | cd_ativo | pr_compra | pr_venda | =>
        -----------|---------------|----------|-----------|----------|
        xxxxxxxxxx | xxxxxxxxxxxxx | xxxxxxxx | xxxxxxxxx | xxxxxxxx | 

        vl_compra | vl_venda | vl_base | vl_var | pr_var | cd_var |
        ----------|----------|---------|--------|--------|--------|
        xxxxxxxxx | xxxxxxxx | xxxxxxx | xxxxxx | xxxxxx | xxxxxx |

        Onde, as novas colunas representam:
            vl_var => Valor da variação de preço entre o dia atual e anterior
            pr_var => % da variação de preço entre o dia atual e anterior
            cd_var => símbolo de "+" ou "-" referente a variação de preço

        """

        df_quotes.reset_index(inplace=True, drop=True)
        df_quotes = df_quotes[(df_quotes['dt_cotacao'] >= dt_start) & (df_quotes['dt_cotacao'] <= dt_end)]
        df_quotes = df_quotes.sort_values(by=['cd_ativo','dt_cotacao'])
        df_quotes['vl_var'] = df_quotes.vl_compra.diff()
        df_quotes['pr_var'] = df_quotes.vl_compra.pct_change()*100
        df_quotes['cd_var'] = np.where(df_quotes['vl_var'] > 0, '+', '-')

        # exportar para "csv"
        df_quotes.to_csv(f'{folder}{nome_arquivo}.csv', index=False, decimal=',')
        print(f'Arquivo salvo em "{folder}{nome_arquivo}.csv".')

# --------------------------------------------------
# CHAMAR AS FUNCOES
# --------------------------------------------------

if list_symbol != False:
    # buscar as cotacoes historicas
    print(f'Buscando dados do tipo de ativo tesouro-direto')

    folder = config['pasta-dados'] + f'cotacoes/tesouro/'

    if year_end == False:
        year_end = date.today().year

    period = list(range(int(year_start), int(year_end) + 1, 1))
    
    for year in period:
        dt_start = date(year, 1, 1).strftime('%d/%m/%Y')
        dt_end = date(year, 12, 31).strftime('%d/%m/%Y')
        nome_arquivo = f'tesouro_precos_taxas_{year}'

        # buscar os dados de forma incremental
        Tesouro(dt_start, dt_end).prices() 
else:
    print(f'Desconsiderar o tipo de ativo tesouro-direto')
    