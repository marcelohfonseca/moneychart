# Investing Go | Marcelo Henrique Fonseca => https://github.com/marcelohfonseca
# Consulte os arquivos README.md e LICENSE.md para detalhes.

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

    ano_inicial = config['periodo']['ano-inicial']
    ano_final = config['periodo']['ano-final']
    incremental = config['periodo']['incremental']
    lista_ativos = config['filtrar-ativos']['tesouro']

class Tesouro:
    # tabela base para o historico de cotacoes

    def __init__(self, dt_inicial, dt_final):
        self.dt_inicial = dt_inicial
        self.dt_final = dt_final
        self.dict_columns = {'Data Base': 'dt_cotacao', 
                             'Data Vencimento': 'dt_vencimento', 
                             'Codigo': 'cd_ativo', 
                             'Taxa Compra Manha': 'pr_compra', 
                             'Taxa Venda Manha': 'pr_venda',
                             'PU Compra Manha': 'vl_compra',
                             'PU Venda Manha': 'vl_venda',
                             'PU Base Manha': 'vl_base'}
        self.df_cotacoes = pd.DataFrame(columns=self.dict_columns.values())
        self.df_titulos = pd.read_csv('../de_para_tesouro.csv', sep=';')
        self.lista_erros = []

    # buscar o historico de precos e taxas
    def precos_taxas(self):

        link_precos_taxas = 'https://www.tesourotransparente.gov.br/ckan/dataset/df56aa42-484a-4a59-8184-7676580c81e3/resource/796d2059-14e9-44e3-80c9-2d9e30b405c1/download/PrecoTaxaTesouroDireto.csv'

        try:
            df_historico = pd.read_csv(link_precos_taxas, sep=';', decimal=',')
            df_historico = df_historico.merge(self.df_titulos, )
            df_historico['Codigo'] = df_historico['Codigo'] + df_historico['Data Vencimento'].str[-4:]
            df_historico['Data Vencimento'] = pd.to_datetime(df_historico['Data Vencimento'], dayfirst=True)
            df_historico['Data Base'] = pd.to_datetime(df_historico['Data Base'], dayfirst=True)            
            df_historico = df_historico.rename(columns=self.dict_columns).drop(columns='Tipo Titulo')
            self.df_cotacoes = self.df_cotacoes.append(df_historico)
        except:
            self.lista_erros.append(self.dict_columns.items())

        # imprime a lista de erros
        if len(self.lista_erros) > 0:
            print(f'Não foi encontrado dados entre {self.dt_inicial} e {self.dt_final} para estes ativos: {self.lista_erros}')

        return Tesouro.salvar_arquivo(self.df_cotacoes, self.dt_inicial, self.dt_final)
        
    # renomear colunas, classificar e exportar os dados  
    def salvar_arquivo(df_cotacoes, dt_inicial, dt_final):
        df_cotacoes.reset_index(inplace=True, drop=True)
        df_cotacoes = df_cotacoes[(df_cotacoes['dt_cotacao'] >= dt_inicial) & (df_cotacoes['dt_cotacao'] <= dt_final)]
        df_cotacoes = df_cotacoes.sort_values(by=['cd_ativo','dt_cotacao'])
        df_cotacoes['vl_var'] = df_cotacoes.vl_compra.diff()
        df_cotacoes['pr_var'] = df_cotacoes.vl_compra.pct_change()*100
        df_cotacoes['cd_var'] = np.where(df_cotacoes['vl_var'] > 0, '+', '-')
        df_cotacoes.to_csv(f'{diretorio}{nome_arquivo}.csv', index=False, decimal=',')
        print(f'Arquivo salvo em "{diretorio}{nome_arquivo}.csv".')

if lista_ativos != False:
    # buscar as cotacoes historicas
    print(f'Buscando dados do tipo de ativo tesouro-direto')

    diretorio = config['pasta-dados'] + f'cotacoes/tesouro/'

    if ano_final == False:
        ano_final = date.today().year

    periodo = list(range(int(ano_inicial), int(ano_final) + 1, 1))
    
    for ano in periodo:
        dt_inicial = date(ano, 1, 1).strftime('%d/%m/%Y')
        dt_final = date(ano, 12, 31).strftime('%d/%m/%Y')
        nome_arquivo = f'tesouro_precos_taxas_{ano}'

        # buscar os dados de forma incremental
        Tesouro(dt_inicial, dt_final).precos_taxas() 
else:
    print(f'Desconsiderar o tipo de ativo tesouro-direto')
    