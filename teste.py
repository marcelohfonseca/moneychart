import pandas as pd
from pandas_datareader import data as web

df = pd.DataFrame()

acao = 'ITUB3.SA'
 
# importar dados para o DataFrame
df = web.DataReader(acao, data_source='yahoo', start='01-01-2021')
 
# ver as 5 primeiras entradas
df.head()