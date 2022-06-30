import pandas as pd 
import numpy as np  
import seaborn as sns
import matplotlib.pyplot as plt 
import wikipedia as wk
import pandas_datareader as wb
import yfinance as yf

html = wk.page('List of S&P 500 companies').html().encode('UTF-8')
df = pd.read_html(html)[0]
cal = 0
simbolos = df['Symbol'].tolist()
sectores = list(set(df['GICS Sector']))
dias = ['Monday','Tuesday','Wednesday','Thursday','Friday']

for i in simbolos:
    if i == 'CEG':
        simbolos.pop(cal)
    if i == 'BRK.B':
        simbolos[cal] = simbolos[cal].replace('.','-')
    if i == 'BF.B':
        simbolos[cal] = simbolos[cal].replace('.','-')
    cal += 1

industry = df[['Symbol','GICS Sector']]
industry = industry.set_index('Symbol')
industry = industry.to_dict()
industry = industry.pop('GICS Sector')
empresa = df[['Symbol','Security']]
empresa = empresa.set_index('Symbol')
empresa = empresa.to_dict()
empresa = empresa.pop('Security')

pruebayf = yf.download(tickers=simbolos, start='2000-01-01', end='2021-12-31', group_by='ticker')

newframe = pruebayf.copy()
newframe = newframe.stack(level=0).rename_axis(['Date', 'Ticker']).reset_index(level=1)
newframe = newframe.reset_index()

newframe['Dia'] = newframe['Date'].dt.day_name()
newframe['retornogaps'] = np.log(newframe.Open/newframe.Close.shift(1))
newframe['retornogaps'] = newframe['retornogaps'].fillna(0)
newframe['retornos_intra'] = np.log(newframe.Close/newframe.Open).fillna(0)
newframe['variaciones'] = newframe['Adj Close'].pct_change().fillna(0)
newframe['volatilidad'] = newframe['variaciones'].rolling(250).std()*100*(250)**0.5
newframe['volatilidad'] = newframe['volatilidad'].fillna(0)

newframe['industry'] = newframe['Ticker'].map(industry)
newframe['Nombre'] = newframe['Ticker'].map(empresa)

retornogaps_sector = pd.DataFrame()

for i in range(len(sectores)):
    x = pd.DataFrame(newframe['retornogaps'][newframe['industry'] == str(sectores[i])]).sum()
    retornogaps_sector = pd.concat([retornogaps_sector, x], axis=1)
retornogaps_sector.columns = sectores
retornogaps_sector = retornogaps_sector.T

retornogaps_sector.plot(kind='bar',figsize=(30,10),rot=30,color='green',legend=False)
plt.show()

retornointra_sector = pd.DataFrame()

for i in range(len(sectores)):
    x = pd.DataFrame(newframe['retornos_intra'][newframe['industry'] == str(sectores[i])]).sum()
    retornointra_sector = pd.concat([retornointra_sector, x], axis=1)
retornointra_sector.columns = sectores
retornointra_sector = retornointra_sector.T

retornointra_sector.plot(kind='bar',figsize=(30,15),rot=30,color='blue',legend=False)
plt.show()

newframe_dias = newframe.copy()
newframe_dias_gap= newframe_dias.groupby('Dia')[['retornogaps']].sum()
newframe_dias_gap = newframe_dias_gap.reindex(dias)
newframe_dias_intra = newframe_dias.groupby('Dia')[['retornos_intra']].sum()
newframe_dias_intra = newframe_dias_intra.reindex(dias)

newframe_dias_gap.plot.line(figsize=(30,10),color='green')
plt.title('Retorno gap por dia')
plt.show()

newframe_dias_intra.plot.line(figsize=(30,10),color='blue')
plt.title('Retorno intra diario')
plt.show()

newframe.plot.line(x='Date',y='volatilidad',figsize=(40,20),lw=1)
plt.title('Grafico de volatilidad a traves del tiempo')
plt.show()

top_retorno_intra= newframe.groupby(['Ticker','Nombre']).retornos_intra.sum()
top_retorno_intra= top_retorno_intra.to_frame()
top_retorno_intra.sort_values(by=['retornos_intra'], inplace=True, ascending=False)
top_retorno_intra = top_retorno_intra.head(13)

colores=['red','blue','green','grey','orange','yellow','cyan']

top_retorno_intra.plot(kind='bar',figsize=(30,10),y='retornos_intra',
                        xlabel='Empresas', ylabel='Retorno intra diario', legend=False,
                        color=colores
)
plt.title('TOP 13 empresas para invertir segun el retorno intra diario')
plt.show()

top_retorno_gap= newframe.groupby(['Ticker','Nombre']).retornogaps.sum()
top_retorno_gap= top_retorno_gap.to_frame()
top_retorno_gap.sort_values(by=['retornogaps'], inplace=True, ascending=False)
top_retorno_gap = top_retorno_gap.head(13)

top_retorno_gap.plot(kind='bar',figsize=(30,10),y='retornogaps',
                        xlabel='Empresas', ylabel='Retorno gap', legend=False,
                        color=colores
)
plt.title('Top 13 empresas para invertir segun retorno gap')
plt.show()
