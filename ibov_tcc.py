import sys
sys.path.insert(0, '../0. Modulos')
import pandas as pd
import requests
import time
import datetime
from sqlalchemy import create_engine
from sqlalchemy.sql import text

def _log(texto):
    print("[{time}] {texto}".format(time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), texto=texto))


def extract_data(ticker):
    symbol = ticker+'.SAO'
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&outputsize=full&apikey=QMRWACDBIWJOY4KG'
    payload = {}
    response = requests.request("GET", url=url, data=payload).json()
    data = response['Time Series (Daily)']
    raw_data = pd.DataFrame.from_dict(data, orient='index').reset_index()

    return raw_data


def format_data(df, ticker):
    dict_columns = {'index': 'date',
                    '1. open': 'open',
                    '2. high': 'high',
                    '3. low': 'low',
                    '4. close': 'close',
                    '5. volume': 'volume',}

    df.insert(0, 'ticker', ticker)
    df.rename(columns=dict_columns, inplace = True)
    df['date'] = pd.to_datetime(df['date'])
    df = df[df.date >= '2018-01-01']

    return df


def clean_database(ticker, engine):
    texto_delete = f"""DELETE FROM ODS.tb_time_series WHERE ticker LIKE '{ticker}' """
    apagar_base = text(texto_delete)
    engine.execute(apagar_base)
    _log('Registros deletados')


def insert_dataframe(df, engine):
        if  df.shape[0] > 10:
            df.to_sql(con = engine, schema = 'ODS', name='tb_time_series', if_exists = 'append', index = False)
            time.sleep(12) #tempo de espera para próxima requisição
        else:
            _log('Dataframe inválido')


def main():
    db_bi_engine = create_engine('mysql+pymysql://{}:{}@{}/{}'.format('[USER]','[SENHA]', '[ENDPOIND]','[SCHEMA]'))

    ticker_list = ['VALE3', 'PETR4', 'ITUB4','BBDC4','PETR3','B3SA3','ABEV3','ELET3','RENT3','BBAS3',
    'WEGE3','ITSA4','SUZB3','BPAC11','HAPV3','EQTL3','GGBR4','LREN3','RDOR3','RADL3','PRIO3',
    'BBDC3','RAIL3','ENEV3','SBSP3','VBBR3','BBSE3','CSAN3','HYPE3','VIVT3','TOTS3','ASAI3',
    'BRFS3','CMIG4','KLBN11','CCRO3','UGPA3','ELET6','MGLU3','ENGI11','NTCO3','SANB11','AMER3',
    'EGIE3','CPLE6','TIMS3','CRFB3','BRKM5','TAEE11','EMBR3','RRRP3','BRML3','CSNA3','GOAU4',
    'SOMA3','MULT3','CPFE3','CIEL3','BRAP4','SULA11','ARZZ3','FLRY3','ENBR3','VIIA3','COGN3',
    'RAIZ4','CYRE3','AZUL4','IGTI11','SLCE3','ALPA4','LWSA3','YDUQ3','USIM5','SMTO3','CMIN3',
    'BEEF3','MRFG3','MRVE3','PCAR3','PETZ3','IRBR3','DXCO3','QUAL3','EZTC3','GOLL4','CVCB3',
    'ECOR3','POSI3','CASH3']    

    for ticker in ticker_list:
        _log(f'------- ticker: {ticker} -------')
        raw_data = extract_data(ticker=ticker)
        df = format_data(raw_data, ticker=ticker)
        clean_database(ticker=ticker, engine=db_bi_engine)
        insert_dataframe(df=df, engine = db_bi_engine)

    _log('Execução concluída')  

main()