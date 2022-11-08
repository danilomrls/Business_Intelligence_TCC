import sys
sys.path.insert(0, '../0. Modulos')
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import fundamentus

def extract_data(ticker):
    raw_data = fundamentus.get_papel(ticker)
    return raw_data


def format_data(data, ticker):
    df = data
    df.insert(0, 'ticker', ticker)
    return df


def insert_data(df, engine):
    df.to_sql(con = engine, schema = 'ODS', name='tb_fundamentus', if_exists = 'append', index = False)


def clean_database(engine):
    texto_delete = f"""DELETE FROM ODS.tb_fundamentus """
    apagar_base = text(texto_delete)
    engine.execute(apagar_base)


def main():
    df_final = pd.DataFrame()
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
        print(f'------- ticker: {ticker} -------')
        raw_data = extract_data(ticker=ticker)
        df = format_data(data=raw_data, ticker=ticker)        
        df_final = pd.concat([df_final, df])
        
    clean_database(engine=db_bi_engine)
    insert_data(df=df_final, engine=db_bi_engine)
    print('Execução concluída')    
    #pd.set_option('display.max_columns', None)
    #display(df_final)
    
main()