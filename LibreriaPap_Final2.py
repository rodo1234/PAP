import datetime
import time
import requests
"""
        Este es nuestro archivo interno que contiene el total de las funciones que utilizaremos para este proyecto:
        income_statements_extern: nos brinda la informacion del income statement de los tickers que llamamos
        balance_sheet_extern: nos brinda la informacion de los balance sheet de los tickers que llamamos
"""
####### primera libreria para el income statement
def income_statements_extern(tickers, ALPHAVANTAGETOKEN):
    income_statements = {}  # creamos el diccionario que almacenara todos los datos de los tickers
    def get_latest_price(ticker: str, date: str):#funcion reutilizada de clase
        start = datetime.datetime.strptime(date, "%Y-%m-%d") - datetime.timedelta(days=252)
        return yf.Ticker(ticker).history(start=start, end=date)["Close"][-1]

    for ticker in tickers:#iteramos por medio de los tickers
        url = f'https://www.alphavantage.co/query?function=INCOME_STATEMENT&symbol={ticker}&apikey={ALPHAVANTAGETOKEN}'
        r = requests.get(url)
    
        if r.status_code == 200:
            income_statement = r.json()
            income_statements[ticker] = income_statement
        else:
            print(f"Sin información para: {ticker}. Código de estado: {r.status_code}")
            
            
        time.sleep(2) #con time sleep esperamos para que vuelva ejecutar despues de 20 seg el codigo
   # que stickers se descargaron
    print(f"Descargados los income statements de: {', '.join(tickers)}")
    # Retorna el diccionario con los ingresos y las listas de tickers descargados y fallados
    return income_statements


### segunda funcion para los balance sheet
def balance_sheet_extern(tickers, ALPHAVANTAGETOKEN):
    balance_sheets = {}  # creamos el diccionario que almacenara todos los datos de los tickers
    def get_latest_price(ticker: str, date: str):#funcion reutilizada de clase
        start = datetime.datetime.strptime(date, "%Y-%m-%d") - datetime.timedelta(days=252)
        return yf.Ticker(ticker).history(start=start, end=date)["Close"][-1]

    for ticker in tickers:#iteramos por medio de los tickers
        url = f'https://www.alphavantage.co/query?function=BALANCE_SHEET&symbol={ticker}&apikey={ALPHAVANTAGETOKEN}'
        r = requests.get(url)
    
        if r.status_code == 200:
            balance_sheet = r.json()
            balance_sheets[ticker] = balance_sheet
        else:
            print(f"Sin información para: : {ticker}. Código de estado: {r.status_code}")
            
            
        time.sleep(2) #con time sleep esperamos para que vuelva ejecutar despues de 12 seg el codigo
   # que stickers se descargaron
    print(f"Descargados los balance sheet de: {', '.join(tickers)}")
    # Retorna el diccionario con los ingresos y las listas de tickers descargados y fallados
    return balance_sheets

### tercera funcion para los cashflow sheet
def cashflow_sheet_extern(tickers, ALPHAVANTAGETOKEN):
    cashflow_sheets = {}  # creamos el diccionario que almacenara todos los datos de los tickers
    def get_latest_price(ticker: str, date: str):#funcion reutilizada de clase
        start = datetime.datetime.strptime(date, "%Y-%m-%d") - datetime.timedelta(days=252)
        return yf.Ticker(ticker).history(start=start, end=date)["Close"][-1]

    for ticker in tickers:#iteramos por medio de los tickers
        url = f'https://www.alphavantage.co/query?function=CASH_FLOW&symbol={ticker}&apikey={ALPHAVANTAGETOKEN}'
        r = requests.get(url)
    
        if r.status_code == 200:
            cashflow_sheet = r.json()
            cashflow_sheets[ticker] = cashflow_sheet
        else:
            print(f"Sin información para: : {ticker}. Código de estado: {r.status_code}")
            
            
        time.sleep(2) #con time sleep esperamos para que vuelva ejecutar despues de 12 seg el codigo
   # que stickers se descargaron
    print(f"Descargados los cashflow sheet de: {', '.join(tickers)}")
    # Retorna el diccionario con los ingresos y las listas de tickers descargados y fallados
    return cashflow_sheets

def adjusted_close_extern(tickers, ALPHAVANTAGETOKEN):
    adjusted_closes = {}  # creamos el diccionario que almacenara todos los datos de los tickers
    def get_latest_price(ticker: str, date: str):#funcion reutilizada de clase
        start = datetime.datetime.strptime(date, "%Y-%m-%d") - datetime.timedelta(days=252)
        return yf.Ticker(ticker).history(start=start, end=date)["Close"][-1]

    for ticker in tickers:#iteramos por medio de los tickers
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={ticker}&outputsize=full&apikey={ALPHAVANTAGETOKEN}'
        r = requests.get(url)
    
        if r.status_code == 200:
            adjusted_close = r.json()
            adjusted_closes[ticker] = adjusted_close
        else:
            print(f"Sin información para: {ticker}. Código de estado: {r.status_code}")
            
            
        time.sleep(2) #con time sleep esperamos para que vuelva ejecutar despues de 20 seg el codigo
   # que stickers se descargaron
    print(f"Descargados los adjusted_closes de: {', '.join(tickers)}")
    # Retorna el diccionario con los ingresos y las listas de tickers descargados y fallados
    return adjusted_closes

"""
    ----------------- A PARTIR DE AQUI TENDREMOS LOS FUNDAMENTALES CALCULADOS -------
"""
def pad_list(li, len_list):
    return (len_list - len(li)) * [0] + li


import numpy as np
from tqdm import tqdm
def fundamentals(tickers, income_statements, balance_sheets,cashflow_sheets,adjusted_closes):
    fundamental_results = {}
    
    with tqdm(tickers, desc="Procesando tickers") as pbar:

        for ticker in pbar:
            income_statement = income_statements.get(ticker)
            balance_sheet = balance_sheets.get(ticker)
            cashflow_sheet = cashflow_sheets.get(ticker)
            adjusted_close = adjusted_closes.get(ticker)

            if income_statement and balance_sheet and cashflow_sheet and adjusted_close:
                # LLAMAMOS A NET INCOME Y INCOME BEFORE TAX 
                Nincome = [income_statement["quarterlyReports"][i]['netIncome'] for i in range(len(income_statement['quarterlyReports']))][-20:][::-1]
                Nincome = np.array(pad_list(Nincome, 20))
                Nincome = np.where(Nincome == 'None', '0', Nincome).astype(np.float64)
                Tincome = [income_statement["quarterlyReports"][i]['incomeBeforeTax'] for i in range(len(income_statement['quarterlyReports']))][-20:][::-1]
                Tincome = np.array(pad_list(Tincome, 20))
                Tincome = np.where(Tincome == 'None', '0', Tincome).astype(np.float64)
                # CALCULAMOS NER
                NER = np.array([ni / ti if ti != 0 else 0 for ni, ti in zip(Nincome, Tincome)])
                # NER = Nincome / Tincome if (Tincome != 0).any() else 0

                # CALCULAMOS ROA
                Tassets = [balance_sheet["quarterlyReports"][i]['totalAssets'] for i in range(len(balance_sheet['quarterlyReports']))][-20:][::-1] #totalnetassets
                Tassets = np.array(pad_list(Tassets, 20))
                Tassets = np.where(Tassets == 'None', '0', Tassets).astype(np.float64)
                ROA = np.array([ni / ta if ta != 0 else 0 for ni, ta in zip(Nincome, Tassets)])
                # ROA = Nincome / Tassets if (Tassets != 0).any() else 0

                #Componentes STLT NO USADO
                STD = [(balance_sheet["quarterlyReports"][i]['shortTermDebt']) for i in range(len(balance_sheet['quarterlyReports']))][-20:][::-1]
                STD = np.array(pad_list(STD, 20))
                STD = np.where(STD == 'None', '0', STD).astype(np.float64)

                LTD = [(balance_sheet["quarterlyReports"][i]['longTermDebt']) for i in range(len(balance_sheet['quarterlyReports']))][-20:][::-1]
                LTD = np.array(pad_list(LTD, 20))
                LTD = np.where(LTD == 'None', '0', LTD).astype(np.float64)

                #Calculamos STLT
                #STLT = np.array([std / ltd if ltd != 0 else 0 for std, ltd in zip(STD, LTD)])
                # STLT = STD / LTD if (LTD != 0).any() else 0

                #Componentes CUR NO UASO

                CA = [balance_sheet["quarterlyReports"][i]['totalCurrentAssets'] for i in range(len(balance_sheet['quarterlyReports']))][-20:][::-1]
                CA = np.array(pad_list(CA, 20))
                CA = np.where(CA == 'None', '0', CA).astype(np.float64)

                CL = [balance_sheet["quarterlyReports"][i]['totalCurrentLiabilities'] for i in range(len(balance_sheet['quarterlyReports']))][-20:][::-1]
                CL = np.array(pad_list(CL, 20))
                CL = np.where(CL == 'None', '0', CL).astype(np.float64)

                #Calculamos CUR
                #CUR = np.array([ca / cl if cl != 0 else 0 for ca, cl in zip(CA, CL)])
                # CUR = CA / CL if (CL != 0).any() else 0

                #Componentes DER

                TL = [balance_sheet["quarterlyReports"][i]['totalLiabilities'] for i in range(len(balance_sheet['quarterlyReports']))][-20:][::-1]
                TL = np.array(pad_list(TL, 20))
                TL = np.where(TL == 'None', '0', TL).astype(np.float64)

                TSE = [balance_sheet["quarterlyReports"][i]['totalShareholderEquity'] for i in range(len(balance_sheet['quarterlyReports']))][-20:][::-1]
                TSE = np.array(pad_list(TSE, 20))
                TSE = np.where(TSE == 'None', '0', TSE).astype(np.float64)

                #Calculo DER
                DER = np.array([tl / tse if tse != 0 else 0 for tl, tse in zip(TL, TSE)])
                # DER = TL / TSE if (TSE != 0).any() else 0

                #EBITDA

                EBITDA = [(income_statement["quarterlyReports"][i]['ebitda']) for i in range(len(income_statement['quarterlyReports']))][-20:][::-1]
                EBITDA = np.array(pad_list(EBITDA, 20))
                EBITDA = np.where(EBITDA == 'None', '0', EBITDA).astype(np.float64)


                #Componentes CER

                CE = [(cashflow_sheet["quarterlyReports"][i]['capitalExpenditures']) for i in range(len(cashflow_sheet['quarterlyReports']))][-20:][::-1]
                CE = np.array(pad_list(CE, 20))
                CE = np.where(CE == 'None', '0', CE).astype(np.float64)

                #Calculo CER
                CER = np.array([ce / ni if ni != 0 else 0 for ce, ni in zip(CE, Nincome)])
                # CER = CE / Nincome if (Nincome != 0).any() else 0

                #Componentes GPM 

                GP = [income_statement["quarterlyReports"][i]['grossProfit'] for i in range(len(income_statement['quarterlyReports']))][-20:][::-1]
                GP = np.array(pad_list(GP, 20))
                GP = np.where(GP == 'None', '0', GP).astype(np.float64)

                TR = [income_statement["quarterlyReports"][i]['totalRevenue'] for i in range(len(income_statement['quarterlyReports']))][-20:][::-1]
                TR = np.array(pad_list(TR, 20))
                TR = np.where(TR == 'None', '0', TR).astype(np.float64)

                COGS = [(income_statement["quarterlyReports"][i]['costofGoodsAndServicesSold']) for i in range(len(income_statement['quarterlyReports']))][-20:][::-1]
                COGS = np.array(pad_list(COGS, 20))
                COGS = np.where(COGS == 'None', '0', COGS).astype(np.float64)

                GI = TR - COGS

                #Calculo GMR
                GMR = np.array([gi / tr if tr != 0 else 0 for gi, tr in zip(GI, TR)])
                # GPM = GI / GP if (GP != 0).any() else 0

                #Componentes FCF
                # NOPAT = EBIT - TAX
                # WC = CA - CL
                # FCF = NOPAT + DE - (WC + CE)

                TAX = [income_statement["quarterlyReports"][i]['incomeTaxExpense'] for i in range(len(income_statement['quarterlyReports']))][-20:][::-1]
                TAX = np.array(pad_list(TAX, 20))
                TAX = np.where(TAX == 'None', '0', TAX).astype(np.float64)

                EBIT = [(income_statement["quarterlyReports"][i]['ebit']) for i in range(len(income_statement['quarterlyReports']))][-20:][::-1]
                EBIT = np.array(pad_list(EBIT, 20))
                EBIT = np.where(EBIT == 'None', '0', EBIT).astype(np.float64)

                DE = [income_statement["quarterlyReports"][i]['depreciation'] for i in range(len(income_statement['quarterlyReports']))][-20:][::-1]
                DE = np.array(pad_list(DE, 20))
                DE = np.where(DE == 'None', '0', DE).astype(np.float64)

                NOPAT = EBIT - TAX
                WC = CA - CL

                #Calculo FCF NO USADO

                #FCF = NOPAT + DE - (WC-CE)

                # Calculo SOL
                SOL = np.array([ta / tl if tl != 0 else 0 for ta, tl in zip(Tassets, TL)])
                # SOL = Tassets / TL if (TL != 0).any() else 0

                #Componentes QR
                #Quick Assets = CA - INV

                INV = [balance_sheet["quarterlyReports"][i]['inventory'] for i in range(len(balance_sheet['quarterlyReports']))][-20:][::-1]
                INV = np.array(pad_list(INV, 20))
                INV = np.where(INV == 'None', '0', INV).astype(np.float64)


                QA = CA - INV

                #Caluclo QR
                QR = np.array([qa / cl if cl != 0 else 0 for qa, cl in zip(QA, CL)])
                # QR = QA / CL if (CL != 0).any() else 0

                # CALCULO DE BVPS
                EQ = [balance_sheet["quarterlyReports"][i]['totalShareholderEquity'] for i in range(len(balance_sheet['quarterlyReports']))][-20:][::-1]
                EQ = np.array(pad_list(EQ, 20))
                EQ = np.where(EQ == 'None', '0', EQ).astype(np.float64)

                SH = [balance_sheet["quarterlyReports"][i]['commonStockSharesOutstanding'] for i in range(len(balance_sheet['quarterlyReports']))][-20:][::-1]
                SH = np.array(pad_list(SH, 20))
                SH = np.where(SH == 'None', '0', SH).astype(np.float64)

                BVPS = np.array([eq / sh if sh != 0 else 0 for eq, sh in zip(EQ, SH)])


                #Fechas
                adj_dates = [fecha for fecha in adjusted_close.get('Time Series (Daily)', {})]
                dates = [income_statement['quarterlyReports'][i]['fiscalDateEnding'] for i in range(len(income_statement['quarterlyReports']))][-20:][::-1]
                dates = np.array(pad_list(dates, 20))    
                dates = [fecha if fecha != '0' else None for fecha in dates]
                synced_dates = [min(adj_dates, key=lambda d: abs(pd.to_datetime(d) - pd.to_datetime(fecha))) if fecha is not None else None for fecha in dates]
                #synced_dates = [min(adj_dates, key=lambda d: abs(pd.to_datetime(d) - pd.to_datetime(fecha))) for fecha in dates]
                #adjusted_close

                ADJ = [adjusted_close['Time Series (Daily)'][fecha]['5. adjusted close'] if fecha in adjusted_close.get('Time Series (Daily)', {}) else 'None' for fecha in synced_dates]
                ADJ = np.array(pad_list(ADJ, 20))
                ADJ = np.where(ADJ == 'None', '0', ADJ).astype(np.float64)



                # CALCULO DE P/E
                GPA = np.array([ni / sh if sh != 0 else 0 for ni, sh in zip(Nincome, SH)]) #Ganancias por acción NetIncome/TotalAcciones
                PE = np.array([adj / gpa if gpa != 0 else 0 for adj, gpa in zip(ADJ, GPA)]) #Precio / Ganancia por acción





                # Almacena los resultados en el diccionario fundamental_results
                fundamental_results[ticker] = {"NER": NER.tolist(),"ROA": ROA.tolist(), "DER": DER.tolist(),"CER": CER.tolist(),"GMR": GMR.tolist(), "SOL": SOL.tolist(),
                                               "QR": QR.tolist(),'BVPS': BVPS.tolist(),"PE": PE.tolist(),"dates": dates,'synced_dates': synced_dates,'ADJ': ADJ}
                
                pbar.set_postfix({"Ticker": ticker})
                
            else:
                print(f"Error al calcular: {ticker}")

    return fundamental_results


import yfinance as yf
import pandas as pd

def get_returns(tickers, start_date=None, end_date=None, freq=None):
    closes = yf.download(tickers, start=start_date, end=end_date)['Adj Close']
    closes.sort_index(inplace=True)
    closes = closes.reset_index()
    fechas = ['2018-06-29','2018-09-28', '2018-12-31', '2019-03-29', '2019-06-28', '2019-09-30', '2019-12-31', '2020-03-31', '2020-06-30', '2020-09-30',
            '2020-12-31', '2021-03-31', '2021-06-30', '2021-09-30', '2021-12-31', '2022-03-31', '2022-06-30', '2022-09-30', '2022-12-30',
            '2023-03-31','2023-06-30']
    fechas2 = [pd.to_datetime(fecha) for fecha in fechas]

    closes_cuarto = closes[closes['Date'].isin(fechas2)]
    precios = closes_cuarto.drop(['Date'], axis=1)
    rendimientos = precios.pct_change()
    rendimientos.insert(0, 'Date', closes_cuarto['Date'])
    rend = rendimientos.melt(id_vars='Date', var_name='Accion', value_name='Rendimientos')
    rend['Decisión'] = rend['Rendimientos'].apply(lambda x: 1 if x > 0 else 0)
    rend = rend.fillna(0)
    fecha_a_eliminar = '2018-06-29'
    rend = rend[rend['Date'] != fecha_a_eliminar]
    return rend