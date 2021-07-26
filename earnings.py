import requests, pandas as pd,datetime, numpy as np
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from bs4 import BeautifulSoup

def converter(ds):
    w=ds+relativedelta(months=-3)
    w=w.year
    x=ds.month
    if x<=3:
        p=datetime.date(w,12,31)
    elif x<=6:
        p=datetime.date(w,3,31)
    elif x<=9:
        p=datetime.date(w,6,30)
    elif x<=12:
        p=datetime.date(w,9,30)
    return p
def get_earning_time(sym):
    df = pd.read_csv(f'https://www1.nseindia.com/corporates/datafiles/AN_{sym}_MORE_THAN_3_MONTHS.csv')
    df.columns = ['symbol','company','industry','subject','date']
    df = df[(df['subject'].str.contains("ESULT")) | (df['subject'].str.contains("esult")) | (df['subject'].str.contains("ividend")) | (df['subject'].str.contains("UDITED")) | (df['subject'].str.contains("udited")) | (df['subject'].str.contains("IVIDEND"))]
    df['dt'] = df['date'].apply(lambda x: parse(x))
    df['qend'] = df['dt'].apply(lambda x: converter(x))
    df['year'] = (df['dt'].apply(lambda x: x+relativedelta(months=-3)).dt.year)
    df['date'] = df['dt'].dt.date
    df['date'] = pd.to_datetime(df.date)
    df.sort_values(by='dt', inplace=True)
    df.drop_duplicates(subset=['qend','year'], inplace=True)
    return df

def fetch_events(s):
    url = 'https://www1.nseindia.com/marketinfo/companyTracker/boardMeeting.jsp?symbol=' + s 
    headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:71.0) Gecko/20100101 Firefox/71.0"}
    page = requests.get(url,headers=headers)
    if page.ok:
        soup = BeautifulSoup(page.content, 'html.parser')
        for body in soup('tbody'):
            body.unwrap()
        df = pd.read_html(str(soup), flavor="bs4")
        if len(df):
            earnings_df = df[2]
            earnings_df.columns = earnings_df.iloc[0]
            earnings_df = earnings_df.reindex(earnings_df.index.drop(0))
            earnings_df = earnings_df.rename(columns = {'Meeting Date': 'ds', 'Meeting Purpose' : 'event'})
            earnings_df['ds'] = pd.to_datetime(earnings_df['ds'])
            buybacks = earnings_df.loc[((earnings_df['event'].str.contains('uyback'))) ]
            earnings_df = earnings_df.loc[(earnings_df['event'].str.contains('esults')) | (earnings_df['event'].str.contains('ESULTS')) | (earnings_df['event'].str.contains("UDITED")) | (earnings_df['event'].str.contains("udited")) | (earnings_df['event'].str.contains("ccount")) | (earnings_df['event'].str.contains("CCOUNT"))]
            earnings_df['event'] = ['Results'] * len(earnings_df)
            earnings_df.sort_values(by='ds', inplace=True)
            earnings_df['qend'] = earnings_df['ds'].apply(converter)
            earnings_df = earnings_df.drop_duplicates(subset=['qend'], keep='first')
            return(earnings_df, buybacks)
    return(pd.DataFrame(), pd.DataFrame())


def earnings_history_dt(sym):
    df = get_earning_time(sym)
    df1,df2 = fetch_events(sym)
    earn=df.merge(df1,on='qend',how='outer') #Done to pad missing datetime with just date happens pre 2010 often
    earn['date']= np.where(earn['date'].isnull(), earn['ds'],earn['date'])
    earn['dt']= np.where(earn['dt'].isnull(), earn['ds'],earn['dt'])
    earn['dt'] = pd.to_datetime(earn['dt'])
    earn['q_year']=pd.to_datetime(earn['qend']).dt.year
    earn['q_month']=pd.to_datetime(earn['qend']).dt.month
    earn['time'] = earn['dt'].dt.time
    cols = ['date','dt','qend','time','q_year','q_month']
    earn = earn[cols]
    earn['symbol'] = sym
    earn.sort_values(by='qend',inplace=True)
    earn['adj_date'] = np.where(earn['time']<=datetime.time(15,25),earn['date'], earn['date']+datetime.timedelta(1))
    return earn,df2,df
if __name__ =="__main__":
    earnings, other1, all = earnings_history_dt('JSWSTEEL')



