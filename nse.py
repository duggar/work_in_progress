import pandas as pd
import datetime, numpy as np
import  requests
from requests.exceptions import ReadTimeout
import six
from zipfile import ZipFile
from dateutil.parser import *
import time,io

def unzip_str(zipped_str, file_name = None):
    if isinstance(zipped_str, six.binary_type):
        fp = six.BytesIO(zipped_str)
    else:
        fp = six.BytesIO(six.b(zipped_str))

    zf = ZipFile(file=fp)
    if not file_name:
        file_name = zf.namelist()[0]
    return zf.read(file_name).decode('utf-8')

def strip_str(x):
    if isinstance(x, str):
        x = x.strip()
    return x

class nsedata:
    def __init__(self):
        self.headers = {
                "Accept":"*/*",
                "Accept-Encoding":"gzip, deflate, br",
                "Accept-Language":"en-US,en;q=0.5",
                "Connection":"keep-alive",
                "Host":"www1.nseindia.com",
                "Upgrade-Insecure-Requests": "1"}
        
    def clean_split_fnobhav(self,df):
        df = df.rename( columns={'INSTRUMENT': 'instrument', 'SYMBOL': 'symbol', 'EXPIRY_DT': 'expiry',
                                     'STRIKE_PR': 'strike', 'OPTION_TYP': 'opt_type', 'OPEN': 'open',
                                     'HIGH': 'high', 'LOW': 'low', 'CLOSE': 'close', 'SETTLE_PR': 'settle_price',
                                     'CONTRACTS': 'volume', 'VAL_INLAKH': 'traded_value', 'OPEN_INT': 'oi',
                                     'CHG_IN_OI': 'oi_change', 'TIMESTAMP': 'date'})
        df['date'] = df['date'].apply(parse).dt.date
        df['expiry']=df['expiry'].apply(parse).dt.date
        fut = df[df.strike==0]
        opt = df[df.strike>0]
        fut = fut[['date','instrument', 'symbol', 'expiry', 'open', 'high', 'low', 'close', 'settle_price', 'volume', 'traded_value','oi', 'oi_change']]
        opt = opt[['date','instrument', 'symbol', 'expiry', 'strike', 'opt_type', 'open', 'high', 'low', 'close', 'settle_price', 'volume', 'traded_value','oi', 'oi_change']]
        fut = fut[~(fut.oi==0) | ~(fut.volume==0)]
        opt = opt[~(opt.oi==0) | ~(opt.volume==0)]
        return fut, opt
    def get_price_list_deriv(self,dt):

        MMM = dt.strftime("%b").upper()
        yyyy = dt.strftime("%Y")
        valid_instrument = ['FUTIDX', 'FUTSTK', 'OPTIDX', 'OPTSTK']

        if dt<=datetime.date(2020,12,31):
            url = "https://www1.nseindia.com/content/historical/DERIVATIVES/{}/{}/fo{}bhav.csv.zip".format(yyyy, MMM, dt.strftime("%d%b%Y").upper())
            res = requests.get(url, headers=self.headers)
        else:
            try:
                url = "https://archives.nseindia.com/content/historical/DERIVATIVES/{}/{}/fo{}bhav.csv.zip".format(yyyy, MMM, dt.strftime("%d%b%Y").upper())
                res = requests.get(url,timeout=6)
            except requests.exceptions.Timeout as err: 
                print(err)
                pass
                return pd.DataFrame(), pd.DataFrame()
        if res.status_code==200:
            zf = unzip_str(res.content)
            df = pd.read_csv(six.StringIO(zf))
            if 'Unnamed: 15' in df.columns:
                del df['Unnamed: 15']
            df = df[df.INSTRUMENT.isin(valid_instrument)]
            fut, opt = self.clean_split_fnobhav(df)
            return fut, opt
        else:
            return pd.DataFrame(), pd.DataFrame()
        return df
    def get_participant_fo_data(self,dt):
        dat = dt.strftime( "%d%m%Y" )
        url = 'https://www1.nseindia.com/content/nsccl/fao_participant_{}_{}.csv'
        fao = ['oi','vol']
        part_df = []
        for val in fao:
            requrl = url.format(val,dat)
            req = requests.get(requrl, headers=self.headers)
            a = req.text.splitlines()
            if req.ok:
                a = a[-6:]
                df = pd.DataFrame([sub.split(",") for sub in a])
                df.iloc[0] = df.iloc[0].apply(lambda x: x.strip())
                df.iloc[0] = df.iloc[0].apply(lambda x: x.replace('"',''))
                df.iloc[0] = df.iloc[0].apply(lambda x: x.replace('"',''))
                df.iloc[0]= df.iloc[0].apply(lambda x: x.replace("\t",""))
                df.columns = df.iloc[0]
                df = df.drop(df.index[0])
                if 'CLIENT_TYPE' in df.columns:
                    df = df.rename(columns = {'CLIENT_TYPE': 'Client Type'})
                df = df.replace('NA',0)     
                df = df.rename(columns = {'Client Type': 'client_type', 'Future Index Long': 'fut_idx_long', 'Future Index Short': 'fut_idx_short','Future Stock Long': 'fut_stk_long',
                                          'Future Stock Short': 'fut_stk_short', 'Option Index Call Long': 'opt_idx_call_long','Option Index Put Long':'opt_idx_put_long',
                                          'Option Index Call Short' : 'opt_idx_call_short','Option Index Put Short': 'opt_idx_put_short',
                                          'Option Stock Call Long': 'opt_stk_call_long', 'Option Stock Put Long': 'opt_stk_put_long',
                                          'Option Stock Call Short' : 'opt_stk_call_short', 'Option Stock Put Short': 'opt_stk_put_short',
                                          'Total Long Contracts': 'total_long','Total Short Contracts': 'total_short',"Future Stock Short":'fut_stk_short',"Total Long Contracts":'total_long'})
                cnames=['client_type', 'fut_idx_long', 'fut_idx_short', 'fut_stk_long','fut_stk_short', 'opt_idx_call_long', 'opt_idx_put_long','opt_idx_put_short', 'opt_idx_call_short', 'opt_stk_call_long','opt_stk_call_short', 'opt_stk_put_long', 'opt_stk_put_short','total_long', 'total_short']
                try:
                    df = df[cnames]
                except KeyError:
                    cnames.append("useless")
                    df.columns = cnames
                    cnames=cnames[:-1]
                    df = df[cnames]
                df['date'] = [dt]*len(df)
                df['metric'] = [val]* len(df)
                df = df.set_index('date', drop = True)
                for col in df.columns:
                    df[col] = df[col].apply(strip_str)
                part_df.append(df)
            else:
                print('No particiapnt data for date {} and for param {}'.format(dt,val))
        if len(part_df) == 2:
            part = pd.concat(part_df)
        elif len(part_df) == 0:
            part = []
        elif len(part_df) == 1:
            part = part_df[0]
        if len(part):
            part = part[['client_type', 'fut_idx_long', 'fut_idx_short', 'fut_stk_long','fut_stk_short', 'opt_idx_call_long', 'opt_idx_put_long','opt_idx_put_short', 'opt_idx_call_short', 'opt_stk_call_long','opt_stk_call_short', 'opt_stk_put_long', 'opt_stk_put_short','total_long', 'total_short', 'metric']]    
            part['client_type'] =part['client_type'].apply(lambda x:x.lower())
        else:
            part = pd.DataFrame()
        return part
    
    
    def get_fullcash_list(self, dt):
        dat = dt.strftime( "%d%m%Y" )
        url  = 'https://archives.nseindia.com/products/content/sec_bhavdata_full_{}.csv'.format(dat)
        df = pd.DataFrame()
        try:
            res=  requests.get(url,timeout=6)
        except ReadTimeout:
            print("No EQT data for date : {}".format(dt))
            return pd.DataFrame()
            pass
        if res.ok:
            content = res.content
            df = pd.read_csv(io.StringIO(content.decode('utf-8')))
            if len(df) > 0:
                col_params ={'SYMBOL':'symbol','SERIES':'series','DATE1':'date','PREV_CLOSE':'prev','OPEN_PRICE':'open','HIGH_PRICE':'high',
                    'LOW_PRICE':'low','LAST_PRICE':'last','CLOSE_PRICE':'close','AVG_PRICE':'wap','TTL_TRD_QNTY':'volume','TURNOVER_LACS':'turnover',
                    'NO_OF_TRADES':'nb_trades','DELIV_QTY':'delivery_qty','DELIV_PER':'delivery_pc'}
                df.columns = [x.strip() for x in list(df.columns)]
                df  =df.rename(columns=col_params)
                stringcols = ['symbol','series','date']
                for c in stringcols:
                    df[c] = [x.strip() for x in list(df[c])]
                df.date = pd.to_datetime(df.date, format = "%d-%b-%Y").dt.date
                numcols = set(df.columns)-set(stringcols)
                for x in numcols:
                    df[x] = pd.to_numeric(df[x],errors='coerce')
                    df.loc[:,x] = df.loc[:,x].fillna(0)
                df['delivery_val'] = np.round(df['delivery_qty'] * df['wap'],2)
                df.set_index('date', inplace=True)
                df.reset_index(inplace=True)
        else:
            print("No EQT data for date : {}".format(dt))
        return df
    
    def get_index_eod(self,dt):
        dat = dt.strftime( "%d%m%Y" )
        index_url = 'https://www1.nseindia.com/content/indices/ind_close_all_{}.csv'.format(dat)
        
        res = requests.get(index_url, headers=self.headers)
        if res.status_code == 200:
            df = pd.read_csv(io.StringIO(res.content.decode('utf-8')),delimiter=',')
        else:
            return pd.DataFrame()
        if len(df) == 0:
            print("No data for ",dt," index consituent")
        else:
            cols = ['index_name','date','open','high','low','close','change','pc_change','volume','turnover','pe','pb','divyield']
            df.columns = cols
            df['symbol'] = df['index_name'].apply(lambda x:x.upper().replace(' ',''))
            df.replace('-',0,inplace=True)
            df.fillna(0,inplace=True)
            df['date']=dt
        return df
    
    
# def get_index_weight(self,dt):
#     dat = dt.strftime( "%d%m%Y" )
#     index_url = 'https://www1.nseindia.com/content/indices/ind_close_all_{}.csv'.format(dat)
    
#     res = requests.get(index_url, headers=self.headers)
#     if res.status_code == 200:
#         df = pd.read_csv(io.StringIO(res.content.decode('utf-8')),delimiter=',')
#     else:
#         return pd.DataFrame()
#     if len(df) == 0:
#         print("No data for ",dt," index consituent")
#     else:
#         cols = ['index_name','date','open','high','low','close','change','pc_change','volume','turnover','pe','pb','divyield']
#         df.columns = cols
#         df['symbol'] = df['index_name'].apply(lambda x:x.upper().replace(' ',''))
#         df.replace('-',0,inplace=True)
#         df.fillna(0,inplace=True)
#         df['date']=dt
#     return df
  

# # if __name__=="__main__":
# #     data = nsedata()
# #     d1,df2 = data.get_price_list_deriv(datetime.date(2021,1,26))
    
    

    
# import fnmatch

# #read data from the csv file
# with ZipFile("Data_files/zipped.zip") as zipped_files:
    
#     #get list of files in zip
#     file_list = zipped_files.namelist()
    
#     #use fnmatch.filter to get the csv file
#     csv_file = fnmatch.filter(file_list, "*.csv")
    
#     #get the csv data
#     data = zipped_files.open(*csv_file)

# #read into dataframe
# df = pd.read_csv(data)

# df.head()

# import zipfile
# from StringIO import StringIO

# zipdata = StringIO()
# zipdata.write(get_zip_data())
# myzipfile = zipfile.ZipFile(zipdata)
# url = "https://www1.nseindia.com/content/indices/indices_dataJun2021.zip"
# res = requests.get(url)
# zf = zipfile.ZipFile(BytesIO(res.content))

# from tqdm import tqdm
# def clean_weight_df(df):
#     colnames = ['symbol','name','industry','close','market_cap','weight']
#     if len(df):
#         df_res = pd.DataFrame()
#         for sdf in df:
#             _sdf = pd.DataFrame()
#             cols=[]
#             mol = sdf.iloc[0,0]
#             if isinstance(mol,str):
#                 if mol.lower() == 'symbol':
#                     sdf = sdf.drop(sdf.index[0])
#             elif mol != mol:
#                 sdf = sdf.drop(sdf.index[0])
#             sdf.dropna(how='all', inplace=True, axis=1)
#             sdf.columns =colnames
#             for c in ['close','market_cap','weight']:
#                 sdf[c] = pd.to_numeric(sdf[c],errors='coerce')
#             for c in sdf.columns:
#                 p = sdf[c].dropna().reset_index()
#                 cols.append(p)
#             _sdf = pd.concat(cols,axis=1)
#             _sdf = _sdf[colnames]
#             _sdf = _sdf.dropna()
#             df_res = df_res.append(_sdf)
#     return df_res

# cons = pd.DataFrame()
# for f in zf.namelist():
#     df = read_pdf(zf.open(f), pages='all',stream=True,multiple_tables=True)
#     print(f)
#     ind = f.split("_")[:-1]
#     ind = "".join(ind)
#     ind = ind.upper().replace("%","")
#     print(ind)
#     df = clean_weight_df(df)
#     df['index_name'] = ind
#     print("Len of df {}".format(len(df)))
#     cons = cons.append(df)
    