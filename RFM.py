#Customer Segmentation with RFM

# master_id: Unique customer id
# order_channel:Which platform used for shopping (Android, ios, Desktop, Mobile)
# last_order_channel: Platform used for the last shopping activity
# first_order_date:  First order date of the customer
# last_order_date:Last order date of the customer
# last_order_date_online:Last online order date of the customer
# last_order_date_offline: Last offline order date of the customer
# order_num_total_ever_online:Number of unique online orders customer have made
# order_num_total_ever_offline:Number of unique offline orders customer have made
# customer_value_total_ever_offline: Number of unique offline orders customer have made
# customer_value_total_ever_online:Amount of money paid by customer for online orders
# interested_in_categories_12 : Categories in which customer has shopped from for the last 12 months

#Understanding and Preparation of Data

import datetime as dt
import pandas as pd
import seaborn as sns
pd.set_option('display.max_columns',None)
pd.set_option('display.float_format', lambda x : '%.3f' %x )

#STEP1
df_ = pd.read_csv("C:/Users/caspe/Desktop/VBO/week4/flo_data_20k.csv")
flo_data=df_.copy()

flo_data.head(10)
flo_data.columns
flo_data.shape
flo_data.describe().T
flo_data.isnull().sum()
flo_data.info()

flo_data["order_num_total"] = flo_data["order_num_total_ever_online"] + flo_data["order_num_total_ever_offline"]
flo_data["customer_value_total"] = flo_data["customer_value_total_ever_offline"] + flo_data["customer_value_total_ever_online"]

flo_data.dtypes

flo_data["last_order_date"]=pd.to_datetime(flo_data["last_order_date"])
flo_data["first_order_date"]=pd.to_datetime(flo_data["first_order_date"])
flo_data["last_order_date_online"]=pd.to_datetime(flo_data["last_order_date_online"])
flo_data["last_order_date_offline"]=pd.to_datetime(flo_data["last_order_date_offline"])
flo_data.dtypes
flo_data.info()


flo_data.groupby("order_channel").agg({"master_id":"count",
                                 "order_num_total":"sum",
                                 "customer_value_total":"sum"})


flo_data.groupby("master_id").agg({"customer_value_total":"sum"}).sort_values("customer_value_total",ascending=False).head(10)


flo_data.groupby("master_id").agg({"order_num_total":"sum"}).sort_values("order_num_total",ascending=False).head(10)



def data_prep(dataframe):
    dataframe["order_num_total"] = dataframe["order_num_total_ever_online"] + dataframe["order_num_total_ever_offline"]
    dataframe["customer_value_total"] = dataframe["customer_value_total_ever_offline"] + dataframe["customer_value_total_ever_online"]
    date_columns = dataframe.columns[dataframe.columns.str.contains("date")]
    dataframe[date_columns] = dataframe[date_columns].apply(pd.to_datetime)
    return flo_data




flo_data["last_order_date"].max()
today_date = dt.datetime(2021,6,1)
type(today_date)

rfm = pd.DataFrame()
rfm["customer_id"] = flo_data["master_id"]
rfm["recency"] = (today_date  - flo_data["last_order_date"]).astype('timedelta64[D]')
rfm["frequency"] = flo_data["order_num_total"]
rfm["monetary"] = flo_data["customer_value_total"]

rfm.head()



rfm["recency_scores"]=pd.qcut(rfm['recency'],5,labels=[5,4,3,2,1])
rfm["monetary_scores"]=pd.qcut(rfm['monetary'],5,labels=[1,2,3,4,5])
rfm["frequency_scores"]=pd.qcut(rfm['frequency'].rank(method="first"),5,labels=[1,2,3,4,5])
rfm["RF SCORE"] = (rfm['recency_scores'].astype(str)+
                  rfm['frequency_scores'].astype(str))
rfm.head()

seg_map = {
    r'[1-2][1-2]' : 'hibernating',
    r'[1-2][3-4]': 'at_Risk',
    r'[1-2]5': 'cant_loose',
    r'3[1-2]' : 'about_to_sleep',
    r'33': 'need_attention',
    r'[3-4][4-5]' : 'loyal_customers',
    r'41' : 'promising',
    r'51' : 'new_customers',
    r'[4-5][2-3]' :' potential_loyalists',
    r'5[4-5]' : 'champions'

}
rfm['segment']= rfm['RF SCORE'].replace(seg_map,regex=True)



rfm[["segment", "recency", "frequency", "monetary"]].groupby("segment").agg(["mean", "count"])



new_brand_=rfm.loc[(rfm['segment']=='champions') | (rfm['segment']=='loyal_customers'),'customer_id']
flo_data.loc[flo_data['interested_in_categories_12'].str.contains('KADIN'),'master_id']
flo_data.loc[flo_data['master_id'].isin(new_brand_.values)]


target_segments_customer_ids = rfm[rfm["segment"].isin(["cant_loose","hibernating","new_customers"])]["customer_id"]
cust_ids =flo_data[(flo_data["master_id"].isin(target_segments_customer_ids)) & ((flo_data["interested_in_categories_12"].str.contains("ERKEK"))|(flo_data["interested_in_categories_12"].str.contains("COCUK")))]["master_id"]
cust_ids.to_csv("indirim_hedef_müşteri_ids.csv", index=False)
