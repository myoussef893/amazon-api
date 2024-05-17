import urllib.parse
import requests 
import urllib
from datetime import datetime as dt
from datetime import timedelta
import pandas as pd 
import gspread
from gspread_dataframe import set_with_dataframe

client_id = None
client_secret = None
refresh_token = None
orders_endpoint = "https://sellingpartnerapi-na.amazon.com/orders/v0/orders"

token_response= requests.post(
    "https://api.amazon.com/auth/o2/token",
    data={
    'grant_type':'refresh_token',
    'refresh_token': refresh_token,
    'client_id':client_id,
    'client_secret':client_secret,
    },
)

st_date_input = input('Set the Starting Date please: \n')
ed_date_input = input('Set the Ending Date please: \n')
date_format = '%Y-%m-%d'
today = dt.now().date()
starting_date = dt.strptime(st_date_input,date_format).date()
# ending_date = dt.strptime(ed_date_input,date_format).date()



while starting_date != today: 

    access_token = token_response.json()['access_token']
    endpoint ='https://sellingpartnerapi-eu.amazon.com'
    marketplace_id ='ARBP9OOSHTCHU'
    parameters = {
        'MarketplaceIds':'ARBP9OOSHTCHU',
        'CreatedAfter': starting_date-timedelta(days=15)
        
    }

    # Requesting Orders through APIs 
    orders = requests.get(url=endpoint+
                            '/orders/v0/orders?'+
                            urllib.parse.urlencode(parameters),
                            headers={
                                'x-amz-access-token':access_token
                            }).json()['payload']['Orders']


    # # Google Sheet Part:

    sheet_url = 'https://docs.google.com/spreadsheets/d/16Yw7FkQ8qHgILHi24RmFiBQ5OQ338TVhNEAaaMP4oQA/edit#gid=1529271290'
    gc = gspread.service_account('bot_creds.json')
    sp = gc.open_by_url(sheet_url)
    worksheet = sp.worksheet('Order_API')
    strg_wrksheet = sp.worksheet('Storage')
    df = pd.DataFrame(orders)

    df2 = df.loc[:,df.columns.isin(['PurchaseDate',
                                    'AmazonOrderId',
                                    'OrderStatus',
                                    'OrderTotal',
                                    'PaymentMethod',
                                    'ShipmentServiceLevelCategory',
                                    'OrderType','LatestShipDate',
                                    'LatestDeliveryDate',
                                    'LastUpdateDate',
                                    'EasyShipShipmentStatus',
                                    'PaymentMethodDetails',
                                    'NumberOfItemsShipped',
                                    'ShippingAddress'])]

    set_with_dataframe(worksheet,df2,include_column_header=True)
    last_scanned_date = dt.strptime(worksheet.col_values(7)[-1][:10],date_format).date()
    st_date_input = last_scanned_date
    tobe_stored = strg_wrksheet.insert_rows(worksheet)
