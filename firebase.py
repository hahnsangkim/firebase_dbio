import pyrebase
import json
from config import config
import pandas as pd

params = config(section='firebase')
fire_config = {
    "apiKey": params['apikey'],
    "authDomain": params['authdomain'],
    "databaseURL": params['databaseurl'],
    "projectId": params['projectid'],
    "storageBucket": params['storagebucket'],
    "messagingSenderId": params['messagingsenderid'],
    "appId": params['appid'],
    "email": params['email'],
    "password": params['password']
}

def read_list_firebase(db, childname):
    results = db.child(childname).get()
    # results = db.child(childname).order_by_key().get()

    # print(results)
    # exit()
    cdic = []
    for item in results.each():
        cdic.append(item.val())
    return cdic

def read_json(ticker):
    with open('table_{}.json'.format(ticker.lower()), 'r') as f:
        json_items = json.load(f)
    return json_items


def get_csv_df(ticker):
    sp = pd.read_csv('table_{}.csv'.format(ticker.lower()), index_col=False)
    sp['ticker'] = ticker.upper()
    cols = sp.columns.tolist()
    cols = [cols[-1]]+cols[:-1]
    sp = sp.reindex(columns=cols)
    return sp


if __name__ == '__main__':

    firebase = pyrebase.initialize_app(fire_config)

    auth = firebase.auth()

    #input('Please enter your email\n')
    #input('Please enter your password\n')
    # #user = auth.create_user_with_email_and_password(email, password)

    email = params['email'] 
    password = params['password'] 
    user = auth.sign_in_with_email_and_password(email, password)
    print(user)
    db = firebase.database()

    ticker = 'MSFT'

    commend = 'read'

    if commend == 'read':
        res = read_list_firebase(db, "stocks")
        print(res)

    # upload a list of jsons -> doesn't work
    # res = db.child("test1").push(stocks_list, user['idToken'])
    # print(res)

    if commend == 'upload':
        res = db.child("stocks").remove()

        # df = get_csv_df('aapl')

        # jdata = df.to_json(orient='records')
        # for i in jdata:
        #     print(jdata)
        #     exit()

        json_items = read_json(ticker)
        jdata = json_items["Time Series (Daily)"]

        # upload one json after another
        for j in jdata:
            newjson = {}
            newjson['ticker'] = ticker.upper()
            newjson['date'] = j
            newjson['open'] = jdata[j].pop('1. open')
            newjson['high'] = jdata[j].pop('2. high')
            newjson['low'] = jdata[j].pop('3. low')
            newjson['close'] = jdata[j].pop('4. close')
            newjson['adj_close'] = jdata[j].pop('5. adjusted close')
            newjson['volume'] = jdata[j].pop('6. volume')
            newjson['dividend'] = jdata[j].pop('7. dividend amount')
            newjson['split'] = jdata[j].pop('8. split coefficient')

            res = db.child("stocks").push(newjson)
            print(res)

    # Not working as expected
    #res = db.child("test1").child(user['idToken']).update({"share_price": 190.00}, user['idToken'])

