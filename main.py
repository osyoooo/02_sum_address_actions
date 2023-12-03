from bs4 import BeautifulSoup
import requests
import pandas as pd
from tqdm import tqdm
import time
from datetime import datetime
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from oauth2client.service_account import ServiceAccountCredentials
import googlemaps
import os
import json
import pytz
import re

# Google APIへのアクセスにはOAuth 2.0という認証プロトコルが使用されており、scope呼ばれる権限の範囲を使ってアクセスを制御
scope = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

#GCPからダウンロードした認証用のjson
GEOCODING_API_URL = 'https://maps.googleapis.com/maps/api/geocode/json'

# 環境変数からAPIキーを読み込む
API_KEY = os.getenv('GOOGLEAPIKEY')

# 環境変数からサービスアカウントキーのJSONコンテンツを取得
credentials_json_str = os.environ['CREDENTIALS_JSON']
credentials_info = json.loads(credentials_json_str)

# 認証情報を生成
credentials = Credentials.from_service_account_info(credentials_info, scopes=scope)

#認証情報を取得
gc = gspread.authorize(credentials)

# スプレッドシートのIDを環境変数から取得
SPREADSHEET_KEY = os.getenv('SPREADSHEET_KEY')

# 指定されたスプレッドシートとシート名からDataFrameを作成する関数
def get_dataframe_from_sheet(spreadsheet, sheet_name):
    worksheet = spreadsheet.worksheet(sheet_name)
    data = worksheet.get_all_values()
    return pd.DataFrame(data[1:], columns=data[0])

# スプレッドシートのIDを指定して開く
spreadsheet = gc.open_by_key(SPREADSHEET_KEY)

# DataFrameに変換
df_url = get_dataframe_from_sheet(spreadsheet, 'suumo_url')

# Kankyo_url 列のみを取り出してリストに変換
kankyo_urls = df_url['Kankyo_url'].tolist()

# 結果を格納するリスト
results = []

for url in tqdm(kankyo_urls):
    # bc_codeをURLから抽出
    bc_code_match = re.search(r'bc=(\d+)', url)
    bc_code = bc_code_match.group(1) if bc_code_match else None

    res = requests.get(url)
    soup = BeautifulSoup(res.text, 'html.parser')

    # scriptタグの中身を確認し、JSONとして解析
    script_tag = soup.find('script', id='js-gmapData')
    if script_tag and script_tag.string:
        gmap_data = json.loads(script_tag.string.strip())

        # centerオブジェクトから緯度と経度を取得
        lat = gmap_data['center']['lat']
        lng = gmap_data['center']['lng']

        # Geocoding APIリクエストパラメータ
        params = {
            'latlng': f'{lat},{lng}',
            'key': API_KEY,
            'language': 'ja'
        }

        # Geocoding APIリクエストを送信
        response = requests.get(GEOCODING_API_URL, params=params)
        data = response.json()

        # 郵便番号と住所コンポーネントを取得
        address_parts = {
            'postal_code': '',
            'administrative_area_level_1': '',
            'locality': '',
            'sublocality_level_2': '',
            'sublocality_level_3': '',
            'sublocality_level_4': '',
            'sublocality_level_5': ''  # 追加
        }

        for component in data['results'][0]['address_components']:
            for address_type in address_parts.keys():
                if address_type in component['types']:
                    address_parts[address_type] = component['long_name']

        # 郵便番号を取得
        postcode = address_parts['postal_code']

        # 日本の住所形式に合わせてメインアドレスを組み立てる
        main_address = ''.join([
            address_parts['administrative_area_level_1'],
            address_parts['locality'],
            address_parts['sublocality_level_2'],
            address_parts['sublocality_level_3'],
            address_parts['sublocality_level_4'],
            address_parts['sublocality_level_5']  # 追加
        ])

        # 全住所を取得
        all_address = data['results'][0]['formatted_address'] if data['results'] else None

        # place_idを取得
        place_id = data['results'][0]['place_id'] if data['results'] else None

        time.sleep(1)

        # 結果をリストに追加
        results.append({
            "Bc_code": bc_code,
            "Postcode": postcode,
            "Main_address": main_address,
            "All_address": all_address,  # 追加
            "Place ID": place_id,
            "Lat": lat,
            "Lng": lng,
            "URL": url
        })

# データフレームに変換
df = pd.DataFrame(results)

# スプレッドシートのaddress用のsheetを指定して開く
worksheet = spreadsheet.worksheet('suumo_address_db')

# ワークシートの内容をクリア
worksheet.clear()

set_with_dataframe(worksheet, df)

###### EOF
