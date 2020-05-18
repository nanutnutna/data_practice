from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

import pymysql.cursors
import pandas as pd
import requests

def get_data_from_api():
  url = 'https://covid19.th-stat.com/api/open/cases'
  use  = requests.get(url)
  convert_json = use.json()

  # จะได้ข้อมูลที่ต้องการอยู่ใน dataframe แล้ว 
  data = convert_json['Data']
  df = pd.DataFrame(data)

  #เริ่ม data cleansing
  # เริ่มจาก ConfirmDate ลบ เวลาออก ให้เหลือแต่วันที่
  new_date = [date.split()[0] for date in df['ConfirmDate']]
  df_1 = df.drop(columns='ConfirmDate')
  df_1['Date'] = new_date

  #drop coloumn ที่คล้ายกันถึงเพื่อลดขนาดตารางลง
  df_2 = df_1.drop(columns=['Gender','Nation','ProvinceEn'])

  #เติมช่องว่างใน District เป็น -1 แทนสิ่งที่ไม่ทราบ
  new_district = ['-1' if district == '' else district for district in df_2['District']]

  df_3 = df_2.drop(columns='District')
  df_3['District'] = new_district

  new_province = ['-1' if province == 'ไม่พบข้อมูล' else province for province in df_3['Province']]

  df_4 = df_3.drop(columns='Province')
  df_4['Province'] = new_province

  #ต่อมาคือ Age ขอเปลี่ยนค่าที่น้อยกว่า 2 เป็น -1 
  new_age = [-1 if age <= 2 else age for age in df_4['Age']] 

  df_5 = df_4.drop(columns='Age')
  df_5['Age'] = new_age

  #ไม่มีรายละเอียดอะไร drop เลยจ้าาา
  df_6 = df_5.drop(columns='Detail')

  new_nation = ['-1' if nation == 'Unknown' else nation for nation in df_6['NationEn']] 

  df_7 = df_6.drop(columns='NationEn')
  df_7['NationEn'] = new_nation

  # จัดรูปแบบตารางใหม่
  final_df = df_7[['No','Date','Age','GenderEn','NationEn','Province','District','ProvinceId']]
  # save to csv
  final_df.to_csv("/home/airflow/gcs/data/covid_cases.csv",index=False)

# Default Args

default_args = {
    'owner': 'Nattapot',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG

dag = DAG(
    'covid_pipeline',
    default_args=default_args,
    description='Pipeline for ETL covid-19 data',
    schedule_interval=timedelta(days=1),
)

# api call
t1 = PythonOperator(
    task_id= 'api_call',
    python_callable=get_data_from_api,
    dag=dag,
)

# TODO: Dependencies

t1 