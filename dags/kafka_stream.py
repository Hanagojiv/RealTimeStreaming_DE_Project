from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
    
     

default_args = {
    'owner': 'vicky',
    'start_date': datetime(2024, 12, 23, 10, 00)
}
def get_data():
    import json
    import requests
    
    URL = 'https://randomuser.me/api/'
    res = requests.get(URL)
    res = res.json()
    
    res = res['results'][0]
    
    return res


def format_data(res):
    data = {}
    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']
    
    return data
    

def stream_data():
    import json
    from kafka import KafkaProducer

    import time
    import logging
    res = get_data()
    res = format_data(res)
    print(json.dumps(res, indent=3)) #this is pretty print for better human readability. Cosmetic pretty print.
    
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms = 5000)
    # producer.send('user_created', json.dumps(res).encode('utf-8'))
    curr_time = time.time()
    
    while True:
        if time.time() > curr_time + 60: # 1 minute
            break
        try:
            res = get_data()
            res = format_data(res)
            producer.send('user_created', json.dumps(res).encode('utf-8'))
        except Exception as e:
            print(e)
            logging.error(f'An error occured: {e}')
            continue
    return "Stream completed successfully."
# def debug_task():
#     logging.info("This is a test task to verify DAG loading.")

with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )

# stream_data() #checking the output