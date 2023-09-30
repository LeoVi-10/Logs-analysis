from confluent_kafka import Producer
import time
from datetime import datetime
import random

# Kafka configuration
kafka_config = {

    'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka broker address

}

def delivery_report(err, msg):

    if err is not None:

        print('Message delivery failed: {}'.format(err))

    else:

        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

 

def main():

    producer = Producer(kafka_config) 
    
    

    while True:
            now = datetime.datetime.now()
            current_time = now.strftime("%b %d %H:%M:%S")
            user_type=random.choice(['su','sshd'])
            session_id= random.randint(10000,99999)
            session_status=random.choice(['opened','failed'])
            user_no = random.randint(1,50)
            uid=random.randint(0,10000)


            log_entry = current_time + " srbarriga " + user_type + "(pam_unix)" + f"[{session_id}]" + ": " + f"session {session_status} for user test{user_no} by (uid={uid}) "
            #print(log_entry)

            producer.produce('testElkCapstone', key=None, value=log_entry, callback=delivery_report)

            producer.poll(0.5)  # Poll for events

            time.sleep(0.001)  # Simulate logs every  seconds

 

    producer.flush()

 

if __name__ == "__main__":

    main()

 