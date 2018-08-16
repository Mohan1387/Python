# requirements pip install confluent_kafka

from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "127.0.0.1:9092"
KEY = "testkey" # optional we set all the msg will go to the same partition where the key is present
RETRIES = "3" # try to publish msg to topic if failed try 3 times
ACKS = "1" # acknowledges when the msg is received by the leader. -1 for all replications to acknowledge and 0 for no acknowledgement
LINGER_MILLSEC = "1" # publish msg every 10 sec

# The key and value must be binary when passed into Producer().
# We don't need a key and value serializer, because we later
# encode the key and value manually with 'encode(utf-8)'

#set topic where msg needs to be published
TOPIC = "second_topic" 

#setting the configurations
conf = {"bootstrap.servers": BOOTSTRAP_SERVERS, "retries": RETRIES, "acks": ACKS, "linger.ms": LINGER_MILLSEC}

#creating producer object with defined confs
producer = Producer(**conf)

# this method is used in callback prints is msg is published with its details like partition name and offset values
# if not published prints the error
def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}] offset {}'.format(msg.topic(), msg.partition(), msg.offset()))

# creating a dummy value in for loop to publish
for i in range(3):
    producer.produce(topic=TOPIC,value="mymessage_{}".format(i).encode('utf-8'),key=KEY.encode('utf-8'), callback=delivery_report)

# will publish all the messages
producer.flush()

