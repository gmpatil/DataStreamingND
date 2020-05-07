import asyncio
import json
import confluent_kafka

from confluent_kafka import Consumer

BROKER_URL = "PLAINTEXT://localhost:9092"
TOPIC_NAME = "org.sfopd.cs.events.1"

async def consume(topic_name):
    """Consumes data from the Kafka Topic"""   
    c = Consumer({"bootstrap.servers": BROKER_URL, "group.id": "G-002"})
    #, "auto.offset.reset": "earliest"})

    #c.subscribe([topic_name])
    c.subscribe([topic_name], on_assign=on_assign)    
        
    while True:
        message = c.poll(1.0)
        
        if message is None:
            print("no message received by consumer")
        elif message.error() is not None:
            print(f"error from consumer {message.error()}")
        else:
#             print("message received by consumer")            
            try:
#                 pj = json.loads(message.value())
                print(message.value())
            except KeyError as e:
                print(f"Failed to unpack message {e}")
            except:
                print("Unexpected error:", sys.exc_info()[0])                
                
        await asyncio.sleep(1.0)

"""Callback for when topic assignment takes place"""        
def on_assign(consumer, partitions):

    for partition in partitions:
        partition.offset = confluent_kafka.OFFSET_BEGINNING

    consumer.assign(partitions)

"""Checks for topic and creates the topic if it does not exist"""    
def main():
    try:
        asyncio.run(produce_consume(TOPIC_NAME))
    except KeyboardInterrupt as e:
        print("shutting down")

"""Runs the Producer and Consumer tasks"""
async def produce_consume(topic_name):
    t1 = asyncio.create_task(consume(topic_name))
    await t1

if __name__ == "__main__":
    main()
