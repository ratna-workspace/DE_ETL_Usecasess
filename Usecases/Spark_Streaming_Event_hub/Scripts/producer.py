import asyncio
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
import json
import requests

EVENT_HUB_CONNECTION_STR = " " 
EVENT_HUB_NAME =  " "

async def run():
    # Create a producer client to send messages to the event hub.
    # Specify a connection string to your event hubs namespace and
    # the event hub name.
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STR, eventhub_name=EVENT_HUB_NAME
    )
    async with producer:
        # Create a batch.
        while True:
            response=requests.get("https://randomuser.me/api/").json()
            d=response['results'][0]
            event_data_batch = await producer.create_batch()
            # Add events to the batch.
            event_data_batch.add(EventData(json.dumps(d)))
            # Send the batch of events to the event hub.
            await producer.send_batch(event_data_batch)
            time.sleep(5)
            print(d)
asyncio.run(run())