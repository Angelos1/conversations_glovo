import json
import random
from datetime import datetime, timedelta


def generate_random_datetime(start, end):
    return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))


def generate_conversation(customer_id, courier_id, order_id):
    conversation = []
    conversation_started = False
    current_time = generate_random_datetime(
        datetime(2019, 1, 1), datetime(2021, 12, 31)
    )
    stages = ["PICKING_UP", "DELIVERING", "DELIVERED"]
    current_stage = random.choice([0, 1, 2])

    while random.random() < 0.8:  # Generate multiple messages per conversation
        is_customer_message = random.choice([True, False])
        current_time += timedelta(minutes=random.randint(1, 60))

        message = {
            "senderAppType": "Customer iOS" if is_customer_message else "Courier App",
            "customerId": customer_id,
            "fromId": customer_id if is_customer_message else courier_id,
            "toId": courier_id if is_customer_message else customer_id,
            "chatStartedByMessage": not conversation_started,
            "orderId": order_id,
            "orderStage": stages[current_stage],
            "courierId": courier_id,
            "messageSentTime": current_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        conversation.append(message)

        conversation_started = True

        if current_stage < 2:
            if random.random() < 0.4:
                current_stage += 1

    return conversation


def generate_datasets(num_conversations):
    city_codes = ["AGP", "ALC", "LEI", "OVD", "BCN", "BIO", "ODB", "LCG", "YJD", "GRO",
                  "GRX", "IBZ", "XRY", "QGJ", "MJV", "MAD", "MAH", "PMI", "PNA", "REU",
                  "LPA", "SLM", "SDR", "SCQ", "TFS", "TFN", "VLC", "VLL", "VGO", "VIT", "ZAZ"]
    messages_dataset = []
    orders_dataset = []

    for _ in range(num_conversations):
        customer_id = random.randint(10000000, 99999999)
        courier_id = random.randint(15000000, 19999999)
        order_id = random.randint(50000000, 59999999)

        conversation = generate_conversation(customer_id, courier_id, order_id)
        messages_dataset.extend(conversation)
        orders_dataset.append({"orderId": order_id, "cityCode": random.choice(city_codes)})

    return messages_dataset, orders_dataset


# Generate dataset with conversations
data = generate_datasets(600)  # Generate 1000 conversations

# Save dataset to JSON file
with open("datasets/customer_courier_chat_messages.json", "w") as json_file:
    json.dump(data[0], json_file, indent=2)

with open("datasets/orders.json", "w") as json_file:
    json.dump(data[1], json_file, indent=2)
