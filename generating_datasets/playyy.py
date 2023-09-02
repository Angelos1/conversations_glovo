import json
import csv

# Load the generated JSON dataset
with open("datasets/customer_courier_chat_messages.json", "r") as json_file:
    data = json.load(json_file)

# Define CSV file path
csv_file_path = "datasets/customer_courier_chat_messages.csv"

# Write the JSON data to a CSV file
with open(csv_file_path, "w", newline="") as csv_file:
    csv_writer = csv.DictWriter(csv_file, fieldnames=data[0].keys())
    csv_writer.writeheader()
    csv_writer.writerows(data)

print("CSV file has been created.")
