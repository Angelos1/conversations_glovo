import os
import json


def create_directory_structure_json(path):
    result = {
        "name": os.path.basename(path),
        "type": "directory",
        "children": []
    }

    if os.path.isdir(path):
        for item in os.listdir(path):
            item_path = os.path.join(path, item)
            if os.path.isdir(item_path):
                result["children"].append(create_directory_structure_json(item_path))
            else:
                file_type = os.path.splitext(item)[1][1:]
                if file_type in ["json", "parquet"]:
                    result["children"].append({
                        "name": item,
                        "type": os.path.splitext(item)[1][1:]
                    })

    return result


directory_path = 'data_lake'

if os.path.exists(directory_path) and os.path.isdir(directory_path):
    directory_structure_json = create_directory_structure_json(directory_path)

    # Write the JSON to a file
    with open('catalog.json', 'w') as json_file:
        json.dump(directory_structure_json, json_file, indent=4)
    print("JSON structure saved to 'directory_structure.json'")
else:
    print(f"The directory '{directory_path}' does not exist.")
