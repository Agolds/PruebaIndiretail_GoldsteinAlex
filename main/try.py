import json

with open("../resources/files.json") as file_properties:  #
    data = json.load(file_properties)


for action in data["actions"].items():
    print(type(action))
    print(action)
    print(action[1]["selected_columns"])