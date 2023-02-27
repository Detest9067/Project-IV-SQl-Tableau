import json
import requests
import os

if not os.path.exists("images"):
    os.makedirs("images")

with open("images/game_of_thrones_characters.json", "r") as f:
    character_data = json.load(f)

for character in character_data:
    name = f"{character['firstName']} {character['lastName']}"
    image_url = character['imageUrl']

    image_data = requests.get(image_url).content

    file_name = f"{name}.jpg"
    file_path = os.path.join("images", file_name)
    with open(file_path, "wb") as f:
        f.write(image_data)

    print(f"Saved {file_path}")

print("All images have been downloaded!")

