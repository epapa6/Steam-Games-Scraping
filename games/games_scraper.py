import csv
import json
import os
import re
import time

import requests
from tqdm import tqdm


def clean_price(price):
    if price:
        price = price.replace(',', '.')
        price = price.replace('-', '0')
        return price
    return None


def clean_text(text):
    if text:
        text = text.replace('\n\r', ' ')
        text = text.replace('\r\n', ' ')
        text = text.replace('\r \n', ' ')
        text = text.replace('\r', ' ')
        text = text.replace('\n', ' ')
        text = text.replace('\t', ' ')
        text = text.replace('&quot;', "'")
        text = re.sub(r'(https|http)?:\/\/(\w|\.|\/|\?|\=|\&|\%)*\b', '', text, flags=re.MULTILINE)
        text = re.sub('<[^<]+?>', ' ', text)
        text = re.sub(' +', ' ', text)
        text = text.lstrip(' ')
    return text


def get_game_request(game_id, max_retries=10):
    retry_count = 1

    while retry_count <= max_retries:
        try:
            response = requests.get(f'https://store.steampowered.com/api/appdetails',
                                    params={'appids': game_id, 'cc': 'it', 'l': 'en'})

            time.sleep(1.1)

            # Request OK
            if response.status_code == 200:

                try:
                    data = response.json()

                    # Success request
                    if str(game_id) in data and data[str(game_id)]['success']:
                        game_info = data[str(game_id)]['data']
                        return game_info

                    # Failed request
                    else:
                        with open('./no_success_game_requests.txt', 'a') as file:
                            file.write(f'{game_id}\n')
                        return None

                    # Error in decoding game data
                except json.decoder.JSONDecodeError:
                    print(f'\n\033[91mSteam: Failed to decode {game_id}\033[0m')
                    with open('./bad_game_requests.txt', 'a') as file:
                        file.write(f'{game_id}\n')
                    return None

            # Other status
            else:
                print(f'\n\033[91mSteam: Unknown request for {game_id} - Status Code: {response.status_code}\033[0m')
                retry_count += 1
                time.sleep(60)

        # Connection lost
        except requests.ConnectionError:
            print(f'\n\033[91mSteam: Connection Error\033[0m')
            time.sleep(60)

        # Empty request
        except requests.exceptions.RequestException:
            print(f'\n\033[91mSteam: No infos for {game_id}: {retry_count}\033[0m')
            retry_count += 3
            time.sleep(60)

    # It was not possible to get infos of the game        
    with open('./bad_game_requests.txt', 'a') as file:
        file.write(f'{game_id}\n')
    return None


def get_steamspy_tags(game_id, max_retries=10):
    retry_count = 1

    while retry_count <= max_retries:
        try:
            response = requests.get(f'https://steamspy.com/api.php',
                                    params={'request': 'appdetails', 'appid': game_id})

            if response.status_code == 200:
                try:
                    data = response.json()
                    if 'tags' in data:
                        return data['tags']
                    else:
                        return None
                except json.decoder.JSONDecodeError:
                    print(f'\n\033[91mSteamSpy: Failed to decode data for {game_id}\033[0m')
                    return {}
            else:
                print(f'\n\033[91mSteamSpy: Failed request for {game_id} - Status Code: {response.status_code}\033[0m')
                retry_count += 1
                time.sleep(60)

        except requests.ConnectionError:
            print(f'\n\033[91mSteamSpy: Connection Error\033[0m')
            time.sleep(60)

        except requests.exceptions.RequestException:
            print(f'\n\033[91mSteamSpy: No infos for {game_id}: {retry_count}\033[0m')
            retry_count += 3
            time.sleep(60)

    return None


def get_game_details(game_request):
    if game_request:

        # Get only games
        if game_request.get('type', '') == 'game':  # or game_info.get('type', '') == 'dlc'

            # Id
            game_id = game_request.get('steam_appid', None)

            # Name
            game_name = game_request.get('name', None)

            # Price
            price_info = game_request.get('price_overview', {})
            is_free = game_request.get('is_free', False)
            game_price = '0.00€' if is_free else clean_price(price_info.get('final_formatted', '0.00€'))

            # Developers
            game_developers = []
            if 'developers' in game_request:
                for developer in game_request['developers']:
                    game_developers.append(developer.strip())

            # Publishers
            game_publishers = []
            if 'publishers' in game_request:
                for publisher in game_request['publishers']:
                    game_publishers.append(publisher.strip())

            # Long Description
            game_long_description = clean_text(game_request.get('detailed_description', None))

            # Short Description
            game_short_description = clean_text(game_request.get('short_description', None))

            # Header Image
            game_header_image = game_request.get('header_image', None)

            # Recommendations
            game_recommendations = game_request.get('recommendations', {}).get('total', 0)

            # Categories
            game_categories = []
            if 'categories' in game_request:
                for category in game_request['categories']:
                    game_categories.append(clean_text(category['description']))

            # Genres
            game_genres = []
            if 'genres' in game_request:
                for genre in game_request['genres']:
                    game_genres.append(clean_text(genre['description']))

            steamspy_tags = get_steamspy_tags(game_id)

            game_tags = []
            if steamspy_tags:
                game_tags = [tag for tag in steamspy_tags.keys()]

            return {
                'id': game_id,
                'name': game_name,
                'price': game_price,
                'developer': game_developers,
                'publisher': game_publishers,
                'long_description': game_long_description,
                'short_description': game_short_description,
                'header_image': game_header_image,
                'recommendations': game_recommendations,
                'categories': game_categories,
                'genres': game_genres,
                'tags': game_tags
            }

        else:
            with open('./not_a_game.txt', 'a') as file:
                file.write(f'{game_request["steam_appid"]}\n')
            return None

    else:
        return None


def load_json_file(json_file):
    with open(json_file, 'r', encoding='utf-8-sig') as file:
        games = json.load(file)
    return games


def load_txt_file(txt_file):
    processed_ids = set()
    with open(txt_file, 'r', encoding='utf-8-sig') as file:
        for row in file:
            processed_id = int(row.strip())
            processed_ids.add(processed_id)

    return processed_ids


def save_to_csv(game, filename):
    with open(filename, 'a', newline='', encoding='utf-8-sig') as file:
        writer = csv.DictWriter(file, fieldnames=game[0].keys())
        if file.tell() == 0:
            writer.writeheader()

        for row in game:
            formatted_row = {key: ', '.join(map(str, value)) if isinstance(value, list) else value for key, value in
                             row.items()}
            writer.writerow(formatted_row)


def setup():
    if os.path.exists('./games_processed_id.txt'):
        processed_ids = load_txt_file('./games_processed_id.txt')
    else:
        processed_ids = set()

    if os.path.exists('./not_a_game.txt'):
        not_a_game_ids = load_txt_file('./not_a_game.txt')
    else:
        not_a_game_ids = set()

    if os.path.exists('./no_success_game_requests.txt'):
        no_success_ids = load_txt_file('no_success_game_requests.txt')
    else:
        no_success_ids = set()

    if os.path.exists('./bad_game_requests.txt'):
        os.remove('./bad_game_requests.txt')

    games = load_json_file('steam_games.json')

    games = [game for game in games
             if game['appid'] not in not_a_game_ids and
             game['appid'] not in no_success_ids and
             game['appid'] not in processed_ids]

    return games, processed_ids


def main():
    games, processed_ids = setup()

    for game in tqdm(games, desc='Processing Games'):
        game_id = game.get('appid')
        if game_id not in processed_ids:
            game_request = get_game_request(game_id)
            if game_request:
                if game_request['steam_appid'] not in processed_ids:
                    game_details = get_game_details(game_request)
                    if game_details:
                        save_to_csv([game_details], '../steam_games.csv')
                        print(f"\n\033[92mAdded game {game_details['id']} - {game_details['name']}\033[0m")
                        with open('./games_processed_id.txt', 'a') as file:
                            file.write(f'{game_id}\n')
                        processed_ids.add(game_details['id'])


if __name__ == '__main__':
    main()
