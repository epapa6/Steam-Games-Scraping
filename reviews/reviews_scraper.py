import requests
import json
import csv
import os
import time
from tqdm import tqdm
import concurrent.futures


def get_review_request(game_id, max_retries=10):
    retry_count = 0
    all_reviews = []
    cursor = '*'

    with tqdm(desc=f'Reviews for {game_id}', unit=' reviews') as progress_bar:
        while retry_count < max_retries:
            try:
                response = requests.get(f'https://store.steampowered.com/appreviews/{game_id}',
                                        params={'json': 1, 'filter': 'recent', 'language': 'english',
                                                'num_per_page': 100, 'cursor': cursor})
                # time.sleep(1.5)

                # Request OK
                if response.status_code == 200:

                    try:
                        data = response.json()

                    # Error in decoding review data
                    except json.decoder.JSONDecodeError:
                        print(f'\n\033[91mFailed to decode reviews for {game_id}\033[0m')
                        with open('bad_reviews_requests.txt', 'a') as file:
                            file.write(f'{game_id}\n')
                        return None

                    # Success request
                    if 'reviews' in data and data['success']:
                        reviews = data['reviews']
                        all_reviews.extend(reviews)

                        # Check if there are more pages
                        if 'cursor' in data and data['cursor'] != cursor:
                            cursor = data['cursor']
                            progress_bar.update(len(reviews))

                        else:
                            time.sleep(1.5)
                            return all_reviews

                    # Failed request
                    else:
                        with open('no_success_reviews_requests.txt', 'a') as file:
                            file.write(f'{game_id}\n')
                        return None

                # Too Many Requests   
                elif response.status_code == 429:
                    print(f'\n\033[91mToo many requests for reviews of {game_id}: {retry_count}\033[0m')
                    retry_count += 1
                    time.sleep(60)

                # Other status
                else:
                    print(
                        f'\n\033[91mUnknown request for reviews of {game_id} - Status Code: {response.status_code}\033[0m')
                    retry_count += 1
                    time.sleep(60)

            # Connection lost
            except requests.ConnectionError:
                print(f'\n\033[91mConnection Error\033[0m')
                time.sleep(60)

            # Empty request
            except requests.exceptions.RequestException:
                print(f'\n\033[91mNo reviews for {game_id}: {retry_count}\033[0m')
                retry_count += 3
                time.sleep(60)

        # It was not possible to get reviews of the game        
        with open('bad_reviews_requests.txt', 'a') as file:
            file.write(f'{game_id}\n')
        time.sleep(1.5)
        return None


def get_review_details(review_request, game_id):
    if review_request:
        reviews = []

        for review_request in review_request:
            # Id
            review_id = review_request.get('recommendationid', None)

            # User_id
            review_user_id = review_request.get('author', {}).get('steamid', None)

            # Voted up
            review_voted_up = review_request.get('voted_up', False)

            if review_voted_up:
                reviews.append({
                    'recommendation_id': review_id,
                    'app_id': game_id,
                    'steam_id': review_user_id,
                })

        return reviews

    else:
        return None


def load_csv_file(csv_file):
    games_id = []
    required_fields = ['app_id', 'name', 'developer', 'publisher', 'long_description', 
                       'short_description', 'header_image', 'recommendations', 'categories', 
                       'genres', 'tags']
    
    with open(csv_file, 'r', encoding='utf-8-sig') as file:
        reader = csv.DictReader(file)
        for row in reader:
            if all(row.get(field) for field in required_fields):
                game_id = row.get('app_id')
                if game_id:
                    games_id.append(game_id)
    
    return games_id


def load_txt_file(txt_file):
    processed_ids = set()
    with open(txt_file, 'r', encoding='utf-8-sig') as file:
        for row in file:
            processed_id = row.strip()
            processed_ids.add(processed_id)

    return processed_ids


def save_to_csv(reviews, filename):
    with open(filename, 'a', newline='', encoding='utf-8-sig') as file:
        writer = csv.DictWriter(file, fieldnames=reviews[0].keys())
        if file.tell() == 0:
            writer.writeheader()

        for review in reviews:
            writer.writerow({
                'recommendation_id': review['recommendation_id'],
                'app_id': review['app_id'],
                'steam_id': review['steam_id']
            })


def process_game_reviews(game_id, processed_ids, games_id):
    if not game_id in processed_ids:
        reviews_request = get_review_request(game_id)
        if reviews_request:
            reviews_details = get_review_details(reviews_request, game_id)
            if reviews_details:
                save_to_csv(reviews_details, '../steam_reviews.csv')
                print(
                    f"\n\033[92mAdded {len(reviews_details)} reviews for {game_id}\t{len(processed_ids)}/{len(games_id)}\033[0m")

            with open('reviews_processed_id.txt', 'a') as file:
                file.write(f'{game_id}\n')
            processed_ids.add(game_id)


def setup():
    if os.path.exists('./no_success_reviews_requests.txt'):
        no_success_ids = load_txt_file('no_success_reviews_requests.txt')
    else:
        no_success_ids = set()

    if os.path.exists('./reviews_processed_id.txt'):
        processed_ids = load_txt_file('./reviews_processed_id.txt')
    else:
        processed_ids = set()

    games = load_csv_file('../steam_games.csv')

    games = [game_id for game_id in games
             if game_id not in no_success_ids and
             game_id not in processed_ids]

    if os.path.exists('bad_reviews_requests.txt'):
        os.remove('bad_reviews_requests.txt')

    return games, processed_ids


def main():
    
    games, processed_ids = setup()

    max_parallel_executions = 10

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_parallel_executions) as executor:
        futures = [executor.submit(process_game_reviews, game, processed_ids, games) for game in games]
        concurrent.futures.wait(futures)


if __name__ == '__main__':
    main()
