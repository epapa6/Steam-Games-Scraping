import json

import requests


def fetch_steam_games():
    url = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"
    response = requests.get(url)

    if response.status_code == 200:
        games = response.json().get('applist', {}).get('apps', [])
        return games
    else:
        print(f"Errore nella richiesta: {response.status_code}")
        return []


def save_games_to_json(games, filename):
    with open(filename, 'w') as file:
        json.dump(games, file, indent=4)


def main():
    games = fetch_steam_games()
    # Ordina i giochi per appid in ordine crescente
    sorted_games = sorted(games, key=lambda x: x['appid'])
    save_games_to_json(sorted_games, 'games/steam_games.json')


if __name__ == "__main__":
    main()
