import requests
import time
from database import db
from sqlalchemy import text
from datetime import datetime

API_RANK_URL = "https://gwent-rankings.gog.com/ranked_2_0/seasons/"
SEASON_ID = None

FACTIONS = [
    'Nilfgaard',
    'Scoiatael',
    'Syndicate',
    'Monster',
    'NorthernKingdom',
    'Skellige'
]

factions_dict = {
    'Nilfgaard': 'nilfgaard_wr',
    'Scoiatael': 'scoiatael_wr',
    'Syndicate': 'syndicate_wr',
    'Monster': 'monsters_wr',
    'NorthernKingdom': 'northern_realms_wr',
    'Skellige': 'skellige_wr'
}

def init_factions():
    return {f: {'wins_count': 0, 'games_count': 0} for f in FACTIONS}

from sqlalchemy import text

def get_all_gwent_ids():
    result = db.execute(text("SELECT gwent_id FROM players"))
    return [row[0] for row in result.fetchall()]

def get_current_season_id():
    result = db.execute(text("SELECT value FROM properties WHERE name='season_id' ORDER BY id DESC LIMIT 1"))
    row = result.fetchone()
    return row[0] if row else None

def get_ranking_info(user_id):
    if user_id is not None:
        response = requests.get(f'{API_RANK_URL}{SEASON_ID}/users/{user_id}?_version=27')
        if response.status_code == 200:
            ranking_data = response.json()
            if 'error' in ranking_data:
                return False
            return ranking_data

def calc_winrate(factions_stats):
    winrates = {}
    for faction in FACTIONS:
        games = factions_stats[faction]['games_count']
        wins = factions_stats[faction]['wins_count']
        winrates[faction] = (wins / games * 100) if games > 0 else None
    return winrates

def save_winrate(table, winrates, players_count):
    columns = ', '.join([factions_dict[f] for f in FACTIONS] + ['players_count', 'date'])
    values = ', '.join([f':{factions_dict[f]}' for f in FACTIONS] + [':players_count', ':date'])
    sql = f"INSERT INTO {table} ({columns}) VALUES ({values})"
    data = {factions_dict[f]: winrates[f] for f in FACTIONS}
    data['players_count'] = players_count
    data['date'] = datetime.now()
    db.execute(text(sql), data)
    db.commit()

def get_winrate(players):
    factions_stats = init_factions()
    factions_top = init_factions()
    factions_rank = init_factions()
    players_checked = 0
    top_players_checked = 0
    rank_players_checked = 0

    for player_id in players:
        info = get_ranking_info(player_id)
        if info and 'error' not in info:
            players_checked += 1
            is_top = 0 < info.get('position', 0) <= 200
            is_rank = info.get('rank_id') == 1
            if is_top:
                top_players_checked += 1
            if is_rank:
                rank_players_checked += 1
            for stat in info.get('faction_games_stats', []):
                faction = stat['faction']
                wins = int(stat['faction_games_stats']['wins_count'])
                games = int(stat['faction_games_stats']['games_count'])
                if faction in FACTIONS:
                    factions_stats[faction]['wins_count'] += wins
                    factions_stats[faction]['games_count'] += games
                    if is_top:
                        factions_top[faction]['wins_count'] += wins
                        factions_top[faction]['games_count'] += games
                    elif is_rank:
                        factions_rank[faction]['wins_count'] += wins
                        factions_rank[faction]['games_count'] += games
        time.sleep(3)

    print("Парсирование окончено")
    print(f"All players: {factions_stats}")
    print(f"Top players: {factions_top}")
    print(f"Rank players: {factions_rank}")
    print(f"Всего игроков парсировано: {players_checked}\nТоп игроков парсировано: {top_players_checked}\nИгроков на ранге 1 парсировано: {rank_players_checked}")

    # Calculate winrates
    overall_wr = None
    top_wr = None
    rank_wr = None
    if players_checked > 0:
        overall_wr = calc_winrate(factions_stats)
        save_winrate('overall_win_rate', overall_wr, players_checked)
    if top_players_checked > 0:
        top_wr = calc_winrate(factions_top)
        save_winrate('top_win_rate', top_wr, top_players_checked)
    if rank_players_checked > 0:
        rank_wr = calc_winrate(factions_rank)
        save_winrate('rank_win_rate', rank_wr, rank_players_checked)

if __name__ == "__main__":
    SEASON_ID = get_current_season_id()
    players = get_all_gwent_ids()
    get_winrate(players)
