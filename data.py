import pandas as pd
import json
import subprocess
import config
import os
from google.cloud import bigquery as bqc


def _access_statsperform_api(feed_name: str,
                             tourney_cal_id: str = None,
                             match_id: str = None
                             ):

    proxy_url = "http://127.0.0.1:3128"
    statsperform_base_url = 'https://api.performfeeds.com/soccerdata'
    auth_key = str(os.environ['STATSPERFORM_API_KEY'])

    master_dict = {
        '_rt':'b',
        '_fmt':'json'
    }

    if feed_name == 'match':
        assert tourney_cal_id is not None, "To access match feed data, a tournament calendar ID must be passed in."
        master_dict['tmcl'] = tourney_cal_id
        master_dict['_pgSz'] = 1000
    if feed_name == 'matchstats':
        assert match_id is not None, "To access match stats feed, a match ID must be passed in."
        master_dict['fx'] = match_id
        master_dict['detailed'] = 'yes'
        master_dict['people'] = 'yes'

    query_string = '&'.join([f'{k}={v}' for k, v in master_dict.items()])

    q_command = f""" curl -x "{proxy_url}" '{statsperform_base_url}/{feed_name}/{auth_key}/authorized?&{query_string}' """ if feed_name == 'tournamentcalendar' else \
        f""" curl -x "{proxy_url}" '{statsperform_base_url}/{feed_name}/{auth_key}?&{query_string}' """

    process = subprocess.run(
                q_command,
                shell=True,
                capture_output=True,
                check=False # Set to True if you want a CalledProcessError for non-zero exit codes
            )

    stdout = process.stdout
    json_output = json.loads(stdout)
    return json_output



# Get all matches of a tourneycal (one season of one competition) with MA1 feed
def _get_all_matches_in_tourneycal(tourney_cal_id: str):

    all_matches = _access_statsperform_api(feed_name='match',
                         tourney_cal_id=tourney_cal_id)

    return pd.DataFrame([x['matchInfo'] for x in all_matches['match']])



# calls statsperform api to get stats of one game then calculates team and player data.
def process_game_data(match_id):
    
    match_data = _access_statsperform_api(feed_name='matchstats',
                         match_id=match_id)
    
    # TODO: enable checking of big query tables to avoid repeat loading of certain games
    team_data = _aggregate_team_data(match_data)

    player_data = _aggregate_player_data(match_data)
    
    return team_data, player_data


# method to aggregate team data

def _aggregate_team_data(match_stats):

    # collect tournament calendar, competition and team information
    tourney_cal_id, tourney_cal_season = match_stats['matchInfo']['tournamentCalendar']['id'], \
        match_stats['matchInfo']['tournamentCalendar']['name']
    competition_id, competition_name = match_stats['matchInfo']['competition']['id'], \
        match_stats['matchInfo']['competition']['name']
    
    competitors = match_stats['matchInfo']['contestant']
    
    # turn match data into dataframe
    match_data = pd.DataFrame(match_stats['liveData']['lineUp'])

    match_list = []

    for _, team in match_data.iterrows():
        # turn statistics into numerics and get team object from competitor
        team_stats = pd.DataFrame(team.stat)
        team_stats['value'] = pd.to_numeric(team_stats.value)
        competitor = competitors[0] if competitors[0]['id'] == team.contestantId else competitors[1]

        # transpose statistics to make ts dataframe (team statistics)
        ts = team_stats[['type', 'value']].set_index('type').T

        stat_cols = ts.columns
        missing_cols = set(config.all_team_match_stats).difference(stat_cols)

        if missing_cols:
            missing_df = pd.DataFrame(0, index=ts.index, columns=list(missing_cols), dtype=float)
            ts = pd.concat([ts, missing_df], axis=1)

        # create dataframe for cs (config statistics)
        cs = pd.DataFrame({
            'competition_id': [competition_id],
            'competition_name': [competition_name],
            'tourney_cal_id': [tourney_cal_id],
            'tourney_cal_name': [tourney_cal_season],
            'match_id' : [match_stats['matchInfo']['id']],
            'match_date': match_stats['matchInfo']['date'],
            'team_id' : [str(competitor['id'])],
            'team_name': [str(competitor['shortName'])],
            'home' : True if competitor['position'] == 'home' else False,
        })

        team_row = pd.concat([cs.reset_index(drop=True), ts[config.all_team_match_stats].reset_index(drop=True)], axis=1)
        team_row['formationUsed'] = str(team.formationUsed)

        match_list.append(team_row)

    # return dataframe of len == 2 with rows representing both teams, sharing the same game_id
    return pd.concat(match_list).fillna(0)


# method to aggregate player data

def _aggregate_player_data(match_request):
    match_data = pd.DataFrame(match_request['liveData']['lineUp'])

    subs = pd.DataFrame(match_request['liveData']['substitute']).set_index('playerOnId').to_dict('index')

    list_of_dfs = []

    for _, team in match_data.iterrows():
        players = pd.DataFrame(team['player'])

        # replace substitution position with boolean column
        players['isSub'] = players.position.apply(lambda x: True if x == 'Substitute' else False)
        cols_to_keep = ['playerId', 'matchName', 'position', 'positionSide', 'isSub']

        # turn statistics json into pandas dataframe + make all numbers into floats
        stats = players.stat.apply(lambda lisa: {x['type']:float(x['value']) for x in lisa})
        players = pd.concat([players[cols_to_keep].reset_index(drop=True), pd.json_normalize(stats).reset_index(drop=True)], axis=1)

        # remove all players from dataframe who didn't play
        players = players[~players['minsPlayed'].isna()]

        # checks all subs in players dataframe, replace values with subbed out player info
        subbed_players = players[players['isSub'] == True]

        # replace substitution position info with player who subbed out for substitute
        for i, row in subbed_players.iterrows():
            sub_info = subs[row.playerId]
            subbed_out_player = players[players['playerId'] == sub_info['playerOffId']].iloc[0]

            players.at[i, 'position'] =  subbed_out_player.position
            players.at[i, 'positionSide'] =  subbed_out_player.positionSide
            players.at[i, 'formationPlace'] =  subbed_out_player.formationPlace

        # add config columns
        players.insert(0, 'match_id', match_request['matchInfo']['id'])
        players.insert(1, 'team_id', team.contestantId)
        config_cols = ['match_id', 'team_id'] + cols_to_keep

        # add all columns not found in player data to keep StatsPerform detailed player statistics schema 
        # (schema can be found in config.py)
        stat_cols = players.drop(config_cols, axis=1).columns
        missing_cols = set(config.all_player_match_stats).difference(stat_cols)

        if missing_cols:
            missing_df = pd.DataFrame(0, index=players.index, columns=list(missing_cols), dtype=float)
            players = pd.concat([players, missing_df], axis=1)

        list_of_dfs.append(players[config_cols + config.all_player_match_stats])

    # return dataframe of all players with their statistics
    return pd.concat(list_of_dfs, axis=0, ignore_index=True).fillna(0)
    