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



def get_statsperform_tourneys(to_bq=False):
    # Get all available tournament calendar IDs with OT2 feed
    feed = 'tournamentcalendar'
    comps = _access_statsperform_api(feed)
    competitions = pd.DataFrame(comps['competition'])
    competitions = competitions.explode('tournamentCalendar')

    extended_tourneys = pd.json_normalize(competitions['tournamentCalendar'])
    extended_tourneys.columns = ['tourneyCalId', 'includesVenues', 'tcOcId', 'tcName', 'startDate', 'endDate', 'active', 'lastUpdated', 'includesStandings']

    final_df = pd.concat([competitions.drop(columns=['tournamentCalendar']).reset_index(drop=True),
                      extended_tourneys.reset_index(drop=True)], axis=1)
                                
    if to_bq:
        try:
            final_df.to_gbq('soccer_simulations.tourneycal_data',
                    'prizepicksanalytics',
                    if_exists='fail')
        except Exception as e:
            print("Table already exists on BQ!")

    return final_df


# Get all matches of a tourneycal (one season of one competition) with MA1 feed
def _get_all_matches_in_tourneycal(tourney_cal_id: str):

    all_matches = _access_statsperform_api(feed_name='match',
                         tourney_cal_id=tourney_cal_id)

    return pd.DataFrame([x['matchInfo'] for x in all_matches['match']])



# getting access to player data using MA2 feed
def backload_season_data(tourney_cal_id: str, 
                         game_limit=None,
                         project_name: str = 'prizepicksanalytics',
                        table_name: str = f'soccer_simulations.schema_match_data'):
    
    matches = _get_all_matches_in_tourneycal(tourney_cal_id)
    processed_team_match_ids = []
    processed_player_match_ids = []

    try:
        processed_team_match_ids = get_processed_team_match_ids()
        processed_team_match_ids = list(processed_team_match_ids.match_id.unique())
    except Exception as e:
        print('team table doesnt exist')

    try:
        processed_player_match_ids = get_processed_player_match_ids()
        processed_player_match_ids = list(processed_player_match_ids.match_id.unique())
    except Exception as e:
        print('player table doesnt exist')

    list_of_matches = matches.id.unique()
    processed_matches = list(set(processed_team_match_ids) & set(processed_player_match_ids))
    list_of_matches = [x for x in list_of_matches if x not in processed_matches]

    if game_limit:
        list_of_matches = list_of_matches[:game_limit]

    all_teams = []
    all_players = []

    for i, id in enumerate(list_of_matches):

        if id in processed_team_match_ids and id in processed_player_match_ids:
            continue

        game_stats = None

        if id not in processed_team_match_ids:

            print(f"Now processing team match stats #{i}: {id}")
            game_stats = get_game_stats(id)
            team_stats = _aggregate_team_data(game_stats)
            all_teams.append(team_stats)
            
        if id not in processed_player_match_ids:
            if game_stats == None:
                game_stats = get_game_stats(id)

            print(f"Now processing player match stats #{i}: {id}")
            player_stats = _aggregate_player_data(game_stats)
            all_players.append(player_stats)
        


    if all_teams :
        team_data_to_bq = pd.concat(all_teams, axis=0, ignore_index=True)
        team_data_to_bq.to_gbq(table_name.replace('schema', 'team'),
                project_name,
                if_exists='append')
        
    if all_players:
        player_data_to_bq = pd.concat(all_players, axis=0, ignore_index=True)
        player_data_to_bq.to_gbq(table_name.replace('schema', 'player'),
                project_name,
                if_exists='append')
        
    print(f"Data Uploaded for the following Game Ids: {list_of_matches}")
    #     return team, player
    # else:
    #     return None, None


# calls statsperform api to get stats of one game then calculates team and player data.
def get_game_stats(match_id):
    
    match_data = _access_statsperform_api(feed_name='matchstats',
                         match_id=match_id)
    
    # # TODO: enable checking of big query tables to avoid repeat loading of certain games
    # team_data = _aggregate_team_data(match_data)

    # player_data = _aggregate_player_data(match_data)
    
    return match_data


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

    competitors = match_request['matchInfo']['contestant']

    list_of_dfs = []

    for _, team in match_data.iterrows():
        competitor = competitors[0] if competitors[0]['id'] == team.contestantId else competitors[1]
        home = True if competitor['position'] == 'home' else False

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
        players.insert(1, 'match_date', match_request['matchInfo']['date'])
        players.insert(2, 'team_id', team.contestantId)
        players.insert(3, 'home', home)
        config_cols = ['match_id', 'match_date', 'team_id', 'home'] + cols_to_keep

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
    

def get_processed_team_match_ids():
    team_query = """
        select distinct match_id
        from prizepicksanalytics.soccer_simulations.team_match_data
    """

    return execute_bq_query(team_query)    


def get_processed_player_match_ids():
    player_query = """
        select distinct match_id
        from prizepicksanalytics.soccer_simulations.player_match_data
    """

    return execute_bq_query(player_query)



# DEPRECATED!: DataFrame should be of only one match
def check_bq_duplicates(dataframe: pd.DataFrame,
                        schema: str,
                        project_name: str = 'prizepicksanalytics',
                        table_name: str = f'soccer_simulations.schema_match_data'):
    
    assert dataframe.shape[0] != 0, "Empty dataframes not accepted"
    assert schema in ['team', 'player'], "Only team and player data accepted"
    table_name = table_name.replace('schema', schema)

    match_ids = list(dataframe.match_id.unique())

    column_key = 'team_id' if schema == 'team' else 'playerId'

    query = f"""
        select match_id, {column_key}
        from {project_name}.{table_name}
        where match_id in ({", ".join(["'" + x + "'" for x in match_ids])}) 
    """

    rows_on_bq = execute_bq_query(query)

    if rows_on_bq.shape[0] == dataframe.shape[0]:
        print("Data already loaded onto BQ!")
        return
    else:
        dataframe = dataframe.merge(rows_on_bq,
                                    on=['match_id', column_key],
                                    how='left',
                                    indicator=True)
        
        dataframe = dataframe[dataframe['_merge'] == 'left_only'].drop(columns=['_merge'])
        

    return dataframe



def execute_bq_query(query: str) -> pd.DataFrame:
    bigquery_client = bqc.Client(project='prizepicksanalytics')
    query_results = bigquery_client.query(query)
    data = query_results.to_dataframe()
    bigquery_client.close()

    return data
