import csv
import json

from pymongo import MongoClient
import sys, getopt, pprint


connection = MongoClient('localhost', 27017)
db = connection.test

#Country_document
db.Country_doc.drop()
allCountries = db.Country_input.find()
print 'hello'
for country in allCountries:
    print country['Country']


    #Players that are playing for mthe country
    players_list = db.Players_input.find({'Country':country['Country']})
    player_array = []
    for player in players_list:
        #print player['Player_id']
        player_cards = db.Player_card_input.find_one({'Player_id': player['Player_id']})
        #print player_cards
        player_assists_goals = db.Player_assists_goals_input.find_one({'Player_id': player['Player_id']})
        #print player_assists_goals

        No_of_Yellow_cards = 0.0
        No_of_Red_cards = 0.0
        if (player_cards):
            No_of_Yellow_cards = player_cards['No_of_Yellow_cards']
            No_of_Red_cards = player_cards['No_of_Red_cards']
##        else:
##            #print 'record not found'

        No_Goals = 0.0
        No_Assists = 0.0
        if (player_assists_goals):
            No_goals = player_assists_goals['Goals']            
            No_Assists = player_assists_goals['Assists']
        

        player_info = {
            'Player_id': player['Player_id'],
            'Lname': player['Lname'],
            'Fname' : player['Fname'],
            'Height': player['Height'],
            'DOB' : player['DOB'],
            'is_captain' : player['is_captain'],
            'Position': player['Position'],
            'No_Yellow_cards' : No_of_Yellow_cards,
            'No_Red_cards' :  No_of_Red_cards,
            'No_Goals' : No_Goals,
            'No_Assists' : No_Assists
            }
        player_array.append(player_info)

    #World_cup history
    worldcup_history_array = []    
    if (country['No_of_Worldcup_won']==0):
        worldcup_history_array = []
    
    else:
        Worldcups_won = db.World_cup_history_input.find({'Winner': country['Country']})
        for worldcup in Worldcups_won:
            print 'hello---'
            worldcup_string = {
                'Year': worldcup['Year'],
                'Host': worldcup['Host']
                }
            worldcup_history_array.append(worldcup_string)
    
    db.Country_doc.insert({
        'Cname': country['Country'],
        'Capital': country['Capital'],
        'Population':country['Population'],
        'Manager' :country['Manager'],
        'No_of_Worldcup_won' :country['No_of_Worldcup_won'],
        'players':player_array,
        'Worldcup_history': worldcup_history_array
        })
    


#Stadium Document
db.Stadium_doc.drop()
distinct_stadium = db.Match_results_input.distinct('Stadium')
for stadium in distinct_stadium:
    print stadium
    stadium_city = db.Match_results_input.find_one({'Stadium':stadium})['Host_city']
    print stadium_city
    #for match details
    stadium_details = db.Match_results_input.find({'Stadium':stadium})
    Match_History = []
    for each_stadium in stadium_details:
        
        Match = {
            'Team1' : each_stadium['Team1'],
            'Team2' : each_stadium['Team2'],
            'Team1_score' : each_stadium['Team1_Score'],
            'Team2_score' : each_stadium['Team2_Score'],
            'Date' : each_stadium['Date']
            }
        Match_History.append(Match)

    db.Stadium_doc.insert({
        'Stadium': stadium,
        'City': stadium_city,
        'Matches' : Match_History
        })
    




connection.close()
