import csv
import json

from pymongo import MongoClient
import sys, getopt, pprint

#Read country.csv
def read_country() :
   db = connection.test.Country_input
   db.drop()
   csvfile = open('D:\\UTA\Sem 3\\DB2- Elmasri\\Projects\\Project 2\\Input\\Country.csv', 'r')
   header= [ "Country", "Population", "No_of_Worldcup_won", "Manager", "Capital"]
   for row in csvfile:
      db.insert({header[0]:row.split(',')[0],
                 header[1]:float(row.split(',')[1]),
                 header[2]:float(row.split(',')[2]),
                 header[3]:row.split(',')[3],
                 header[4]:row.split(',')[4],
                 })
  
# Read Players.csv   
def read_player() :
   db = connection.test.Players_input
   db.drop()
   csvfile = open('D:\\UTA\\Sem 3\\DB2- Elmasri\\Projects\\Project 2\\Input\\Players.csv', 'r')
   header= [ "Player_id", "Name", "Fname", "Lname", "DOB", "Country", "Height", "Club", "Position", "Caps_for_country", "is_captain"]
   for row in csvfile:
      db.insert({header[0]:float(row.split(',')[0]),
                 header[1]:row.split(',')[1],
                 header[2]:row.split(',')[2],
                 header[3]:row.split(',')[3],
                 header[4]:row.split(',')[4],
                 header[5]:row.split(',')[5],
                 header[6]:float(row.split(',')[6]),
                 header[7]:row.split(',')[7],
                 header[8]:row.split(',')[8],
                 header[9]:float(row.split(',')[9]),
                 header[10]:row.split(',')[10]
                 })
    
#Read Match_results.csv
def read_match_results() :
   db = connection.test.Match_results_input
   db.drop()
   csvfile = open('D:\\UTA\\Sem 3\\DB2- Elmasri\\Projects\\Project 2\\Input\\Match_results.csv', 'r')
   header= [ "Match_id", "Date", "Start_time", "Team1", "Team2", "Team1_Score", "Team2_Score", "Stadium", "Host_city"]
   for row in csvfile:
      db.insert({header[0]:float(row.split(',')[0]),
                 header[1]:row.split(',')[1],
                 header[2]:row.split(',')[2],
                 header[3]:row.split(',')[3],
                 header[4]:row.split(',')[4],
                 header[5]:float(row.split(',')[5]),
                 header[6]:float(row.split(',')[6]),
                 header[7]:row.split(',')[7],
                 header[8]:row.split(',')[8]
                 
                 })
#Read player_card.csv
def read_player_card() :
   db = connection.test.Player_card_input
   db.drop()
   csvfile = open('D:\\UTA\\Sem 3\\DB2- Elmasri\\Projects\\Project 2\\Input\\Player_Cards.csv', 'r')
   header= [ "Player_id", "No_of_Yellow_cards", "No_of_Red_cards"]
   for row in csvfile:
      db.insert({header[0]:float(row.split(',')[0]),
                 header[1]:float(row.split(',')[1]),
                 header[2]:float(row.split(',')[2])
                 
                 })
#Read player_assists_goals.csv
def read_player_assist_goals() :
   db = connection.test.Player_assists_goals_input
   db.drop()
   csvfile = open('D:\\UTA\\Sem 3\\DB2- Elmasri\\Projects\\Project 2\\Input\\Player_Assists_Goals.csv', 'r')
   header= [ "Player_id", "No_of_matches", "Goals", "Assists", "Minutes_played"]
   for row in csvfile:
      db.insert({header[0]:float(row.split(',')[0]),
                 header[1]:float(row.split(',')[1]),
                 header[2]:float(row.split(',')[2]),
                 header[3]:float(row.split(',')[3]),
                 header[4]:float(row.split(',')[4])
                 
                 })
#Read world_cup_history.csv
def read_world_cup_history() :
   db = connection.test.World_cup_history_input
   db.drop()
   csvfile = open('D:\\UTA\\Sem 3\\DB2- Elmasri\\Projects\\Project 2\\Input\\Worldcup_history.csv', 'r')
   header= [ "Year", "Host", "Winner"]
   for row in csvfile:
      db.insert({header[0]:row.split(',')[0],
                 header[1]:row.split(',')[1],
                 header[2]:row.split(',')[2]
                 
                 })


#Creating a country document
def write_Country_doc():
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
#creating a stadium document
def write_Stadium_doc():
    #Stadium Document
    
    db = connection.test
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
    





 
connection = MongoClient('localhost', 27017)
read_country()
read_player()
read_match_results()
read_player_card()
read_player_assist_goals()
read_world_cup_history()
write_Country_doc()
write_Stadium_doc()


connection.close()
