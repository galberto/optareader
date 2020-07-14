configsDict = { 
	"OptaTypeIdentifiers" : {
			"Game.Event" :  "F24", # game eventing in a given match
			"Possession.PossessionWave" :  "F28", # possesion by team in a given match
			"MatchData.Stat": "MatchResults", # game results in a given match
			"SoccerFeed.Player" : "PassMatrix",  # possesion by player in a given match
			"Team.Player": "SeasonStats", #season event statistcs por a given team player by player in a given season
			"Qualification.Type": "Standings", #season games statistcs of every team in a given season
			"PlayerChanges.Team" : "Squads", # players of every team in a given season
			"TimingType.TimingType": "results", # results of every match in a given season
      "Result.@Winner": "F7"  # Basic Live Feed
			},

	"OptaDictDates" : {
			"F24" :  "Game.@game_date",
			"F28" :  "Possession.@date",
			"MatchResults" : "MatchInfo.Date",
			"PassMatrix" : "SoccerFeed.@game_date",
			"results": "SoccerDocument.MatchData.MatchInfo.Date",
      "F7" : "MatchInfo.Date"
			},

	"OptaDictPlayersID" : {
			"F24" : "Game.Event.@player_id",
			"MatchResults" : "SoccerDocument.Team.Player.@uID",
			"PassMatrix" : "SoccerFeed.Player.Player.@player_id",
			"SeasonStats": "Team.Player.@player_id", 
			"Squads": "SoccerDocument.Team.Player.@uID",
			"results": "SoccerDocument.MatchData.TeamData.Goal.@PlayerRef",
      "F7" : "SoccerDocument.Team.Player.@uID"
			},

	"OptaDictTeams" : {
			"F24" : ["Game.@away_team_name","Game.@home_team_name" ],
			"F28" : ["Possession.@away_team_name", "Possession.@home_team_name"],
			"MatchResults" : "SoccerDocument.Team.Name",
			"PassMatrix" : ["SoccerFeed.@away_team_name","SoccerFeed.@home_team_name"],
			"SeasonStats": "Team.@name",
			"Standings": "SoccerDocument.Team.Name",
			"Squads": "SoccerDocument.Team.Name",
			"results": "SoccerDocument.Team.Name",
      "F7" : "SoccerDocument.Team.Name"
			},

	"OptaDictSeason" : {
			"F24" :  "Game.@season_name",
			"F28" :  "Possession.@season_name",
			"MatchResults" : ("T", "Competition.Stat", "season_name"),
			"PassMatrix" : "SoccerFeed.@season_name",
			"SeasonStats": "SeasonStatistics.@season_name",
			"Standings": "SoccerDocument.@season_name",
			"Squads": "SoccerDocument.@season_name",
			"results": "SoccerDocument.@season_name",
      "F7" : ("T", "Competition.Stat", "season_name")
			},

	"OptaDictCompetition" : {
			"F24" :  "Game.@competition_name",
			"F28" :  "Possession.@competition_name",
			"MatchResults" : "Competition.Name",
			"PassMatrix" : "SoccerFeed.@competition_name",
			"SeasonStats": "SeasonStatistics.@competition_name",
			"Standings": "SoccerDocument.@competition_name",
			"Squads": "SoccerDocument.@competition_name",
			"results": "SoccerDocument.@competition_name",
      "F7" : "Competition.Name"
			}
}


F24Dict = {
	"game" : "Game.@id",
	"events" : "Game.Event",
	"qualifiers" : "Game.Event.Q",
	"Away ID"  : "Game.@away_team_id",
	"Away Name"  : "Game.@away_team_name", 
	"Home ID"  : "Game.@home_team_id", 
	"Home Name"  : "Game.@home_team_name"
}

PassMatrixDict = {
	"game" : "SoccerFeed.@game_id",
	"player stats" : "SoccerFeed.Player",
	"player pass" : "SoccerFeed.Player.Player",
	"file Team id": "SoccerFeed.@team_id",
	"Away ID"  : "SoccerFeed.@away_team_id",
	"Away Name"  : "SoccerFeed.@away_team_name", 
	"Home ID"  : "SoccerFeed.@home_team_id", 
	"Home Name"  : "SoccerFeed.@home_team_name"
}


MatchResults = {
	"game" : "Competition.@uID",
	"Main FirstName" : "OfficialName.First",
	"Main LastName" : "OfficialName.Last",
	"Main Type" : "OfficialRef.@Type",
	"Main uID" : "MatchOfficial.@uID",
	"Assistants" : "AssistantOfficials.AssistantOfficial",
	"Players" : "SoccerDocument.Team.Player.PersonName",
	"Teams" : "SoccerDocument.Team",
	"Booking": "MatchData.TeamData.Booking",
	"Goals" : "MatchData.TeamData.Goal.Assist",
	"Substitution" : "MatchData.TeamData.Substitution",
	"PlayerStats" : "MatchData.TeamData.PlayerLineUp.MatchPlayer.Stat",
}


F7 = {
  "Players" : "SoccerDocument.Team.Player.PersonName",
  "Teams" : "SoccerDocument.Team",
  "LineUps" : "MatchData.TeamData.PlayerLineUp.MatchPlayer",
  "Substitution" : "MatchData.TeamData.Substitution",
  "game" : "Competition.@uID",
  "Booking": "MatchData.TeamData.Booking",
  "Goals" : "MatchData.TeamData.Goal.Assist",
  "officials" : "SoccerDocument.Team.TeamOfficial.PersonName"
}

SeasonStats = {
	"team" : "Team.@name",
	"Team Stats": "Team.Stat",
	"Team Players" : "Team.Player",
	"Team Player Stat": "Team.Player.Stat"
}

Standings = {
	"Teams" : "SoccerDocument.Team",
	"Standings" : "TeamStandings.TeamRecord.Standing",
	"Qualification" : "Qualification.Type.Team"
}

Squads = {
	"officials" : "SoccerDocument.Team.TeamOfficial.PersonName",
	"playersStat" : "SoccerDocument.Team.Player.Stat",
	"players" :"SoccerDocument.Team.Player",
	"officialsChanges": "PlayerChanges.Team.TeamOfficial.PersonName",
	"playersChanges" : "PlayerChanges.Team.Player",
	"playersChangesStats": "PlayerChanges.Team.Player.Stat",
	"Stadiums" : "SoccerDocument.Team.Stadium"	
}

Results = {
	"Teams" : "SoccerDocument.Team",
	"TeamsPath": "SoccerDocument.MatchData.TeamData",
	"Goals" : "SoccerDocument.MatchData.TeamData.Goal",
	"Results" : "SoccerDocument.MatchData.TeamData",
	"Officials" : "SoccerDocument.MatchData.MatchOfficials.MatchOfficial",
	"Match Info" : "SoccerDocument.MatchData.MatchInfo"
}

F28 = {
	"Intervals" : "Possession.PossessionWave.Intervals.IntervalLength.Interval",
	"LastX" : "Possession.PossessionWave.LastX.Last",
	"Overall": "Possession.PossessionWave.Overall",
	"Away ID"  : "Possession.@away_team_id",
	"Away Name"  : "Possession.@away_team_name", 
	"Home ID"  : "Possession.@home_team_id", 
	"Home Name"  : "Possession.@home_team_name"
}

RenamePlayersMaps = {
	"PassMatrix" : {
		"@player_id": "PlayerID",
		"@player_name": "name",
		"@position": "position",
		"teamName": "team"
		},
	"SeasonStats" : {
		"@player_id": "PlayerID" ,
		"@first_name": "name" ,
		"@last_name": "lastname" ,
		"@position": "position"		},
	"Squads" : {
		"STP.@uID": "PlayerID" ,
		"STP.Name": "name" ,
		"Position": "position" ,
		"ST.Name_team": "team" 
		},
	"MatchResults" : {
		"STP.@uID":"PlayerID" ,
		"First":"name" ,
		"Last":"lastname" ,
		"@Position":"position" ,
		"Name":"team" 
		},
  "F7" : {
    "STP.@uID":"PlayerID" ,
    "First":"name" ,
    "Last":"lastname" ,
    "@Position":"position" ,
    "Name":"team" 
    }

	}


EventsDicts = [
  {
    "Event_id": 1,
    "Name": "Pass",
    "Description": "Any pass attempted from one player to another - free kicks, corners, throw-ins, goal kicks and goal assists"
  },
  {
    "Event_id": 2,
    "Name": "Offside Pass",
    "Description": "Attempted pass made to a player who is in an offside position"
  },
  {
    "Event_id": 3,
    "Name": "Take On",
    "Description": "Attempted dribble past an opponent (excluding when qualifier 211 is present as this is 'overrun' and is not always a duel event)"
  },
  {
    "Event_id": 4,
    "Name": "Foul",
    "Description": "This event ID shown when a foul is committed resulting in a free kick"
  },
  {
    "Event_id": 5,
    "Name": "Out",
    "Description": "Shown each time the ball goes out of play for a throw-in or goal kick"
  },
  {
    "Event_id": 6,
    "Name": "Corner Awarded",
    "Description": "Ball goes out of play for a corner kick"
  },
  {
    "Event_id": 7,
    "Name": "Tackle",
    "Description": "Tackle = dispossesses an opponent of the ball - Outcome 1 = win & retain possession or out of play, 0 = win tackle but not possession"
  },
  {
    "Event_id": 8,
    "Name": "Interception",
    "Description": "When a player intercepts any pass event between opposition players and prevents the ball reaching its target. Cannot be a clearance."
  },
  {
    "Event_id": 9,
    "Name": "Turnover",
    "Description": "Unforced error / loss of possession - i.e. bad control of ball – Replaced with Unsuccessful Touch + Overrun in recent seasons"
  },
  {
    "Event_id": 10,
    "Name": "Save",
    "Description": "Goalkeeper event; saving a shot on goal. Can also be an outfield player event with qualifier 94 for blocked shot."
  },
  {
    "Event_id": 11,
    "Name": "Claim",
    "Description": "Goalkeeper event; catching a crossed ball"
  },
  {
    "Event_id": 12,
    "Name": "Clearance",
    "Description": "Player under pressure hits ball clear of the defensive zone or/and out of play"
  },
  {
    "Event_id": 13,
    "Name": "Miss",
    "Description": "Any shot on goal which goes wide or over the goal"
  },
  {
    "Event_id": 14,
    "Name": "Post",
    "Description": "Whenever the ball hits the frame of the goal"
  },
  {
    "Event_id": 15,
    "Name": "Attempt Saved",
    "Description": "Shot saved - this event is for the player who made the shot. Qualifier 82 can be added for blocked shot."
  },
  {
    "Event_id": 16,
    "Name": "Goal",
    "Description": "All goals"
  },
  {
    "Event_id": 17,
    "Name": "Card",
    "Description": "Bookings - will have red, yellow or 2nd yellow qualifier plus a reason"
  },
  {
    "Event_id": 18,
    "Name": "Player Off",
    "Description": "Player is substituted off"
  },
  {
    "Event_id": 19,
    "Name": "Player on",
    "Description": "Player comes on as a substitute"
  },
  {
    "Event_id": 20,
    "Name": "Player retired",
    "Description": "Player is forced to leave the pitch due to injury and the team have no substitutions left"
  },
  {
    "Event_id": 21,
    "Name": "Player returns",
    "Description": "Player comes back on the pitch"
  },
  {
    "Event_id": 22,
    "Name": "Player becomes goalkeeper",
    "Description": "When an outfield player has to replace the goalkeeper"
  },
  {
    "Event_id": 23,
    "Name": "Goalkeeper becomes player",
    "Description": "Goalkeeper becomes an outfield player"
  },
  {
    "Event_id": 24,
    "Name": "Condition change",
    "Description": "Change in playing conditions"
  },
  {
    "Event_id": 25,
    "Name": "Official change",
    "Description": "Referee or linesman is replaced"
  },
  {
    "Event_id": 27,
    "Name": "Start delay",
    "Description": "Used when there is a stoppage in play such as a player injury"
  },
  {
    "Event_id": 28,
    "Name": "End delay",
    "Description": "Used when the stoppage ends and play resumes"
  },
  {
    "Event_id": 30,
    "Name": "End",
    "Description": "End of a match period"
  },
  {
    "Event_id": 32,
    "Name": "Start",
    "Description": "Start of a match period"
  },
  {
    "Event_id": 34,
    "Name": "Team set up",
    "Description": "Team line up - qualifiers 30, 44, 59, 130, 131 will show player line up and formation"
  },
  {
    "Event_id": 35,
    "Name": "Player changed position",
    "Description": "Player moved to a different position but the team formation remained the same"
  },
  {
    "Event_id": 36,
    "Name": "Player changed Jersey number",
    "Description": "Player is forced to change jersey number, qualifier will show the new number"
  },
  {
    "Event_id": 37,
    "Name": "Collection End",
    "Description": "Event 30 signals end of half. This signals end of the match and thus data collection."
  },
  {
    "Event_id": 38,
    "Name": "Temp_Goal",
    "Description": "Goal has occurred but it is pending additional detail qualifiers from Opta. Will change to event 16."
  },
  {
    "Event_id": 39,
    "Name": "Temp_Attempt",
    "Description": "Shot on goal has occurred but is pending additional detail qualifiers from Opta. Will change to event 15."
  },
  {
    "Event_id": 40,
    "Name": "Formation change",
    "Description": "Team alters its formation"
  },
  {
    "Event_id": 41,
    "Name": "Punch",
    "Description": "Goalkeeper event; ball is punched clear"
  },
  {
    "Event_id": 42,
    "Name": "Good skill",
    "Description": "A player shows a good piece of skill on the ball – such as a step over or turn on the ball"
  },
  {
    "Event_id": 43,
    "Name": "Deleted event",
    "Description": "Event has been deleted – the event will remain as it was originally with the same ID but will be resent with the type altered to 43."
  },
  {
    "Event_id": 44,
    "Name": "Aerial",
    "Description": "Aerial duel – 50/50 when the ball is in the air – outcome will represent whether the duel was won or lost"
  },
  {
    "Event_id": 45,
    "Name": "Challenge",
    "Description": "When a player fails to win the ball as an opponent successfully dribbles past them"
  },
  {
    "Event_id": 47,
    "Name": "Rescinded card",
    "Description": "This can occur post match if the referee rescinds a card he has awarded"
  },
  {
    "Event_id": 49,
    "Name": "Ball recovery",
    "Description": "When a player takes possession of a loose ball"
  },
  {
    "Event_id": 50,
    "Name": "Dispossessed",
    "Description": "Player is successfully tackled and loses possession of the ball"
  },
  {
    "Event_id": 51,
    "Name": "Error",
    "Description": "Mistake by player losing the ball. Leads to a shot or goals as described with qualifier 169 or 170"
  },
  {
    "Event_id": 52,
    "Name": "Keeper pick-up",
    "Description": "Goalkeeper event; picks up the ball"
  },
  {
    "Event_id": 53,
    "Name": "Cross not claimed",
    "Description": "Goalkeeper event; cross not successfully caught"
  },
  {
    "Event_id": 54,
    "Name": "Smother",
    "Description": "Goalkeeper event; comes out and covers the ball in the box winning possession"
  },
  {
    "Event_id": 55,
    "Name": "Offside provoked",
    "Description": "Awarded to last defender when an offside decision is given against an attacker"
  },
  {
    "Event_id": 56,
    "Name": "Shield ball opp",
    "Description": "Defender uses his body to shield the ball from an opponent as it rolls out of play"
  },
  {
    "Event_id": 57,
    "Name": "Foul throw-in",
    "Description": "A throw-in not taken correctly resulting in the throw being awarded to the opposing team"
  },
  {
    "Event_id": 58,
    "Name": "Penalty faced",
    "Description": "Goalkeeper event; penalty by opposition"
  },
  {
    "Event_id": 59,
    "Name": "Keeper Sweeper",
    "Description": "When keeper comes off his line and/or out of his box to clear the ball"
  },
  {
    "Event_id": 60,
    "Name": "Chance missed",
    "Description": "Used when a player does not actually make a shot on goal but was in a good position to score and only just missed receiving a pass"
  },
  {
    "Event_id": 61,
    "Name": "Ball touch",
    "Description": "Used when a player makes a bad touch on the ball and loses possession. Outcome 1 – ball simply hit the player unintentionally. Outcome 0 – Player unsuccessfully controlled the ball."
  },
  {
    "Event_id": 63,
    "Name": "Temp_Save",
    "Description": "An event indicating a save has occurred but without full details. Event 10 will follow shortly afterwards with full details."
  },
  {
    "Event_id": 64,
    "Name": "Resume",
    "Description": "Match resumes on a new date after being abandoned mid game"
  },
  {
    "Event_id": 65,
    "Name": "Contentious referee decision",
    "Description": "Any major talking point or error made by the referee – decision will be assigned to the relevant team"
  },
  {
    "Event_id": 66,
    "Name": "Possession Data",
    "Description": "Possession event will appear every 5 mins **No longer recorded in the feed**"
  },
  {
    "Event_id": 67,
    "Name": "50/50",
    "Description": "2 players running for a loose ball - GERMAN ONLY. Outcome 1 or 0. | No longer collected as of June 2017"
  },
  {
    "Event_id": 68,
    "Name": "Referee Drop Ball",
    "Description": "Delay - ref stops - this to event given to both teams on restart. No Outcome"
  },
  {
    "Event_id": 69,
    "Name": "Failed to Block",
    "Description": "Attempt to block a shot or pass - challenge lost. Put Through (qualifiers 266) is the winning duel event. | Collected for DFL competitions only between 2013/14 and 2016/17"
  },
  {
    "Event_id": 70,
    "Name": "Injury Time Announcement",
    "Description": "Injury Time awarded by Referee"
  },
  {
    "Event_id": 71,
    "Name": "Coach Setup",
    "Description": "Coach Type; 1,2,18,30,32,54,57,58,59"
  },
  {
    "Event_id": 72,
    "Name": "Caught Offside",
    "Description": "New event to just show player who is offside instead of offside pass event"
  },
  {
    "Event_id": 73,
    "Name": "Other Ball Contact",
    "Description": "This is an automated extra event for DFL. It comes with a tackle or an interception and indicates if the player who made the tackle/interception retained the ball after this action or if the tackle/interception was a single ball touch (other ball contact with type “interception”, type “Defensive Clearance” or type “ TackleRetainedBall”)."
  },
  {
    "Event_id": 74,
    "Name": "Blocked Pass",
    "Description": "Similar to interception but player already very close to ball"
  },
  {
    "Event_id": 75,
    "Name": "Delayed Start",
    "Description": "Match start delayed"
  },
  {
    "Event_id": 76,
    "Name": "Early end",
    "Description": "The match has had an early end"
  },
  {
    "Event_id": 77,
    "Name": "Player Off Pitch",
    "Description": "Event indicating that a player is now off the pitch"
  },
  {
    "Event_id": 79,
    "Name": "Coverage interuption",
    "Description": "Used to identify when our analysis of a game has been interupted. outcome 0 = start of interruption, outcome 1 = end of interruption"
  }
]
