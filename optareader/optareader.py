import os
from pathlib import Path
import fnmatch
import os
import xmltodict
import threading
import pandas as pd
import numpy as np
from .custom_functions import *
from .dictionary import *
import concurrent.futures
import collections
import matplotlib.pyplot as plt
from matplotlib.patches import Arc
from random import shuffle



####################################################################################
########################             Start Class            ########################
########################            to work with            ########################
########################             Directories            ########################
########################           with xml files           ########################
####################################################################################
class OptaCatalog:
	"""Collection of opta xml files 

	Attributes:
		xmlfiles (list): with all the xml collected files 
		xmlfilesCount (int): number of collected files found in folder
		FileStruct (dict): structure of files 
        
	params:
		location (str): parent folder with all opta xml
    """

	def __init__(self, location:str):
		self.xmlfiles = self.getListOfFiles(location)
		self.xmlfilesCount = len(self.xmlfiles)
		
		summary = self.getFilesOptaType()
		self.OptaFiles = summary['OptaFiles']
		self.OptaTeams = summary['teams']
		self.Teams = self.OptaTeams.keys()
		self.OptaSeasons = summary['seasons']
		self.Seasons = self.OptaSeasons.keys()
		self.OptaCompetition = summary['competitions']
		self.Competitions = self.OptaCompetition.keys()
		
		self.OptaPlayersID = summary['playersID']
		self.PlayersID = self.OptaPlayersID.keys()


	def getListOfFiles(self, dirName:str):
		""" Get the list of files with xml extension in a directory
        
		params:
			dirName(str): directory where the files are located

		return:
			xmlfiles(list): list of files with xml extension

		"""
		xmlfiles = []
		#iterateting over all files and subbiles containing xml files
		for root, dirnames, filenames in os.walk(dirName):
			for filename in fnmatch.filter(filenames, '*.xml'):
				xmlfiles.append(os.path.join(root, filename))
		return xmlfiles


	def getFilesOptaType(self):
		"""
		Iterate over all files classificate it and add to a dict with all data

		params:
			just it self

		return:
			dict with all the files in each category (Opta types, dates, teams, players, seasons and competitions)

		"""

		#Output types
		OptaFiles = {}
		OptaTeams = {}
		OptaSeasons = {}
		OptaCompetitions = {}
		OptaPlayersID = {}



		#Create function to run in parallel bucle
		def worker(x):
			#create an instance of OptaFile class to get access to al his methods and clasificate it
			fileOpta = OptaFile(x)
			#CreateNewKey function make a key for each type if it doesn't exist
			#Some data is simple value other ones are dicts.
			OptaFileList = createNewKey(path=OptaFiles, newKey=fileOpta.OptaType, datatype=list())
			OptaFileList.append(x)


			try:
				for a in fileOpta.players:
					OptaPlayerList = createNewKey(path=OptaPlayersID, newKey=a, datatype=dict())
					OptaPlayerList = createNewKey(path=OptaPlayerList, newKey=fileOpta.OptaType, datatype=list())
					OptaPlayerList.append(x)
			except:
				pass

			try:		
				if isinstance(fileOpta.teams, str):
					OptaTeamList = createNewKey(path=OptaTeams, newKey=fileOpta.teams, datatype=dict())
					OptaTeamList = createNewKey(path=OptaTeamList, newKey=fileOpta.OptaType, datatype=list())
					OptaTeamList.append(x)

				else:
					for a in fileOpta.teams:
						OptaTeamList = createNewKey(path=OptaTeams, newKey=a, datatype=dict())
						OptaTeamList = createNewKey(path=OptaTeamList, newKey=fileOpta.OptaType, datatype=list())
						OptaTeamList.append(x)
			except:
				pass
				
			OptaSeasonsList = createNewKey(path=OptaSeasons, newKey=fileOpta.season, datatype=list())
			OptaSeasonsList.append(x)

			OptaCompsList = createNewKey(path=OptaCompetitions, newKey=fileOpta.competition, datatype=list())
			OptaCompsList.append(x)
			
		#run clasificator in parallel for all xml files found en OptaCatalog
		with concurrent.futures.ThreadPoolExecutor() as executor:
			result = executor.map(worker, self.xmlfiles)

		summary = {}
		summary['OptaFiles'] = OptaFiles
		summary['teams'] = OptaTeams
		summary['playersID'] = OptaPlayersID
		summary['seasons'] =  OptaSeasons
		summary['competitions'] = OptaCompetitions
		for i in summary:
			summary[i].pop(None, None)

		return summary


	def optasCount(self):
		""" get the count of opta files by types
        
		params:
			self

		return:
			optasCount(list): for each opta type in OptaDictKeys we get a element in list with 
			the type and the count of files found.
			This functions is originally intended to be shown in the summary
		"""

		optasCount = []
		self.OptasFilesCount = 0
		for i in self.OptaFiles:
			counts = '{} : {} '.format(i, len(self.OptaFiles[i]) )
			self.OptasFilesCount = self.OptasFilesCount+len(self.OptaFiles[i])

			optasCount.append(counts)
		
		return optasCount 

	def showSummary(self):
		""" get the summary of files found
        
		params:
			self

		return:
			toprint(str): string with the count of files found by type
		"""
		optasCount =  self.optasCount()
		toPrint = []
		toPrint.append("total xml files: {}".format(self.xmlfilesCount))
		
		toPrint.append("OptasFiles: {}".format(self.OptasFilesCount) )
		for x in optasCount:
			toPrint.append("\t {}".format(x))

		toPrint.append("Teams: {}".format(len(self.OptaTeams)))
		toPrint.append("Players: {}".format(len(self.PlayersID)))
		toPrint.append("Seasons: {}".format(len(self.OptaSeasons)))
		toPrint.append("Competitions: {}".format(len(self.OptaCompetition)))		

		toPrint = '\n'.join(toPrint)
		return toPrint

	def eventCatalog(self):
		df = pd.DataFrame(EventsDicts)  		
		return df


	def getPlayersDict(self):
		""" show the list of players in all files
        
		params:
			self

		return:
			dataframe
		"""
		files = {}
		for i in self.OptaFiles:
			for a in self.OptaFiles[i]:
				if i in ['MatchResults', 'PassMatrix', 'SeasonStats', 'Squads']:
					files[a] = i

		dfs = []
		def mappingPlayers(x):
			optatype = files[x]
			if optatype == 'MatchResults':
				try:
					obj = OptaMatchResults(x)
				except:
					pass
			elif optatype == 'PassMatrix':
				try:
					obj = OptaPassMatrix(x)
				except:
					pass

			elif optatype == 'SeasonStats':
				try:
					obj = OptaSeasonStats(x)
				except:
					pass
			elif optatype == 'Squads':
				try:
					obj = OptaSquads(x)	
				except:
					pass

			try:
				dfs.append(obj.players)
			except:
				pass

		#run clasificator in parallel for all xml files found en OptaCatalog
		with concurrent.futures.ThreadPoolExecutor() as executor:
			result = executor.map(mappingPlayers, files)

		return pd.concat(dfs)

####################################################################################
########################             Start Class            ########################
########################            to work with            ########################
########################             opta Files             ########################
####################################################################################

class OptaFile:
	"""Read an xml file and extract opta data 

	Attributes:
		location (str) - directory of the file
		fullDict (dict) - all data in xml file loeaded as json 
		Catalog (dict) - field identificators of all the data in file with. key is the 
			field's name and value is a dict with {path : data location, 
			datatype: kind of value (df, list or value), parent:location to the parent of the data}
		baseCatalog (dict) - first iteration of the catalog with the main keys of the file
		OptaType (str) : Type of opta file
		date (str) :Data of the file
		teams (list) :Teams in the file
		season (str) :Season in the file
		competition (str) :competition in the file
		players (str) :Players in the file


	params:
		location (str): physical location of file
		configsDict (dict): optional - Dict with path that identificate each ind of opta file 
			and with paths to get specific date as dates, temas, players...
    """


	def __init__(self, location:str, **kwargs):
		configs = kwargs['configsDict'] if 'configsDict' in kwargs else configsDict

		self.location = location
		self.fullDict = getJson(self.location)
		self.baseCatalog = {}
		getJsonCatalog(self.fullDict, output=self.baseCatalog) 
		self.OptaType = clasiJson(self.baseCatalog, configs['OptaTypeIdentifiers'])
		# self.Catalog = self.enrichCatalog()
		self.Catalog = {}
		self.enrichCatalog(self.Catalog) 


		self.date = self.getDataFromCatalog(dictPath = configs['OptaDictDates'])
		self.teams = self.getDataFromCatalog(dictPath = configs['OptaDictTeams'])
		self.season = self.getDataFromCatalog(dictPath = configs['OptaDictSeason'])
		self.competition = self.getDataFromCatalog(dictPath = configs['OptaDictCompetition'])
		players = self.getDataFromCatalog(dictPath = configs['OptaDictPlayersID'])
		players = list(dict.fromkeys(players)) if players is not None else []
		self.players = list(x[1:] if 'p' in str(x) else str(x) for x in players)



	def getStruct(self):
		"""
		get the file structure as: {path: identificator} allos to know what data is in each dict path
		we understand a path as a group of key in a dict -> [field][subfield][subsubfield]

		params
			just itself

		return 
			dict

		"""
		self.struct = {}
		getJsonStruct(self.fullDict, output=self.struct)
		return self.struct


	def enricher(self, Catalogfield:str):
		"""
		return a field from a base catalog as a built in type or a dataframe. 

		params:
			Catalogfield: a field of base catalog

		return 
			specific value in file. Could be a pandas dataframe or a built in type 
		"""
		searchPath = self.baseCatalog[Catalogfield]
		
		if isinstance(searchPath, str):
			value = eval("self.fullDict"+searchPath)
			if isinstance(value, list):
				value = pd.DataFrame(value)

		return value


	def enrichCatalog(self, output):
		"""
		iterate over base catalog. Search on all field and return the full catalog allowing to acces all data
		
		params:
			output (dict): dict where the full catalog will goes to

		return:
			None

		"""
		#Iterate over the base catalog
		for i in self.baseCatalog:
			#Check if field is a single value or a list of dicts
			FielOrFrame = self.enricher(i)

			# in first iteration saves the values of first values
			if not isinstance(FielOrFrame, pd.DataFrame):
				parent = RemoveLastStep(i)
				output[i] = {}
				output[i]['path'] = self.baseCatalog[i]
				output[i]['datatype'] = 'Value'
				output[i]['parent'] = parent

			#if value is a dataframe check every value and try to convert in Dataframe and iterate over each column recursively
	
			else:
				searchOnPanda(df = FielOrFrame, outputDict = output, ParentKey = i, ParentValue= self.baseCatalog[i] )



	def giveMe(self, Catalogfield:str, tryPandas=True, listAsPandas=True, **kwargs):
		"""
		Allow to get a specific field from the file 

		Params:
			Catalogfield (str) : field from catalog looked
			tryPandas (bool) : default True. When true if filed is a list of dict retrieves a pandas dataframe else return the value as it comes
			listAsPandas (bool) : default True. When true return a pandas series with index. When False return a list of values
			catalog (str) : optional. Allows to use a diferent catalog

		Return
			specific value in file. Could be a pandas dataframe or a built in type 

		"""
		#setting catalog to use.
		catalog = self.Catalog if 'catalog' not in kwargs else kwargs['catalog']
		searchPath = catalog[Catalogfield]['path']
		dataType = catalog[Catalogfield]['datatype']
		parent = catalog[Catalogfield]['parent']
		
		
		if isinstance(searchPath, str):
			value = eval("self.fullDict"+searchPath)
			if tryPandas:
				if isinstance(value, list):
					value = pd.DataFrame(value)
					return value
		
		if isinstance(searchPath, list):
			value = eval("self.fullDict"+searchPath[0])
			value = pd.DataFrame(value) 

			for i, v in enumerate(searchPath[1]):
				if i+1 == len(searchPath[1]):
					if dataType == 'list':
						value = value[v] if listAsPandas else value[v].values

					elif dataType == 'Dataframe':
						value = pandasFromList(value[v], parentIndex=parent+".index")
				else:
					value = pandasFromList(value[v])

		return value

	def getDFsWithXParentLvl(self, path, levels=2):
		"""
		return dataframe and dataframe's parents for a given path and a given number of levels.

		params:
			path (str): a specific key from Catalog
			levels (int): default 1 to get dataframe. 2 returns df and it's parents. 

		return:
			dict as {path : {df : pd.Dataframe, parent: str, df2 : pd.Dataframe, parent: str}
		"""
		df = {}
		runningPath = path

		for i in range(levels):
			#if file has less levels than the passed in params get the exception
			try:
				dfToAdd = self.giveMe(runningPath) 
				df[runningPath] = {} 
				#Saving DF from path
				inDC = self.Catalog[runningPath]
				df[runningPath]['df'] = dfToAdd 
				df[runningPath]['parent'] = inDC['parent']
				#changin path value to get parent in next iteration
				runningPath = inDC['parent']
			except Exception as e:
				error = e
		#if only get excepetions and no dataframe is added to dict then show the last error
		output = df if len(df) >= 1 else error
		return output
			

	def giveMeDF(self, path, levels=1): 
		"""
		Specific for Dataframes. Allows to get a dataframe joined with it's parents

		params:
			path (str) : field in Catalog
			levels (int): default 1 to get dataframe. 2 returns df and it's parents. 

		return:
			dataframe
		"""
		dfs = self.getDFsWithXParentLvl(path, levels) 
		# if getDFsWithXParentLvl doesn't return data show message
		if isinstance(dfs, KeyError):
			return "incorrect path, dataframes not found: {}".format(dfs)


		#get the df to enrich with parents
		MainDF = dfs[path]['df']
		#adding prefix to columns names to avoid names repetitions
		alteredNames = {}
		

		mainPrefix = getPrefix(path)	
		for i in MainDF:
			alteredNames[i] = []
			alteredNames[i].append(mainPrefix+i)
		MainDF = MainDF.add_prefix(mainPrefix)


		#get the first table to join 
		mainparent = dfs[path]['parent']

		#iterating one time for each element but the first
		for a in range(len(dfs)-1):
			parentDF = dfs[mainparent]['df']		
			parentPrefix = getPrefix(mainparent)
			for i in parentDF:
				# alteredNames[i] = []
				appendTo = createNewKey(path=alteredNames, newKey=i, datatype=list())
				alteredNames[i].append(parentPrefix+i)

			parentDF = parentDF.add_prefix(parentPrefix)

			MainDF = MainDF.merge(parentDF, how='left', 
		                                 	# left_on='region', 
		                                 	right_index=True,
		                                	left_on='{}{}.index'.format(mainPrefix, mainparent)
											,suffixes=('','_prnt')
											)
			mainPrefix = parentPrefix
			mainparent = dfs[mainparent]['parent']

		
		for a in MainDF:
			field = a.partition(".")[2]
			if len(alteredNames[field]) == 1:
				MainDF = MainDF.rename(columns={a: field})


		return MainDF


	def getDataFromCatalog(self, dictPath):
		"""
		Allows to get specific data depending the type of path in dictionary

		params:
			dictPath (dict): as type of file and path with data

		return:
			(list or str) - with data of the specific path

		"""
		path = dictPath[self.OptaType] if self.OptaType in dictPath else None

		if isinstance(path, str):
			return self.giveMe(path, listAsPandas=False)
		
		elif isinstance(path, list):
			listRet = []
			for i in path:
				listRet.append(self.giveMe(i, listAsPandas=False))
			return listRet

		elif isinstance(path, tuple):
			if path[0] == 'T':
				df = self.giveMe(path[1])
				df = df.set_index(list(df)[0]).T
				return df[path[2]].values

	def checkType(self, checkIf:str):
		"""
		check if the Opta file is of a specific type

		params:
			checkIf : type to check

		return:
			bool or raise an error
		"""
		if self.OptaType != checkIf:
			raise Exception("file doesn't look like a {} Opta File".format(checkIf))

		return True

####################################################################################
########################             Start Class            ########################
########################            to work with            ########################
########################              F24 Files             ########################
####################################################################################


class OptaF24(OptaFile):
	"""Class to work specifically with OptaF24 Files 

	Attributes:
		configs (dict) - dictionary with data definition and location in catalog
		match (str) - game identificator

	params:
		location (str): physical location of file
		F24Dict (dict): optional - Dict with catalogs path that identificate the location of data in file
    """

	def __init__(self, location:str, **kwargs):
		super().__init__(location, **kwargs)
		self.checkType('F24')
		self.configs = kwargs['F24Dict'] if 'F24Dict' in kwargs else F24Dict
		self.match = self.giveMe(self.configs['game'])


	def getTeamsDict(self, **kwargs):
		"""
		Get data about name and rival of each team
		
		away_id (str) : optional - catalog path where away team id located. If not setted uses default
		away_name (str) : optional - catalog path where away team name is located. If not setted uses default
		home_id (str) : optional - catalog path where home team id is located. If not setted uses default
		home_name (str) : optional - catalog path where home name is located. If not setted uses default

		return dict
		"""
		away_id = kwargs['away_id'] if 'events' in kwargs else self.configs['Away ID']
		away_name = kwargs['away_name'] if 'events' in kwargs else self.configs['Away Name']
		home_id = kwargs['home_id'] if 'events' in kwargs else self.configs['Home ID']
		home_name = kwargs['home_name'] if 'events' in kwargs else self.configs['Home Name']
		
		teamsDict = {}
		aid = self.giveMe (away_id)
		aname = self.giveMe (away_name)
		hid = self.giveMe (home_id)
		hname = self.giveMe (home_name)
		hrival = self.giveMe (away_name)
		arival = self.giveMe (home_name)

		teamsDict['rival'] = {}
		teamsDict['name'] = {}

		teamsDict['name'][aid] = aname
		teamsDict['rival'][aid] = arival
		teamsDict['name'][hid] = hname
		teamsDict['rival'][hid] = aname

		return teamsDict


	def getEvents(self, **kwargs):
		"""
		create event dataframe and return specific filtered fields and rows
		
		fields (list) : optional fields to return , 
		removeFields (list): columns to exclude from finalDataframe
		filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
		exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
		constants (dict) : dict as {"column to add": "constant Value"}
		events (str) : optional - catalog path where main events is located. If not setted uses default
		
		return DataFrame : with events Data
		"""
		events = kwargs['events'] if 'events' in kwargs else self.configs['events']

		originalDF =  self.giveMeDF(events)

		teamsDict = self.getTeamsDict()
		originalDF['teamName'] = originalDF['@team_id'].map(teamsDict['name'])
		originalDF['rival'] = originalDF['@team_id'].map(teamsDict['rival'])
		pruned = pruningDF(originalDF, **kwargs )
		return pruned


	def getQualifiedEvents(self, **kwargs):
		"""
		create event with qualifieras dataframe and return specific filtered fields and rows
		fields with same name in both tables have a suffix "prnt_" in parent field
	
		fields (list) : optional fields to return , 
		removeFields (list): columns to exclude from finalDataframe
		filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
		exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
		constants (dict) : dict as {"column to add": "constant Value"}
		qualifiers (str) : optional - catalog path where qualifiers events is located. If not setted uses default

		return DataFrame : with events Data and qualifiers. Events ara many time as qualifiers have the event

		"""
		qualifiers = kwargs['qualifiers'] if 'qualifiers' in kwargs else self.configs['qualifiers']

		df = self.giveMeDF(qualifiers, levels=2)
		
		teamsDict = self.getTeamsDict()
		df['teamName'] = df['@team_id'].map(teamsDict['name'])
		df['rival'] = df['@team_id'].map(teamsDict['rival'])
		
		pruned = pruningDF(df, **kwargs)
		return pruned


	def PlotRanking(self, groupBy:str, metric:str, agg:str, **kwargs):
		"""
		Make a horizontal bar chart with event data order desc grouping with groupBy param and aggregating a metric with agg param
		
		params:
			groupBy (str) : column name to group by the metric
			metric (str) : column name to summarize
			agg (str) : kind of aggregation. Can be sum, min, max, mean, std, count, nunique
			top (int) : number of rows to show. default 50
			qualified (bool) : when true retrieves dataframe with qualifiers else only events
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
		
		return:
			dataframe
		"""
		top = kwargs['top'] if 'top' in kwargs else 50

		qualified = kwargs['qualified'] if 'qualified' in kwargs else False
		data = self.getQualifiedEvents(**kwargs) if qualified else self.getEvents(**kwargs)

		getRanking(df=data, groupBy=groupBy, metric=metric, agg=agg, top=top )


	def PlotPitchEvents(self, **kwargs):
		"""
		Show a footbaal pitch with events location
		
		params:
			agg (str) : kind of aggregation. Can be sum, min, max, mean, std, count, nunique
			qualified (bool) : when true retrieves dataframe with qualifiers else only events
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			marker (str) : optional - column name to set as marker type
			color (str) : optional - column name to set as color
			tag (str) : optional - column name to show as tag
			recolor (bool) : optoional allows to change colors settigns randomly
			title (str) : Optional -  Title of the chart
			colorsSet (list) : list of color in hexa to use as codes color
			shapesSet (list) : list of shapes to use as codes color
			

		return:
			dataframe
		"""
		marker = kwargs['marker'] if 'marker' in kwargs else ""
		recolor = kwargs['recolor'] if 'recolor' in kwargs else False
		color = kwargs['color'] if 'color' in kwargs else ""
		tag = kwargs['tag'] if 'tag' in kwargs else ""
		title =  kwargs['title'] if 'title' in kwargs else ""
		codeColors = kwargs['colorsSet'] if 'colorsSet' in kwargs else codes['colors']
		codeShapes = kwargs['shapesSet'] if 'shapesSet' in kwargs else codes['shapes']
	
		if recolor:	
			shuffle(codeShapes)
			shuffle(codeColors)

		qualified = kwargs['qualified'] if 'qualified' in kwargs else False
		
		data = self.getQualifiedEvents(**kwargs) if qualified else self.getEvents(**kwargs)

		data = data.astype({"@x": float, "@y": float})
		
		markers = list(dict.fromkeys(data[marker].values)) if marker else [""]
		colors = list(dict.fromkeys(data[color].values)) if color else [""]

		fig, ax = drawOptaPitch()

		for i, row in data.iterrows():
			mar = row[marker] if marker else ""
			col = row[color] if color else ""
			t = row[tag] if tag else ""

			pointMarkers = codeShapes[markers.index(mar)]
			pointColors = codeColors[colors.index(col)]


			plt.scatter(x=row['@x'], y =row['@y'], s= 100, marker= pointMarkers, color= pointColors)
			ax.text(x=row['@x'], y =row['@y'], s= t)
			
		plt.axis("off")
		plt.title(title)
		plt.show()
		
####################################################################################
########################             Start Class            ########################
########################            to work with            ########################
########################          PassMatrix Files          ########################
####################################################################################

class OptaPassMatrix(OptaFile):
	"""Class to work specifically with Opta Pass Matrix Files 

	Attributes:
		configs (dict) - dictionary with data definition and location in catalog
		match (str) - game identificator

	params:
		location (str): physical location of file
		PassMatrix (dict): optional - Dict with catalogs path that identificate the location of data in file
		RenamePlayersMaps (dict): optional - Dict with catalogs that identificates colum names and normalize 
			with other kind of files
    """

	def __init__(self, location:str, **kwargs):
		super().__init__(location, **kwargs)
		self.checkType('PassMatrix')
		self.configs = kwargs['PassMatrix'] if 'PassMatrix' in kwargs else PassMatrixDict
		self.match = self.giveMe(self.configs['game'])
		self.players = self.getPlayersDict(**kwargs)
		# self.getPlayerPassStats(fields=['@player_id', '@player_name', '@position', 'teamName'])


	def getTeamsDict(self, **kwargs):
		"""
		Get data about name and rival of each team
		
		team_id (str) : optional - catalog path where team id is located. If not setted uses default
		away_id (str) : optional - catalog path where away team id located. If not setted uses default
		away_name (str) : optional - catalog path where away team name is located. If not setted uses default
		home_id (str) : optional - catalog path where home team id is located. If not setted uses default
		home_name (str) : optional - catalog path where home name is located. If not setted uses default

		return dict
		"""
		team_id  = kwargs['team_id'] if 'team_id' in kwargs else self.configs['file Team id'] 
		away_id = kwargs['away_id'] if 'events' in kwargs else self.configs['Away ID']
		away_name = kwargs['away_name'] if 'events' in kwargs else self.configs['Away Name']
		home_id = kwargs['home_id'] if 'events' in kwargs else self.configs['Home ID']
		home_name = kwargs['home_name'] if 'events' in kwargs else self.configs['Home Name']
		
		teamsDict = {}
		team_id = self.giveMe (team_id)
		
		aid = self.giveMe (away_id)
		aname = self.giveMe (away_name)
		
		hid = self.giveMe (home_id)
		hname = self.giveMe (home_name)

		teamsDict['name'] = aname if aid == team_id else hname
		teamsDict['rival'] = hname if aid == team_id else aname
		

		return teamsDict


	def getPlayerPassStats(self, **kwargs):
		"""
		create event dataframe and return specific filtered fields and rows
		
		params:
		fields (list) : fields to return , 
		filters (dict) : filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
		exclude (dict) : filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}			player_stats (str) : optional - catalog path where player stats data is located. If not setted uses default
		returns:
			dataframe
		"""
		player_stats = kwargs['player_stats'] if 'player_stats' in kwargs else self.configs['player stats']
		originalDF =  self.giveMeDF(player_stats)


		teamsDict = self.getTeamsDict()

		originalDF['teamName'] = teamsDict['name']
		originalDF['rival'] = teamsDict['rival']


		pruned = pruningDF(originalDF, **kwargs )
		return pruned


	def getPlayerPass(self, **kwargs):
		"""
		Get data from passes including passer to receptor
		
		params:
			fields (list) : fields to return , 
			filters (dict) : filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
		return:
			df with fields requested
		"""
		player_pass = kwargs['player_pass'] if 'player_pass' in kwargs else self.configs['player pass']
		df = self.giveMeDF(player_pass, levels = 2)

		teamsDict = self.getTeamsDict()
		df['teamName'] = teamsDict['name']
		df['rival'] = teamsDict['rival']


		pruned = pruningDF(df, **kwargs)
		return pruned

	def getPassMatrix(self, **kwargs):
		"""
		Get data for make a pass matrix with coordinates
		
		params:
			fields (list) : optional fields to return , 
			removeFields (list): columns to exclude from finalDataframe
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			constants (dict) : dict as {"column to add": "constant Value"}
		return:
			Dataframe
		"""

		players = self.getPlayerPassStats(fields = ['@player_id', '@x', '@y'])
		
		fieldsLocal = ['SPP.@player_id', 'SPP.@player_name', "@position", "@x", "@y", "SP.@player_id", "SP.@player_name", "#text"]
		df = self.getPlayerPass(fields =fieldsLocal )

		finalDF = df.merge(players, how='left', 
	                                 	right_on='@player_id',
	                                 	left_on= 'SPP.@player_id',
										suffixes=('','_dest')
										)

		finalDF = finalDF.rename({'@x': '@x_origin', '@y': '@y_origin'}, axis=1)
		finalDF = finalDF.rename({'SP.@player_id': 'player_id_origin' , 'SP.@player_name':'@player_name_origin'}, axis=1)
		finalDF = finalDF.rename({'SPP.@player_id': 'player_id_dest' , 'SPP.@player_name':'@player_name_dest'}, axis=1)
		finalDF = finalDF.rename({'#text': 'passes'}, axis=1)

		teamsDict = self.getTeamsDict()
		finalDF['teamName'] = teamsDict['name']
		finalDF['rival'] = teamsDict['rival']

		pruned = pruningDF(finalDF, **kwargs )

		return pruned
	

	def plotPassMatrix(self, **kwargs):
		"""
		Show a footbaal pitch with matrix pass basing every player in average position
		
		params:
			marker (str) : column name to assign a specific marker type
			color (str) : column name to assign a specific color
			tag (str) : optional - column name to show as tag
			recolor (bool) : optoional allows to change colors settigns randomly Default False
			atLeast (int) : base number of passes to show. Default is 0
			colorsSet (list) : list of color in hexa to use as codes color
			shapesSet (list) : list of shapes to use as codes color
			title (str) : Optional -  Title of the chart
				filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}

		return:
			dataframe
		"""

		recolor = kwargs['recolor'] if 'recolor' in kwargs else False
		atLeast = kwargs['atLeast'] if 'atLeast' in kwargs else 0
		marker = kwargs['marker'] if 'marker' in kwargs else ""
		color = kwargs['color'] if 'color' in kwargs else ""
		tag = kwargs['tag'] if 'tag' in kwargs else ""
		codeColors = kwargs['colorsSet'] if 'colorsSet' in kwargs else codes['colors']
		codeShapes = kwargs['shapesSet'] if 'shapesSet' in kwargs else codes['shapes']
		title =  kwargs['title'] if 'title' in kwargs else ""
		
		if recolor:	
			shuffle(codeShapes)
			shuffle(codeColors)


		points = self.getPassMatrix()
		points = points.astype({"@x_origin": float, "@y_origin": float, "@x_dest": float, "@y_dest": float, "passes":int})

		pm = pruningDF(points, **kwargs)
		pm = pm.astype({"@x_origin": float, "@y_origin": float, "@x_dest": float, "@y_dest": float, "passes":int})


		markers = list(dict.fromkeys(points[marker].values)) if marker else [""]
		colors = list(dict.fromkeys(points[color].values)) if color else [""]

		fig, ax = drawOptaPitch()

		#ubicamos a los jugadores según la posición media
		for i, r  in points.iterrows():
			mar = r[marker] if marker else ""
			col = r[color] if color else ""
			t = r[tag] if tag else ""
			pointMarkers = codeShapes[markers.index(mar)]
			pointColors = codeColors[colors.index(col)]

			ax.text(x=r['@x_origin'], y =r['@y_origin'], s= t)
			plt.scatter(x=r['@x_origin'], y =r['@y_origin'], s= 350, marker= pointMarkers, color= pointColors)


		for i, row in pm.iterrows():
			if row['passes'] > atLeast: 
				col = r[color] if color else ""
				c = codeColors[colors.index(col)]

				plt.arrow(
						x =  row['@x_origin'],
						y =  row['@y_origin'],
						dx = (row['@x_dest'] - row['@x_origin'] ),
						dy = (row['@y_dest'] - row['@y_origin'] ),
						linewidth = row['passes'],
						alpha = row['passes'],
						length_includes_head=False,
						color= c)

		plt.axis("off")
		plt.title(title)
		plt.show()

	def getPlayersDict(self, **kwargs):
		"""
		Gets ids and names of every player in file
	
		params:
			RenamePlayersMaps (dict) : Optional - dict with original names and final names to normalize wito other kind of files
		return:
			Dataframe
		"""

		configsDict = kwargs['RenamePlayersMaps'] if 'RenamePlayersMaps' in kwargs else RenamePlayersMaps

		field = list(configsDict['PassMatrix'])

		df = self.getPlayerPassStats(fields=field)
		df.rename(columns=configsDict['PassMatrix'], inplace=True)

		return df

	def PlotRanking(self, groupBy:str, metric:str, agg:str, **kwargs):
		"""
		Make a horizontal bar chart with pass stats order desc grouping with groupBy param and aggregating a metric with agg param
		
		params:
			groupBy (str) : column name to group by the metric
			metric (str) : column name to summarize
			agg (str) : kind of aggregation. Can be sum, min, max, mean, std, count, nunique
			top (int) : number of rows to show. default 50
			qualified (bool) : when true retrieves dataframe with qualifiers else only events
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
		
		return:
			dataframe
		"""

		top = kwargs['top'] if 'top' in kwargs else 50
		data = self.getPlayerPassStats(**kwargs)

		getRanking(df=data, groupBy=groupBy, metric=metric, agg=agg, top=top )

####################################################################################
########################             Start Class            ########################
########################            to work with            ########################
########################         Match Results Files        ########################
####################################################################################

class OptaMatchResults(OptaFile):
	"""Class to work specifically with Opta Match Results Files 

	Attributes:
		configs (dict) - dictionary with data definition and location in catalog
		match (str) - game identificator
		players (DataFrame) : dataframe with look up table to players
		teams (DataFrame) : dataframe with look up table to teams

	params:
		location (str): physical location of file
		PassMatrix (dict): optional - Dict with catalogs path that identificate the location of data in file
    """

	def __init__(self, location:str, **kwargs):
		super().__init__(location, **kwargs)
		self.checkType('MatchResults')
		self.configs = kwargs['MatchResults'] if 'MatchResults' in kwargs else MatchResults
		self.match = self.giveMe(self.configs['game'])
		self.players = self.getPlayersDict()
		self.teams = self.getTeams()


	def getJudges(self, **kwargs):
		"""
		Get datafram with referee and assistants
		
		params:
			Assistants (str) : optional - catalog path where assistants data is located. If not setted uses default
			Main_FirstName (str) : optional - catalog path where main referee name is located. If not setted uses default
			Main_LastName (str) : optional - catalog path where main referee last name is located. If not setted uses default
			Main_Type (str) : optional - catalog path where referee type is located. If not setted uses default
			Main_uID (str) : optional - catalog path where referee luid is located. If not setted uses default

		return:
			df with fields requested
		"""

		Assistants = kwargs['Assistants'] if 'Assistants' in kwargs else self.configs['Assistants']
		Main_FirstName = kwargs['Main_FirstName'] if 'Main_FirstName' in kwargs else self.configs['Main FirstName']
		Main_LastName = kwargs['Main_LastName'] if 'Main_LastName' in kwargs else self.configs['Main LastName']
		Main_Type = kwargs['Main_Type'] if 'Main_Type' in kwargs else self.configs['Main Type']
		Main_uID = kwargs['Main_uID'] if 'Main_uID' in kwargs else self.configs['Main uID']


		Assistants = self.giveMeDF(Assistants, levels=4)
		
		Main = {}

		Main['@FirstName'] = self.giveMe(Main_FirstName)
		Main['@LastName'] = self.giveMe(Main_LastName)
		Main['@Type'] = self.giveMe(Main_Type)
		Main['@uID'] = self.giveMe(Main_uID)
		Main = pd.DataFrame(Main, index=[0])

		output = pd.concat([Main, Assistants], ignore_index=True)
		output["Match_id"] = self.match

		return output

	def getPlayers(self, **kwargs):
		"""
		Get data of players in match
		
		params:
			fields (list) : fields to return , 
			filters (dict) : filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			removeFields (list) : fields of column names to exclude in dataframe to return
			constants (dict) : dict as {"column to add": "constant Value"}
			playersPath (str) : optional - catalog path where players data is located. If not setted uses default

		return:
			df with fields requested
		"""

		playersPath = kwargs['Players'] if 'Players' in kwargs else self.configs['Players']

		players = self.giveMeDF(path=playersPath, levels=3)
		
		fields = kwargs['fields'] if 'fields' in kwargs  else list(players.columns.values)
		
		players["Match_id"] = self.match

		pruned = pruningDF(players, **kwargs)	
		pruned = pruned.set_index('STP.@uID')
		return pruned

	def getTeams(self, **kwargs):
		"""
		Get data of teams in match
		
		params:
			fields (list) : fields to return , 
			filters (dict) : filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			removeFields (list) : fields of column names to exclude in dataframe to return
			constants (dict) : dict as {"column to add": "constant Value"}
			playersPath (str) : optional - catalog path where players data is located. If not setted uses default

		return:
			df with fields requested
		"""
		TeamsPath = kwargs['TeamsPath'] if 'TeamsPath' in kwargs else self.configs['Teams']

		Teams = self.giveMeDF(TeamsPath, levels=3)


		Teams["Match_id"] = self.match
		pruned = pruningDF(Teams, **kwargs)	
		pruned = pruned.set_index('@uID')
		return pruned

	def getBookings(self, **kwargs):
		"""
		Get data of bookings in match
		
		params:
			fields (list) : optional fields to return , 
			removeFields (list): columns to exclude from finalDataframe
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			constants (dict) : dict as {"column to add": "constant Value"}
			Booking (str) : optional - catalog path where Booking data is located. If not setted uses default
			withDetail (bool): optional - allows to add data from players look up table
			players (DataFrame) - Allows to change the players look up table. New table should be index by STP.@uID field 


		return:
			df with fields requested
		"""
		Booking = kwargs['Booking'] if 'Booking' in kwargs else self.configs['Booking']
		withDetail = kwargs['withDetail'] if 'withDetail' in kwargs else True
		players = kwargs['players'] if 'players' in kwargs else self.players

		books = self.giveMeDF(path=Booking, levels=3)
		books["Match_id"] = self.match

		if withDetail:
			books = books.merge(players, how='left', 
	                                 	right_on= 'p'+players['PlayerID'], 
	                                 	left_on= '@PlayerRef',
										suffixes=('','_lk')
										)

		books["Match_id"] = self.match
		
		teams = list(self.teams.Name)

		books['rival'] = books['team'].apply(lambda x :  teams[0] if teams[1] == x else teams[1])

		pruned = pruningDF(books, **kwargs)	
		return pruned

	def getGoals(self, **kwargs):
		"""
		Get data of goals and assits in match
		
		params:
			fields (list) : optional fields to return , 
			removeFields (list): columns to exclude from finalDataframe
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			constants (dict) : dict as {"column to add": "constant Value"}
			goals (str) : optional - catalog path where assists and goals data is located. If not setted uses default
			withDetail (bool): optional - allows to add data from players look up table
			players (DataFrame) - Allows to change the players look up table. New table should be index by STP.@uID field 
			Name_goal (str)  - column name in goals dataframe with team name

		return:
			df with fields requested
		"""

		teamName = kwargs['Name_goal'] if 'Name_goal' in kwargs else 'team_goal'

		fieldsLocal = ["MTGA.@PlayerRef", "@EventID", "@Period", "@Min", "@Sec", "MTG.@PlayerRef"]
		goals = kwargs['Goals'] if 'Goals' in kwargs else self.configs['Goals']
		withDetail = kwargs['withDetail'] if 'withDetail' in kwargs else True
		players = kwargs['players'] if 'players' in kwargs else self.players

		Goals = self.giveMeDF(path=goals, levels=3)

		#maybe file doesn't have any goal 
		try: 
			pruned = pruningDF(Goals, fields=fieldsLocal)

			if withDetail:
				pruned = pruned.merge(players, how='left', 
		                                 	right_on= 'p'+players['PlayerID'], 
		                                 	left_on= 'MTG.@PlayerRef',
											suffixes=('','_lk')
											)

				pruned = pruned.merge(players, how='left', 
		                                 	right_on= 'p'+players['PlayerID'], 
		                                 	left_on= 'MTGA.@PlayerRef',
											suffixes=('_goal','_assists')
											)
			
			pruned["Match_id"] = self.match

			teams = list(self.teams.Name)

			pruned['rival'] = pruned[teamName].apply(lambda x :  teams[0] if teams[1] == x else teams[1])

			pruned = pruningDF(pruned, **kwargs)	
			return pruned
		except:
			pass


	def getPlayersWithStats(self, **kwargs):
		"""
		Get data stats data of players in match
		
		params:
			stats (str) : optional - catalog path where stats players data is located. If not setted uses default
			fields (list) : optional - fields to return. Default is ["@Type", "#text", "@PlayerRef", "@Position", "@ShirtNumber", "@Status", "@Captain", "@SubPosition"]
			agg (str) : optional - fields with numerical data to aggregate. default is '#text'
			withDetail (bool): optional - allows to add data from players look up table
			players (DataFrame) - Allows to change the players look up table. New table should be index by STP.@uID field 

			fields (list) : fields to return , 
			filters (dict) : filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			removeFields (list) : fields of column names to exclude in dataframe to return
			constants (dict) : dict as {"column to add": "constant Value"}

		return:
			df with fields requested
		"""
		stats = kwargs['stats'] if 'stats' in kwargs else self.configs['PlayerStats']
		
		fieldsLocal = ["@Type", "#text", "@PlayerRef", "@Position", "@ShirtNumber", "@Status", "@Captain", "@SubPosition"]
		withDetail = kwargs['withDetail'] if 'withDetail' in kwargs else True
		players = kwargs['players'] if 'players' in kwargs else self.players


		agg = '#text'
		agg = kwargs['agg'] if 'agg' in kwargs else agg


		PlayerStats = self.giveMeDF(path=stats, levels=3)
		
		pruned = pruningDF(PlayerStats, fields=fieldsLocal)


		pruned[agg].apply(pd.to_numeric, errors='ignore')
		fieldsLocal.remove(agg)
		pruned = pruned.fillna('').groupby(fieldsLocal)[agg].sum().unstack('@Type').reset_index()
		

		if withDetail:
			pruned = pruned.merge(players, how='left', 
	                                 	right_on= 'p'+players['PlayerID'], 
	                                 	left_on= '@PlayerRef',
										suffixes=('','_lk')
										)
		
		pruned["Match_id"] = self.match
		teams = list(self.teams.Name)

		pruned['rival'] = pruned['team'].apply(lambda x :  teams[0] if teams[1] == x else teams[1])

		pruned = pruningDF(pruned, **kwargs)	

		return pruned


	def getSubstitutions(self, **kwargs):
		"""
		Get data of goals and assits in match
		
		params:
			fields (list) : optional fields to return , 
			removeFields (list): columns to exclude from finalDataframe
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			constants (dict) : dict as {"column to add": "constant Value"}
			Substitution (str) : optional - catalog path where Substitutions data is located. If not setted uses default
			withDetail (bool): optional - allows to add data from players look up table
			players (DataFrame) - Allows to change the players look up table. New table should be index by STP.@uID field 

		return:
			df with fields requested
		"""
		subs = kwargs['Substitution'] if 'Substitution' in kwargs else self.configs['Substitution']
		withDetail = kwargs['withDetail'] if 'withDetail' in kwargs else True
		players = kwargs['players'] if 'players' in kwargs else self.players
		
		Substitutions = self.giveMeDF(path=subs, levels=3)
		

		if withDetail:
			Substitutions = Substitutions.merge(players, how='left', 
	                                 	right_on= 'p'+players['PlayerID'], 
	                                 	left_on= '@SubOff',
										suffixes=('','_lk')
										)

			Substitutions = Substitutions.merge(players, how='left', 
	                                 	right_on= 'p'+players['PlayerID'], 
	                                 	left_on= '@SubOn',
										suffixes=('_off','_on')
										)

		teams = list(self.teams.Name)
		Substitutions['rival'] = Substitutions['team_on'].apply(lambda x :  teams[0] if teams[1] == x else teams[1])

		Substitutions["Match_id"] = self.match
		pruned = pruningDF(Substitutions, **kwargs)	
		return pruned

	def getPlayersDict(self, **kwargs):
		"""
		Gets ids and names of every player in file
	
		params:
			RenamePlayersMaps (dict) : Optional - dict with original names and final names to normalize wito other kind of files
		return:
			Dataframe
		"""

		configsDict = kwargs['RenamePlayersMaps'] if 'RenamePlayersMaps' in kwargs else RenamePlayersMaps

		field = list(configsDict['MatchResults'])

		df = self.getPlayers(fields=field).reset_index()
		df.rename(columns=configsDict['MatchResults'], inplace=True)
		df['PlayerID'] = df['PlayerID'].apply(lambda x : x[1:] if x[0] == 'p' else x)
		df['name'] = df['name'] + ' ' + df['lastname']
		del df['lastname']

		return df


	def PlotRanking(self, groupBy:str, metric:str, agg:str, **kwargs):
		"""
		Make a horizontal bar chart with pass stats order desc grouping with groupBy param and aggregating a metric with agg param
		
		params:
			groupBy (str) : column name to group by the metric
			metric (str) : column name to summarize
			agg (str) : kind of aggregation. Can be sum, min, max, mean, std, count, nunique
			top (int) : number of rows to show. default 50
			qualified (bool) : when true retrieves dataframe with qualifiers else only events
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
		
		return:
			dataframe
		"""
		top = kwargs['top'] if 'top' in kwargs else 50
		data = self.getPlayersWithStats(**kwargs)

		getRanking(df=data, groupBy=groupBy, metric=metric, agg=agg, top=top )

####################################################################################
########################             Start Class            ########################
########################            to work with            ########################
########################         Season stats Files         ########################
####################################################################################

class OptaSeasonStats(OptaFile):
	"""Class to work specifically with Opta Season Stats Files 

	Attributes:
		configs (dict) - dictionary with data definition and location in catalog
		team (str) : Team about file have data 
		players (DataFrame) : dataframe with look up table to players


	params:
		location (str): physical location of file
		SeasonStats (dict): optional - Dict with catalogs path that identificate the location of data in file
    """

	def __init__(self, location:str, **kwargs):
		super().__init__(location, **kwargs)
		self.checkType('SeasonStats')
		self.configs = kwargs['SeasonStats'] if 'SeasonStats' in kwargs else SeasonStats
		self.team = self.giveMe(self.configs['team'])
		self.players = self.getPlayersDict()


	def getTeamStats(self, **kwargs):
		"""
		Get Season stats of team
		
		params:
		metrics (list): optional -  metrics to show in output. 
		Team_Stats (str):  optional - catalog path where teams stats data is located. If not setted uses default
		transposed (bool): default false. when true shows metrics in columns, when false shows in rows
		constants (dict) : dict as {"column to add": "constant Value"}

		return:
			df with fields requested
		"""

		metrics = kwargs['metrics'] if 'metrics' in kwargs else None
		ts = kwargs['Team_Stats'] if 'Team_Stats' in kwargs else self.configs['Team Stats']
		transposed = kwargs['transposed'] if 'transposed' in kwargs else False

		df = self.giveMeDF(ts)

		df['#text'].apply(pd.to_numeric, errors='ignore')
		df = df.set_index('@name')
		df = df.T

		metrics = metrics if metrics else list(df.columns.values)

		pruned = pruningDF(df, fields=metrics)

		if not transposed:
			pruned = pruned.T

		pruned['season'] = self.season

		pruned = pruningDF(pruned, **kwargs)		
		return pruned


	def getPlayers(self, **kwargs):
		"""
		Get data of players in team
		
		params:
			fields (list) : optional - fields to return. Default is ['@player_id','@first_name', '@last_name', '@known_name', '@Position','@shirtNumber']
			filters (dict) : filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			playersPath (str) : optional - catalog path where players data is located. If not setted uses default

		return:
			df with fields requested
		"""
		playersPath = kwargs['Players'] if 'Players' in kwargs else self.configs['Team Players']
		players = self.giveMeDF(path=playersPath, levels=1)
		
		players['season'] = self.season
		pruned = pruningDF(players, **kwargs)	
		pruned = pruned.set_index('@player_id')
		return pruned


	def getPlayersStats(self, **kwargs):
		"""
		Get stats data of players in team
		
		params:
			fields (list) : optional - fields to return. Default is all data
			filters (dict) : filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			agg (str) : optional - fields with numerical data to aggregate. default is '#text'
			PlayerStatPath (str) : optional - catalog path where players stats data is located. If not setted uses default
			metrics (list): optional -  metrics to show in output. 
		
		return:
			df with fields requested
		"""
		filters = kwargs['filters'] if 'filters' in kwargs else {}
		exclude = kwargs['exclude'] if 'exclude' in kwargs else {}
		constants = kwargs['constants'] if 'constants' in kwargs else {}		
		metrics = kwargs['metrics'] if 'metrics' in kwargs else None
		fields = kwargs['fields'] if 'fields' in kwargs else None
		PlayerStatPath = kwargs['PlayerStatPath'] if 'PlayerStatPath' in kwargs else self.configs['Team Player Stat']
		agg = kwargs['agg'] if 'agg' in kwargs else '#text'

		players = self.giveMeDF(path=PlayerStatPath, levels=3)
		fieldsMetrics = ["#text","@name", "@player_id" ]
		pruned = pruningDF(players, fields = fieldsMetrics )	
		pruned['#text'].apply(pd.to_numeric, errors='ignore')
		pruned = players.fillna('').groupby(['@player_id','@name'])['#text'].sum().unstack('@name')

		metrics = metrics if metrics else list(pruned.columns.values)
		
		pruned = pruned.merge(self.players, how='left', 
                                 	right_on= "PlayerID", 
                                 	left_index= True,
									suffixes=('','_lk')
									)

		fields = fields if fields else list(pruned.columns.values)
		fields = fields + metrics
		pruned['season'] = self.season
		pruned = pruningDF(pruned, constants=constants, filters=filters,  fields=fields, exclude=exclude, metrics=metrics, )	
		return pruned

	def getPlayersDict(self, **kwargs):
		"""
		Gets ids and names of every player in file
	
		params:
			RenamePlayersMaps (dict) : Optional - dict with original names and final names to normalize wito other kind of files
		return:
			Dataframe
		"""
		

		# self.getPlayers(fields=['@player_id', '@first_name', '@last_name', '@known_name', '@position'],  
		# 	constants={'team' : self.team})

		configsDict = kwargs['RenamePlayersMaps'] if 'RenamePlayersMaps' in kwargs else RenamePlayersMaps

		field = list(configsDict['SeasonStats'])

		df = self.getPlayers(fields=field).reset_index()
		df.rename(columns=configsDict['SeasonStats'], inplace=True)
		
		df['PlayerID'] = df['PlayerID'].apply(lambda x : x[1:] if x[0] == 'p' else x)
		df['name'] = df['name'] + ' ' + df['lastname']
		del df['lastname']
		df['team'] = self.team

		return df

	def PlotRanking(self, groupBy:str, metric:str, agg:str, **kwargs):
		"""
		Make a horizontal bar chart with pass stats order desc grouping with groupBy param and aggregating a metric with agg param
		
		params:
			groupBy (str) : column name to group by the metric
			metric (str) : column name to summarize
			agg (str) : kind of aggregation. Can be sum, min, max, mean, std, count, nunique
			top (int) : number of rows to show. default 50
			qualified (bool) : when true retrieves dataframe with qualifiers else only events
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
		
		return:
			dataframe
		"""
		top = kwargs['top'] if 'top' in kwargs else 50
		data = self.getPlayersStats(**kwargs)

		getRanking(df=data, groupBy=groupBy, metric=metric, agg=agg, top=top )

####################################################################################
########################             Start Class            ########################
########################            to work with            ########################
########################           Standings Files          ########################
####################################################################################

class OptaStandings(OptaFile):
	"""Class to work specifically with Opta Season Standings Files 

	Attributes:
		configs (dict) - dictionary with data definition and location in catalog
		Teams (DataFrame) : dataframe with look up table to Teams


	params:
		location (str): physical location of file
		Standings (dict): optional - Dict with catalogs path that identificate the location of data in file
    """

	def __init__(self, location:str, **kwargs):
		super().__init__(location, **kwargs)
		self.checkType('Standings')
		self.configs = kwargs['Standings'] if 'Standings' in kwargs else Standings
		self.teams = self.getTeams()

	def getTeams(self, **kwargs):
		"""
		Get names and id of teams in file
		
		params:
			fields (list) : optional - fields to return. Default is ["@uID", "Name"]
			filters (dict) : filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			TeamsPath (str) : optional - catalog path where players data is located. If not setted uses default

		return:
			df with fields requested
		"""
		filters = kwargs['filters'] if 'filters' in kwargs else {}
		exclude = kwargs['exclude'] if 'exclude' in kwargs else {}		

		fields = ["@uID", "Name"]
		fields = kwargs['fields'] if 'fields' in kwargs else fields
		TeamsPath = kwargs['TeamsPath'] if 'TeamsPath' in kwargs else self.configs['Teams']

		Teams = self.giveMeDF(TeamsPath, levels=1)

		pruned = pruningDF(Teams, **kwargs)	
		pruned = pruned.set_index('@uID')
		return pruned


	def getStandings(self, **kwargs):
		"""
		Get Standings of every team
		
		params:
			fields (list) : optional - fields to return. Default is ["@uID", "Name"]
			filters (dict) : filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			StandingsPath (str) : optional - catalog path where standings data is located. If not setted uses default
			QualificationPath (str) : optional - catalog path where qualification data is located. If not setted uses default

		return:
			df with fields requested
		"""
		fields = kwargs['fields'] if 'fields' in kwargs else ["@TeamRef", "@qualify", "Name"]
		requestedFields = True if 'fields' in kwargs else False

		StandingsPath = kwargs['StandingsPath'] if 'StandingsPath' in kwargs else self.configs['Standings']
		QualificationPath = kwargs['QualificationPath'] if 'QualificationPath' in kwargs else self.configs['Qualification']


		Standings = self.giveMeDF(StandingsPath, levels=2)
		metrics = list(Standings.columns.values)

		Qualifications = self.giveMeDF(QualificationPath, levels=2)
		Qualifications['@team_id'] = 't' + Qualifications['@team_id']
		
		df = Standings.merge(Qualifications, how='left', 
                                 	right_on= '@team_id', 
                                 	left_on= '@TeamRef',
									suffixes=('','_lk')
									)

		df = df.merge(self.teams, how='left', 
                                 	right_index= True,
                                 	left_on= '@TeamRef',
									suffixes=('','_lk')
									)

		fields = fields if requestedFields else fields + metrics
		df['season'] = self.season
		pruned = pruningDF	(df, **kwargs)	

		return pruned

	def PlotRanking(self, groupBy:str, metric:str, agg:str, **kwargs):
		"""
		Make a horizontal bar chart with pass stats order desc grouping with groupBy param and aggregating a metric with agg param
		
		params:
			groupBy (str) : column name to group by the metric
			metric (str) : column name to summarize
			agg (str) : kind of aggregation. Can be sum, min, max, mean, std, count, nunique
			top (int) : number of rows to show. default 50
			qualified (bool) : when true retrieves dataframe with qualifiers else only events
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
		
		return:
			dataframe
		"""
		top = kwargs['top'] if 'top' in kwargs else 50
		data = self.getStandings(**kwargs)

		getRanking(df=data, groupBy=groupBy, metric=metric, agg=agg, top=top )

####################################################################################
########################             Start Class            ########################
########################            to work with            ########################
########################            Squads Files            ########################
####################################################################################

class OptaSquads(OptaFile):
	"""Class to work specifically with Opta Season Squads Files 

	Attributes:
		configs (dict) - dictionary with data definition and location in catalog


	params:
		location (str): physical location of file
		Squads (dict): optional - Dict with catalogs path that identificate the location of data in file
    """

	def __init__(self, location:str, **kwargs):
		super().__init__(location, **kwargs)
		self.checkType('Squads')
		self.configs = kwargs['Squads'] if 'Squads' in kwargs else Squads
		self.players = self.getPlayersDict()


	def getTeamsAndStadiums(self, **kwargs):
		"""
		Get stats of coaches in every team

		params:
			fields (list) : optional - fields to return.
			filters (dict) : filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			StadiumsPath (str) : optional - catalog path where officials data is located. If not setted uses default

		return:
			df with fields requested
		"""
		StadiumsPath = kwargs['StadiumsPath'] if 'StadiumsPath' in kwargs else self.configs['Stadiums']
		stadiums = self.giveMeDF(StadiumsPath, levels=2)

		pruned = pruningDF(stadiums, **kwargs)	

		return pruned


	def getOfficials(self, **kwargs):
		"""
		Get stats of coaches in every team

		params:
			fields (list) : optional - fields to return.
			filters (dict) : filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			officialsPath (str) : optional - catalog path where officials data is located. If not setted uses default

		return:
			df with fields requested
		"""
		officialsPath = kwargs['officialsPath'] if 'officialsPath' in kwargs else self.configs['officials']
		Officials = self.giveMeDF(officialsPath, levels=3)
		
		pruned = pruningDF(Officials, **kwargs)	

		return pruned



	def getPlayersStats(self, **kwargs):
		"""
		Get stats of players in every team
		
		params:
			fields (list) : optional - fields to return.
			filters (dict) : filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			playersPathStat (str) : optional - catalog path where players data is located. If not setted uses default

		return:
			df with fields requested
		"""
		playersPathStat = kwargs['playersPathStat'] if 'playersPathStat' in kwargs else self.configs['playersStat']

		Stats = self.giveMeDF(playersPathStat, levels=1)
		
		Stats = Stats.pivot_table(index=["SoccerDocument.Team.Player.index"], columns=['@Type'],
                 values='#text', aggfunc='first', fill_value=0)

		fields = kwargs['fields'] if 'fields' in kwargs else list(Stats.columns.values)
		pruned = pruningDF(Stats, **kwargs)	

		return pruned


	def getPlayers(self, **kwargs):
		"""
		Get stats of players changed in in every team
		
		params:
			fields (list) : optional - fields to return.
			filters (dict) : filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			playersPath (str) : optional - catalog path where players data is located. If not setted uses default
			withStat (bool) - when true add to Dataframe stats data from  getPlayersStats function
			withTeam (bool) - when true add to Dataframe team data from getTeamsAndStadiums function, default false

		return:
			df with fields requested
		"""
		playersPath = kwargs['playersPath'] if 'playersPath' in kwargs else self.configs['players']
		withStat = kwargs['withStat'] if 'withStat' in kwargs else True
		withTeam = kwargs['withTeam'] if 'withTeam' in kwargs else True

		players = self.giveMeDF(playersPath, levels=2)

		if withStat:
			stats = self.getPlayersStats()
			players = players.merge(stats, how='left', 
                             	right_index= True, 
                             	left_index= True,
								suffixes=('','_stat')
								) 


		if withTeam:
			teams = self.getTeamsAndStadiums()
			players = players.merge(teams, how='left', 
                             	right_index= True, 
                             	left_on= 'SoccerDocument.Team.index',
								suffixes=('','_team')
								) 


		pruned = pruningDF(players, **kwargs)

		return pruned


	def getPlayersChangesStats(self, **kwargs):
		"""
		Get stats of players changed in every team
		
		params:
			fields (list) : optional - fields to return.
			filters (dict) : filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			playersChangesStatsPath (str) : optional - catalog path where players data is located. If not setted uses default

		return:
			df with fields requested
		"""
		filters = kwargs['filters'] if 'filters' in kwargs else {}
		exclude = kwargs['exclude'] if 'exclude' in kwargs else {}		
		playersChangesStats = kwargs['playersChangesStatsPath'] if 'playersChangesStatsPath' in kwargs else self.configs['playersChangesStats']

		Stats = self.giveMeDF(playersChangesStats, levels=1)
		
		Stats = Stats.pivot_table(index=["PlayerChanges.Team.Player.index"], columns=['@Type'],
                 values='#text', aggfunc='first', fill_value=0)

		fields = kwargs['fields'] if 'fields' in kwargs else list(Stats.columns.values)
		pruned = pruningDF(Stats, fields = fields,filters=filters, exclude=exclude )

		return pruned


	def getPlayersChanges(self, **kwargs):
		"""
		Get stats of players changed in in every team
		
		params:
			fields (list) : optional - fields to return.
			filters (dict) : filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			playersChangesPath (str) : optional - catalog path where players data is located. If not setted uses default
			withStat (bool) - when true add to Dataframe stats data from  getPlayersChangesStats function
			withTeam (bool) - when true add to Dataframe team data from getTeamsAndStadiums function, default false

		return:
			df with fields requested
		"""
		filters = kwargs['filters'] if 'filters' in kwargs else {}
		exclude = kwargs['exclude'] if 'exclude' in kwargs else {}		
		playersChanges = kwargs['playersChangesPath'] if 'playersChangesPath' in kwargs else self.configs['playersChanges']
		withStat = kwargs['withStat'] if 'withStat' in kwargs else True
		withTeam = kwargs['withTeam'] if 'withTeam' in kwargs else True

		playerCh = self.giveMeDF(playersChanges, levels=2)

		if withStat:
			stats = self.getPlayersChangesStats()
			playerCh = playerCh.merge(stats, how='left', 
                             	right_index= True, 
                             	left_index= True,
								suffixes=('','_lk')
								) 

		if withTeam:
			teams = self.getTeamsAndStadiums()
			playerCh = playerCh.merge(teams, how='left', 
                             	right_index= True, 
                             	left_on= 'PlayerChanges.Team.index',
								suffixes=('','_team')
								) 

		fields = kwargs['fields'] if 'fields' in kwargs else list(playerCh.columns.values)
		pruned = pruningDF(playerCh, fields = fields,filters=filters, exclude=exclude )

		return pruned


	
	def getOfficialsChanges(self, **kwargs):
		"""
		Get data of changes coaches teams in every team
		
		params:
			fields (list) : optional - fields to return. Default is ['STP.@uID','First', 'Last', 'Known', '@Position','Name']
			filters (dict) : filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			officialsChanges (str) : optional - catalog path where officials changes data is located. If not setted uses default

		return:
			df with fields requested
		"""
		filters = kwargs['filters'] if 'filters' in kwargs else {}
		exclude = kwargs['exclude'] if 'exclude' in kwargs else {}		
		fields = ["First", "Last", "join_date", "leave_date", "@Type", "@country", "PT.@uID", "PTT.@uID", "Name" ]
		fields = kwargs['fields'] if 'fields' in kwargs else fields
		officialsChanges = kwargs['officialsChanges'] if 'officialsChanges' in kwargs else self.configs['officialsChanges']

		officialsTeam = self.giveMeDF(officialsChanges, levels=3)
		pruned = pruningDF(officialsTeam, fields = fields,filters=filters, exclude=exclude )

		return pruned


	def getPlayersDict(self, **kwargs):
		"""
		Gets ids and names of every player in file
	
		params:
			RenamePlayersMaps (dict) : Optional - dict with original names and final names to normalize wito other kind of files
		return:
			Dataframe
		"""

		configsDict = kwargs['RenamePlayersMaps'] if 'RenamePlayersMaps' in kwargs else RenamePlayersMaps

		field = list(configsDict['Squads'])

		df = self.getPlayers(fields=field)

		df.rename(columns=configsDict['Squads'], inplace=True)
		df['PlayerID'] = df['PlayerID'].apply(lambda x : x[1:] if x[0] == 'p' else x)

		return df

####################################################################################
########################             Start Class            ########################
########################            to work with            ########################
########################            Results Files           ########################
####################################################################################

class OptaResults(OptaFile):
	"""Class to work specifically with Opta Results Files 

	Attributes:
		configs (dict) - dictionary with data definition and location in catalog


	params:
		location (str): physical location of file
		results (dict): optional - Dict with catalogs path that identificate the location of data in file
    """

	def __init__(self, location:str, **kwargs):
		super().__init__(location, **kwargs)
		self.checkType('results')
		self.configs = kwargs['results'] if 'results' in kwargs else Results


	def getTeams(self, **kwargs):
		"""
		Get names and id of teams in file
		
		params:
			fields (list) : optional - fields to return. Default is ["@uID", "Name"]
			filters (dict) : filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			Teams (str) : optional - catalog path where players data is located. If not setted uses default

		return:
			df with fields requested
		"""

		TeamsPath = kwargs['Teams'] if 'Teams' in kwargs else self.configs['Teams']

		Teams = self.giveMeDF(TeamsPath, levels=1)

		pruned = pruningDF(Teams, **kwargs)	
		pruned = pruned.set_index('@uID')
		return pruned

	def getTeamsInMatch(self, **kwargs):
		"""
		Get names from home and away teams
		
		params:
			fields (list) : optional - fields to return. Default is ["@uID", "Name"]
			filters (dict) : filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			TeamsPath (str) : optional - catalog path where players data is located. If not setted uses default

		return:
			df with fields requested
		"""
		TeamData = self.getTeams(fields=["@uID", "Name"])

		TeamsPath = kwargs['TeamsPath'] if 'TeamsPath' in kwargs else self.configs['TeamsPath']
		Teams = self.giveMeDF(TeamsPath, levels=1)
		Teams = Teams[['@Side','@TeamRef', 'SoccerDocument.MatchData.index']]

		Teams = Teams.pivot_table(index=["SoccerDocument.MatchData.index"], columns=['@Side'],
		         values='@TeamRef', aggfunc='first', fill_value=0)

		Teams = Teams.merge(TeamData, how='left', 
	                                 	right_index=True,
	                                	left_on='Away'
										,suffixes=('','away')
										)
		Teams.rename(columns={'Name': 'Name_Away'}, inplace=True)
		Teams = Teams.merge(TeamData, how='left', 
	                                 	right_index=True,
	                                	left_on='Home'
										,suffixes=('','_Home')
										)
		Teams.rename(columns={'Name': 'Name_Home'}, inplace=True)
		return Teams


	def getMatchData(self, **kwargs):
		"""
		Get info of every match
		
		params:
			fields (list) : optional - fields to return. Default is ["@uID", "Name"]
			filters (dict) : filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			Match_info (str) : optional - catalog path where match data is located. If not setted uses default

		return:
			df with fields requested
		"""
		match = kwargs['Match_info'] if 'Match_info' in kwargs else self.configs['Match Info']

		Match = self.giveMeDF(match, levels=3)
		
		TinM = self.getTeamsInMatch()

		Match = Match.merge(TinM, how='left', 
	                                 	right_index=True,
	                                	left_index=True
										,suffixes=('','_Teams')
										)
		Match['winnerName'] = Match.apply(lambda x : x['Name_Away'] if x['Away'] == x['@MatchWinner'] else x['Name_Home'] if x['Home'] == x['@MatchWinner'] else "Tied", axis=1)
		pruned = pruningDF(Match, **kwargs)	
		return pruned


	def getGoals(self, **kwargs):
		"""
		Get goals in every match
		
		params:
			fields (list) : optional - fields to return. Default is ["@uID", "Name"]
			filters (dict) : filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			Goals (str) : optional - catalog path where match data is located. If not setted uses default

		return:
			df with fields requested
		"""

		GoalsPath = kwargs['Goals'] if 'Goals' in kwargs else self.configs['Goals']

		#maybe file doesn't have any goal 
		try: 
			Goals = self.giveMeDF(GoalsPath, levels=3)

			TinM = self.getTeamsInMatch()

			Goals = Goals.merge(TinM, how='left', 
		                                 	right_index=True,
		                                	left_on='SoccerDocument.MatchData.index'
											,suffixes=('','_Teams')
											)

			Goals['owner'] = Goals.apply(lambda x : x['Name_Away'] if x ['@Side'] == 'Away' else x['Name_Home'] , axis=1)
			Goals['rival'] = Goals.apply(lambda x : x['Name_Home'] if x ['@Side'] == 'Away' else x['Name_Away'] , axis=1)

			pruned = pruningDF(Goals, **kwargs)	
			return pruned
		except:
			pass

	def getOfficials(self, **kwargs):
		"""
		Get officials in every macth

		params:
			fields (list) : optional - fields to return.
			filters (dict) : filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			Officials (str) : optional - catalog path where officials data is located. If not setted uses default

		return:
			df with fields requested
		"""

		OfficialPath = kwargs['Officials'] if 'Officials' in kwargs else self.configs['Officials']

		Officials = self.giveMeDF(OfficialPath, levels=3)


		TinM = self.getTeamsInMatch()

		Officials = Officials.merge(TinM, how='left', 
	                                 	right_index=True,
	                                	left_on="SoccerDocument.MatchData.MatchOfficials.index"
										,suffixes=('','_Teams')
										)

		pruned = pruningDF(Officials, **kwargs)	
		return pruned

####################################################################################
########################             Start Class            ########################
########################            to work with            ########################
########################              F28 Files             ########################
####################################################################################

class OptaF28(OptaFile):
	"""Class to work specifically with Opta F28 Files 

	Attributes:
		configs (dict) - dictionary with data definition and location in catalog


	params:
		location (str): physical location of file
		F28 (dict): optional - Dict with catalogs path that identificate the location of data in file
    """

	def __init__(self, location:str, **kwargs):
		super().__init__(location, **kwargs)
		self.checkType('F28')
		self.configs = kwargs['F28'] if 'F28' in kwargs else F28


	def getTeamsDict(self, **kwargs):
		"""
		Get data about name and rival of each team
		
		away_id (str) : optional - catalog path where away team id located. If not setted uses default
		away_name (str) : optional - catalog path where away team name is located. If not setted uses default
		home_id (str) : optional - catalog path where home team id is located. If not setted uses default
		home_name (str) : optional - catalog path where home name is located. If not setted uses default

		return dict
		"""
		away_id = kwargs['away_id'] if 'events' in kwargs else self.configs['Away ID']
		away_name = kwargs['away_name'] if 'events' in kwargs else self.configs['Away Name']
		home_id = kwargs['home_id'] if 'events' in kwargs else self.configs['Home ID']
		home_name = kwargs['home_name'] if 'events' in kwargs else self.configs['Home Name']
		
		teamsDict = {}
		aid = self.giveMe (away_id)
		aname = self.giveMe (away_name)
		hid = self.giveMe (home_id)
		hname = self.giveMe (home_name)

		teamsDict['Home'] = {}
		teamsDict['Away'] = {}

		teamsDict['Home']['id'] = hid
		teamsDict['Home']['name'] = hname
		teamsDict['Away']['id'] = aid
		teamsDict['Away']['name'] = aname

		return teamsDict



	def getIntervals(self, **kwargs):
		"""
		Get Possession Waves in intervals

		params:
			fields (list) : optional - fields to return.
			filters (dict) : filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			Intervals (str) : optional - catalog path where intervals data is located. If not setted uses default

		return:
			df with fields requested
		"""
		filters = kwargs['filters'] if 'filters' in kwargs else {}
		exclude = kwargs['exclude'] if 'exclude' in kwargs else {}
		fields = kwargs['fields'] if 'fields' in kwargs else None
		removeFields = kwargs['removeFields'] if 'removeFields' in kwargs else None

		Intervals = kwargs['Intervals'] if 'Intervals' in kwargs else self.configs['Intervals']

		IntervalsPD = self.giveMeDF(Intervals, levels=4)

		teamsDict = self.getTeamsDict()

		IntervalsPD['HomeTeam'] = teamsDict['Home']['name']
		IntervalsPD['AwayTeam'] = teamsDict['Away']['name']

		pruned = pruningDF(IntervalsPD, fields = fields, filters=filters, exclude=exclude, removeFields=removeFields)	

		return pruned


	def getLastX(self, **kwargs):
		"""
		Get Possession Waves by LastX

		params:
			fields (list) : optional - fields to return.
			filters (dict) : filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			LastX (str) : optional - catalog path where LastX data is located. If not setted uses default

		return:
			df with fields requested
		"""
		filters = kwargs['filters'] if 'filters' in kwargs else {}
		exclude = kwargs['exclude'] if 'exclude' in kwargs else {}
		fields = kwargs['fields'] if 'fields' in kwargs else None
		removeFields = kwargs['removeFields'] if 'removeFields' in kwargs else None

		LastX = kwargs['LastX'] if 'LastX' in kwargs else self.configs['LastX']

		LastXPD = self.giveMeDF(LastX, levels=3)


		teamsDict = self.getTeamsDict()
		
		LastXPD['HomeTeam'] = teamsDict['Home']['name']
		LastXPD['AwayTeam'] = teamsDict['Away']['name']

		pruned = pruningDF(LastXPD, fields = fields, filters=filters, exclude=exclude, removeFields=removeFields)	

		return pruned


	def getOverall(self, **kwargs):
		"""
		Get Possession Waves by LastX

		params:
			fields (list) : optional - fields to return.
			filters (dict) : filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			LastX (str) : optional - catalog path where LastX data is located. If not setted uses default

		return:
			df with fields requested
		"""
		filters = kwargs['filters'] if 'filters' in kwargs else {}
		exclude = kwargs['exclude'] if 'exclude' in kwargs else {}
		fields = kwargs['fields'] if 'fields' in kwargs else None
		removeFields = kwargs['removeFields'] if 'removeFields' in kwargs else None

		Overall = kwargs['Overall'] if 'Overall' in kwargs else self.configs['Overall']

		OverallPD = self.giveMeDF(Overall, levels=3)

		teamsDict = self.getTeamsDict()
		OverallPD['HomeTeam'] = teamsDict['Home']['name']
		OverallPD['AwayTeam'] = teamsDict['Away']['name']


		
		pruned = pruningDF(OverallPD, fields = fields, filters=filters, exclude=exclude, removeFields=removeFields)

		return pruned

	def GetWaves(self, wave):
		"""
		Return concatenated waves

		params:
			wave (str) : Filter type of wave

		return:
			df with fields requested
		"""

		overall = self.getOverall(filters = {'@Type':wave})
		del overall['@Type']
		overall['@Type'] = '0-90'
		overall['PPII.@Type'] = '90'
		overall['@Origin'] = 'Overall'

		lastX = self.getLastX(filters = {'PP.@Type':wave})
		del lastX['PP.@Type']
		lastX.rename(columns={'PPLL.@Type': '@Type'}, inplace=True)
		lastX['@Origin'] = 'lastX'
		
		intervals = self.getIntervals(filters = {'PP.@Type':wave})
		del intervals['PP.@Type']
		intervals.rename(columns={'PPIII.@Type': '@Type'}, inplace=True)
		intervals['@Origin'] = 'intervals'

		concatenated = pd.concat([overall,lastX,intervals], ignore_index=True)

		return concatenated


	def getBallPossession(self, **kwargs):
		"""
		Get BallPossession Waves

		params:
			fields (list) : optional - fields to return.
			fields (list) : optional fields to return , 
			removeFields (list): columns to exclude from finalDataframe
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			constants (dict) : dict as {"column to add": "constant Value"}
		return:
			df with fields requested
		"""
		concatenated = self.GetWaves('BallPossession')

		pruned = pruningDF(concatenated, **kwargs)	

		return pruned


	def getTerritorial(self, **kwargs):
		"""
		Get Territorial Waves

		params:
			fields (list) : optional fields to return , 
			removeFields (list): columns to exclude from finalDataframe
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			constants (dict) : dict as {"column to add": "constant Value"}
		return:
			df with fields requested
		"""
		concatenated = self.GetWaves('Territorial')
		pruned = pruningDF(concatenated, **kwargs)	

		return pruned


	def getTerritorialThird(self, **kwargs):
		"""
		Get TerritorialThird Waves

		params:
			fields (list) : optional fields to return , 
			removeFields (list): columns to exclude from finalDataframe
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			constants (dict) : dict as {"column to add": "constant Value"}
		return:
			df with fields requested
		"""
		concatenated = self.GetWaves('TerritorialThird')
		pruned = pruningDF(concatenated, **kwargs)	

		return pruned


	def PlotPossession(self, posType="BallPossession", periods="90", origin="intervals", horizontal=False):
		"""
		Return stacked barchart with possession waves

		params:
			posType (str) : Default BallPossession - Type of possession to plot (BallPossession, Territorial, TerritorialThird)
			periods (str) : Default 90 -  cluster of minutes to aggregate the data (5, 15, 45, 90)
			origin (str) : Default intervals - allows to choose between data in intervals (including overall) and LastX
			horizontal (bool) : Default False - allos to choose between horizontal (True) or vertical (False)

		return
			stacked barchart plot
		"""

		kind = "barh" if horizontal else "bar"
		fields = ['Away', "Middle", "Home", "@Type"] if posType == 'TerritorialThird' else ['Away', "Home", "@Type"]

		concatenated = self.GetWaves(posType)
		filters = {"PPII.@Type": periods} if origin == 'intervals' else {"@Origin": origin}

		pruned = pruningDF(concatenated, filters=filters)	

		pruned = pruned.astype({"Away": float, "Home": float, "Middle": float})

		
		pruned = pruned[fields]
		pruned = pruned.set_index('@Type')
		

		pruned.plot(kind=kind, stacked=True, legend=True)
		plt.title("{} % possession in {}".format(posType, origin))
		plt.show()

		return pruned

####################################################################################
########################             Start Class            ########################
########################            to work with            ########################
########################               Teams                ########################
####################################################################################
class Teams():
	"""
	Allows to interact with teams data in every kind of opta file.
	
	Params:
		team (str): Team about data is
		files (dict) : optional - filesTypes and location of every file with data of the team
		Catalog (object) : optional - instance of OptaCatalog class with direftory of files 
		location (str) : optional - folder with optafiles where data of team is located


	Attributes:
		team (str) : Team Name


	"""
	def __init__(self, team:str, **kwargs):
		self.team = team
		self.TeamsCatalog = self.mapFiles(**kwargs)


	def mapFiles(self, **kwargs):
		"""
		locate the files about team. When instantiate with files uses this dict
		when instantitate with catalog filter the file team attribute of this class to get the team files
		wheren instatiate with locations, make the catalog and filter the team attribute
		"""
		files = kwargs['files'] if 'files' in kwargs else None
		Catalog = kwargs['Catalog'] if 'Catalog' in kwargs else None
		location = kwargs['location'] if 'location' in kwargs else None

		if files:
			return files
		
		elif Catalog:
			return Catalog.OptaTeams[self.team]

		elif location:
			catalog = OptaCatalog(location)
			team = catalog.OptaTeams[self.team]
			return team

		else:
			raise Exception('files, Catalog or location should be passed at init class')


	def getEvents(self, owner="own", **kwargs):
		"""
		Get events of team in all files asociated to team

		params:
			owner (str) = own -> show commited eventes. rival-> show event suffered
			fields (list) : optional fields to return , 
			removeFields (list): columns to exclude from finalDataframe
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			constants (dict) : dict as {"column to add": "constant Value"}


		return:
			pandas dataframe
		
		"""		
		if not 'F24' in self.TeamsCatalog:
			return "No Opta F24 files was found in catalog"

		obj = 'teamName' if owner =='own' else 'rival'

		filtersk = createNewKey(path=kwargs, newKey='filters', datatype=dict())
		filtersk[obj] = self.team

		dfs = []
		for i in self.TeamsCatalog['F24']:
			df = OptaF24(i)
			dfs.append(df.getEvents(**kwargs))

		return pd.concat(dfs)


	def getQualifiedsEvents(self, owner="own", **kwargs):
		"""
		Get events with qualifiers of team in all files asociated to team

		params:
			owner (str) = own -> show commited eventes. rival-> show event suffered
			fields (list) : optional fields to return , 
			removeFields (list): columns to exclude from finalDataframe
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			constants (dict) : dict as {"column to add": "constant Value"}


		return:
			pandas dataframe
		
		"""
		if not 'F24' in self.TeamsCatalog:
			return "No Opta F24 files was found in catalog"

		obj = 'teamName' if owner =='own' else 'rival'

		filtersk = createNewKey(path=kwargs, newKey='filters', datatype=dict())
		filtersk[obj] = self.team

		dfs = []
		for i in self.TeamsCatalog['F24']:
			df = OptaF24(i)
			dfs.append(df.getQualifiedEvents(**kwargs))

		return pd.concat(dfs)


	def PlotPitchEvents(self, **kwargs):
		"""
		Show a footbaal pitch with events location
		
		params:
			agg (str) : kind of aggregation. Can be sum, min, max, mean, std, count, nunique
			qualified (bool) : when true retrieves dataframe with qualifiers else only events
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			marker (str) : optional - column name to set as marker type
			color (str) : optional - column name to set as color
			tag (str) : optional - column name to show as tag
			recolor (bool) : optoional allows to change colors settigns randomly
			title (str) : Optional -  Title of the chart
			colorsSet (list) : list of color in hexa to use as codes color
			shapesSet (list) : list of shapes to use as codes color
			

		return:
			dataframe
		"""
		if not 'F24' in self.TeamsCatalog:
			return "No Opta F24 files was found in catalog"

		marker = kwargs['marker'] if 'marker' in kwargs else ""
		recolor = kwargs['recolor'] if 'recolor' in kwargs else False
		color = kwargs['color'] if 'color' in kwargs else ""
		tag = kwargs['tag'] if 'tag' in kwargs else ""
		title =  kwargs['title'] if 'title' in kwargs else ""
		codeColors = kwargs['colorsSet'] if 'colorsSet' in kwargs else codes['colors']
		codeShapes = kwargs['shapesSet'] if 'shapesSet' in kwargs else codes['shapes']
	
		if recolor:	
			shuffle(codeShapes)
			shuffle(codeColors)

		qualified = kwargs['qualified'] if 'qualified' in kwargs else False
		
		data = self.getQualifiedsEvents(**kwargs) if qualified else self.getEvents(**kwargs)

		data = data.astype({"@x": float, "@y": float})
		
		markers = list(dict.fromkeys(data[marker].values)) if marker else [""]
		colors = list(dict.fromkeys(data[color].values)) if color else [""]

		fig, ax = drawOptaPitch()

		for i, row in data.iterrows():
			mar = row[marker] if marker else ""
			col = row[color] if color else ""
			t = row[tag] if tag else ""

			pointMarkers = codeShapes[markers.index(mar)]
			pointColors = codeColors[colors.index(col)]


			plt.scatter(x=row['@x'], y =row['@y'], s= 100, marker= pointMarkers, color= pointColors)
			ax.text(x=row['@x'], y =row['@y'], s= t)
			
		plt.axis("off")
		plt.title(title)
		plt.show()


	def PlotRankingEvents(self, groupBy:str, metric:str, agg:str, **kwargs):
		"""
		Make a horizontal bar chart with event data order desc grouping with groupBy param and aggregating a metric with agg param
		
		params:
			groupBy (str) : column name to group by the metric
			metric (str) : column name to summarize
			agg (str) : kind of aggregation. Can be sum, min, max, mean, std, count, nunique
			top (int) : number of rows to show. default 50
			qualified (bool) : when true retrieves dataframe with qualifiers else only events
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
		
		return:
			dataframe
		"""
		top = kwargs['top'] if 'top' in kwargs else 50
		if not 'F24' in self.TeamsCatalog:
			return "No Opta F24 files was found in catalog"

		qualified = kwargs['qualified'] if 'qualified' in kwargs else False
		data = self.getQualifiedEvents(**kwargs) if qualified else self.getEvents(**kwargs)

		getRanking(df=data, groupBy=groupBy, metric=metric, agg=agg, top=top )


	def getPossession(self, origin:str, posType:str, **kwargs):
		"""
		Get Possession waves from Opta F28

		params:
			origin (str) = own -> possession group by. Possible Values: Overall, lastX, intervals
			posType (str) = Type of possession. Possible Values: BallPossession, Territorial, TerritorialThird

			fields (list) : optional fields to return , 
			removeFields (list): columns to exclude from finalDataframe
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			constants (dict) : dict as {"column to add": "constant Value"}

		return:
			pandas dataframe
		
		"""		
		if not 'F28' in self.TeamsCatalog:
			return "No Opta F28 files was found in catalog"

		filtersk = createNewKey(path=kwargs, newKey='filters', datatype=dict())
		filtersk['@Origin'] = origin

		removek = createNewKey(path=kwargs, newKey='removeFields', datatype=list())
		removes = ['Intervals', 'Overall', 'Possession.PossessionWave.Intervals.IntervalLength.index', 'LastX','Possession.PossessionWave.index', 'Interval', 'IntervalLength',  'Possession.PossessionWave.Intervals.index', 'Possession.PossessionWave.LastX.index']
		removek = removek.extend(removes) 

		dfs = []
		for i in self.TeamsCatalog['F28']:
			ob = OptaF28(i)

			if posType == 'BallPossession':
				df = ob.getBallPossession(**kwargs)
			elif posType == 'Territorial':
				df = ob.getTerritorial(**kwargs)
			elif posType == 'TerritorialThird':
				df = ob.getTerritorialThird(**kwargs)
		
			df['team'] = df.apply(lambda x : float(x['Away']) if x["AwayTeam"] == self.team else float(x['Home']),axis=1)
			df['rival'] =  df.apply(lambda x : float(x['Home']) if x["AwayTeam"] == self.team else float(x['Away']), axis=1)
			df = df.astype({'Middle':'float'})
			dfs.append(df)

		return pd.concat(dfs)


	def PlotAvgPossession(self, posType="BallPossession", periods="45", origin="intervals", horizontal=False):
		"""
		Return stacked barchart with possession waves

		params:
			origin (str) = own -> possession group by. Possible Values: Overall, lastX, intervals
			posType (str) = Type of possession. Possible Values: BallPossession, Territorial, TerritorialThird
			periods (str) : Default 5 -  cluster of minutes to aggregate the data (5, 15, 45) only for intervals origin
			origin (str) : Default intervals - allows to choose between data in intervals (including overall) and LastX
			horizontal (bool) : Default False - allos to choose between horizontal (True) or vertical (False)

		return
			stacked barchart plot
		"""
		if not 'F28' in self.TeamsCatalog:
			return "No Opta F28 files was found in catalog"

		if posType == "TerritorialThird" and origin=="lastX":
			return "TerritorialThird can't be called for lastX origin"
		if periods == "90" and origin=="intervals":
			return "Intervals can't be called with period 90. Try origin Overal or periodos 5, 15 or 45"


		periods = '90' if origin == 'Overall' else periods


		kind = "barh" if horizontal else "bar"
		fields = ['team', "Middle", "rival", "@Type"] if posType == 'TerritorialThird' else ['team', "rival", "@Type"]

		concatenated = self.getPossession(posType=posType, origin=origin)

		filters = {"PPII.@Type": periods} if origin == 'intervals' else {"@Origin": origin}

		pruned = pruningDF(concatenated, filters=filters)	

		pruned = pruned.fillna('').groupby(["@Type", "PPII.@Type", "@Origin"], as_index=False).mean()

		pruned = pruned[fields]
		pruned = pruned.set_index('@Type')
		

		pruned.plot(kind=kind, stacked=True, legend=True)
		plt.title("{} % possession in {}".format(posType, origin))
		plt.show()


	def getMatchResultsPlayerStats(self, owner="own", **kwargs):
		"""
		Get players stats in match results opta files

		params:
			owner (str) = own -> show commited eventes. rival-> show event suffered
			fields (list) : optional fields to return , 
			removeFields (list): columns to exclude from finalDataframe
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			constants (dict) : dict as {"column to add": "constant Value"}

	
		return:
			pandas dataframe
		
		"""		
		if not 'MatchResults' in self.TeamsCatalog:
			return "No Opta MatchResults files was found in catalog"

		obj = 'team' if owner =='own' else 'rival'
		filtersk = createNewKey(path=kwargs, newKey='filters', datatype=dict())
		filtersk[obj] = self.team


		dfs = []
		for i in self.TeamsCatalog['MatchResults']:
			ob = OptaMatchResults(i)
			df = ob.getPlayersWithStats(**kwargs)

			dfs.append(df)

		return pd.concat(dfs)

	def PlotMatchResultsRanking(self, groupBy:str, metric:str, agg:str, **kwargs):
		"""
		Make a horizontal bar chart with event data order desc grouping with groupBy param and aggregating a metric with agg param
		
		params:
			groupBy (str) : column name to group by the metric
			metric (str) : column name to summarize
			agg (str) : kind of aggregation. Can be sum, min, max, mean, std, count, nunique
			top (int) : number of rows to show default 50
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
		
		return:
			dataframe
		"""
		if not 'MatchResults' in self.TeamsCatalog:
			return "No Opta MatchResults files was found in catalog"

		top = kwargs['top'] if 'top' in kwargs else 50

		qualified = kwargs['qualified'] if 'qualified' in kwargs else False
		data = self.getMatchResultsPlayerStats(**kwargs)

		getRanking(df=data, groupBy=groupBy, metric=metric, agg=agg, top=top )


	def getMatchResultsBookings(self, owner="own", **kwargs):
		"""
		Get bookings in match results opta files

		params:
			owner (str) = own -> show commited eventes. rival-> show event suffered
			fields (list) : optional fields to return , 
			removeFields (list): columns to exclude from finalDataframe
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			constants (dict) : dict as {"column to add": "constant Value"}	
		return:
			pandas dataframe		
		"""		
		if not 'MatchResults' in self.TeamsCatalog:
			return "No Opta MatchResults files was found in catalog"

		removek = createNewKey(path=kwargs, newKey='removeFields', datatype=list())
		removes = ['Substitution', 'Stat', 'PlayerLineUp', 'Goal', 'Booking']
		removek = removek.extend(removes) 


		obj = 'team' if owner =='own' else 'rival'
		filtersk = createNewKey(path=kwargs, newKey='filters', datatype=dict())
		filtersk[obj] = self.team

		dfs = []
		for i in self.TeamsCatalog['MatchResults']:
			ob = OptaMatchResults(i)
			df = ob.getBookings(**kwargs)

			dfs.append(df)

		return pd.concat(dfs)


	def getMatchResultsGoal(self, owner="own", **kwargs):
		"""
		Get goals in match results opta files

		params:
			owner (str) = own -> show commited eventes. rival-> show event suffered
			fields (list) : optional fields to return , 
			removeFields (list): columns to exclude from finalDataframe
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			constants (dict) : dict as {"column to add": "constant Value"}	

		return:
			pandas dataframe		
		"""
		if not 'MatchResults' in self.TeamsCatalog:
			return "No Opta MatchResults files was found in catalog"

		obj = 'team_goal' if owner =='own' else 'rival'
		filtersk = createNewKey(path=kwargs, newKey='filters', datatype=dict())
		filtersk[obj] = self.team

		dfs = []
		for i in self.TeamsCatalog['MatchResults']:
			ob = OptaMatchResults(i)
			df = ob.getGoals(**kwargs)
			dfs.append(df)

		return pd.concat(dfs)


	def getMatchResultsSubstitutions(self, owner="own", **kwargs):
		"""
		Get substitutions in match results opta files

		params:
			owner (str) = own -> show commited eventes. rival-> show event suffered
			fields (list) : optional fields to return , 
			removeFields (list): columns to exclude from finalDataframe
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			constants (dict) : dict as {"column to add": "constant Value"}	
		return:
			pandas dataframe		
		"""		
		if not 'MatchResults' in self.TeamsCatalog:
			return "No Opta MatchResults files was found in catalog"

		obj = 'team_on' if owner =='own' else 'rival'
		filtersk = createNewKey(path=kwargs, newKey='filters', datatype=dict())
		filtersk[obj] = self.team

		dfs = []
		for i in self.TeamsCatalog['MatchResults']:
			ob = OptaMatchResults(i)
			df = ob.getSubstitutions(**kwargs)
			dfs.append(df)

		return pd.concat(dfs)


	def getPassStats(self, owner="own", **kwargs):
		"""
		Get pass stats of player by game

		params:
			owner (str) = own -> show commited pass. rival-> show pass suffered
			fields (list) : optional fields to return , 
			removeFields (list): columns to exclude from finalDataframe
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			constants (dict) : dict as {"column to add": "constant Value"}	
		return:
			pandas dataframe		
		"""		
		if not 'PassMatrix' in self.TeamsCatalog:
			return "No Opta PassMatrix files was found in catalog"

		obj = 'teamName' if owner =='own' else 'rival'
		filtersk = createNewKey(path=kwargs, newKey='filters', datatype=dict())
		filtersk[obj] = self.team


		dfs = []
		for i in self.TeamsCatalog['PassMatrix']:
			ob = OptaPassMatrix(i)
			df = ob.getPlayerPassStats(**kwargs)
			dfs.append(df)

		return pd.concat(dfs)


	def PlotPassStats(self, groupBy:str, metric:str, agg:str, **kwargs):
		"""
		Make a horizontal bar chart with event data order desc grouping with groupBy param and aggregating a metric with agg param
		
		params:
			groupBy (str) : column name to group by the metric
			metric (str) : column name to summarize
			agg (str) : kind of aggregation. Can be sum, min, max, mean, std, count, nunique
			top (int) : number of rows to show default 50
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
		
		return:
			dataframe
		"""
		if not 'PassMatrix' in self.TeamsCatalog:
			return "No Opta PassMatrix files was found in catalog"
		top = kwargs['top'] if 'top' in kwargs else 50

		qualified = kwargs['qualified'] if 'qualified' in kwargs else False
		data = self.getPassStats(**kwargs)

		getRanking(df=data, groupBy=groupBy, metric=metric, agg=agg, top=top )


	def getPassReceptors(self, owner="own", **kwargs):
		"""
		Get pass stats with receptors

		params:
			owner (str) = own -> show commited eventes. rival-> show event suffered
			fields (list) : optional fields to return , 
			removeFields (list): columns to exclude from finalDataframe
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			constants (dict) : dict as {"column to add": "constant Value"}	
	
		return:
			pandas dataframe		
		"""		
		if not 'PassMatrix' in self.TeamsCatalog:
			return "No Opta PassMatrix files was found in catalog"

		obj = 'teamName' if owner =='own' else 'rival'
		filtersk = createNewKey(path=kwargs, newKey='filters', datatype=dict())
		filtersk[obj] = self.team

		dfs = []
		for i in self.TeamsCatalog['PassMatrix']:
			ob = OptaPassMatrix(i)
			df = ob.getPlayerPass(**kwargs)
			dfs.append(df)

		return pd.concat(dfs)

	def getPassMatrixData(self, owner="own", **kwargs):
		"""
		Get data for make the pass matrix with position in pitch

		params:
			owner (str) = own -> show commited eventes. rival-> show event suffered
			fields (list) : optional fields to return , 
			removeFields (list): columns to exclude from finalDataframe
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			constants (dict) : dict as {"column to add": "constant Value"}	
	
		return:
			pandas dataframe		
		"""		
		if not 'PassMatrix' in self.TeamsCatalog:
			return "No Opta PassMatrix files was found in catalog"

		obj = 'teamName' if owner =='own' else 'rival'
		filtersk = createNewKey(path=kwargs, newKey='filters', datatype=dict())
		filtersk[obj] = self.team

		dfs = []
		for i in self.TeamsCatalog['PassMatrix']:
			ob = OptaPassMatrix(i)
			df = ob.getPassMatrix(**kwargs)
			dfs.append(df)

		return pd.concat(dfs)


	def getPassMatrix(self, **kwargs):
		"""
		Get the pass matrix

		params:
			owner (str) = own -> show commited eventes. rival-> show event suffered
			fields (list) : optional fields to return , 
			removeFields (list): columns to exclude from finalDataframe
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			constants (dict) : dict as {"column to add": "constant Value"}	

			agg (str) : kind of aggregation. Can be sum, min, max, mean, std, count, nunique. Defautl mean


		return:
			pandas dataframe		
		"""	
		if not 'PassMatrix' in self.TeamsCatalog:
			return "No Opta PassMatrix files was found in catalog"

		agg = kwargs['agg'] if 'agg' in kwargs else 'mean'

		if not 'PassMatrix' in self.TeamsCatalog:
			return "No Opta PassMatrix files was found in catalog"

		df = self.getPassMatrixData(owner="own", **kwargs)
		df = df.astype({'passes':'float'})

		df = df.pivot_table(index=["@player_name_dest"], columns=['@player_name_origin'],
             values='passes', aggfunc=agg, fill_value=0)

		return df


	def getTeamSeasonStats(self, **kwargs):
		"""
		Get data of seasons Stats 

		params:
		metrics (list): optional -  metrics to show in output. 
		Team_Stats (str):  optional - catalog path where teams stats data is located. If not setted uses default
		transposed (bool): default false. when true shows metrics in columns, when false shows in rows
		constants (dict) : dict as {"column to add": "constant Value"}

	
		return:
			pandas dataframe		
		"""		
		if not 'SeasonStats' in self.TeamsCatalog:
			return "No Opta SeasonStats files was found in catalog"

		metrics = kwargs['fields'] if 'fields' in kwargs else None

		dfs = []
		for i in self.TeamsCatalog['SeasonStats']:
			ob = OptaSeasonStats(i)
			if self.team != ob.team:
				continue
			df = ob.getTeamStats(**kwargs)
			dfs.append(df)

		try:
			return pd.concat(dfs)
		except:
			return "no Opta Season Stats files found abot {}".format(self.team)


	def getTeamPlayersSeasonStats(self, **kwargs):
		"""
		Get data of seasons Stats ofr every player in team

		params:
		metrics (list): optional -  metrics to show in output. 
		Team_Stats (str):  optional - catalog path where teams stats data is located. If not setted uses default
		transposed (bool): default false. when true shows metrics in columns, when false shows in rows
		constants (dict) : dict as {"column to add": "constant Value"}

	
		return:
			pandas dataframe		
		"""		
		if not 'SeasonStats' in self.TeamsCatalog:
			return "No Opta SeasonStats files was found in catalog"

		metrics = kwargs['fields'] if 'fields' in kwargs else None

		dfs = []
		for i in self.TeamsCatalog['SeasonStats']:
			ob = OptaSeasonStats(i)
			if self.team != ob.team:
				continue
			df = ob.getPlayersStats(**kwargs)
			dfs.append(df)

		return pd.concat(dfs)


	def PlotTeamPlayersRank(self, groupBy:str, metric:str, agg:str, **kwargs):
		"""
		Make a horizontal bar chart with event data order desc grouping with groupBy param and aggregating a metric with agg param
		
		params:
			groupBy (str) : column name to group by the metric
			metric (str) : column name to summarize
			agg (str) : kind of aggregation. Can be sum, min, max, mean, std, count, nunique
			top (int) : number of rows to show default 50
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
		
		return:
			dataframe
		"""
		if not 'SeasonStats' in self.TeamsCatalog:
			return "No Opta SeasonStats files was found in catalog"
		top = kwargs['top'] if 'top' in kwargs else 50

		qualified = kwargs['qualified'] if 'qualified' in kwargs else False
		data = self.getTeamPlayersSeasonStats(**kwargs)

		getRanking(df=data, groupBy=groupBy, metric=metric, agg=agg, top=top )



	def getStandings(self, **kwargs):
		"""
		Get standing of team in a season

		params:
			fields (list) : optional fields to return , 
			removeFields (list): columns to exclude from finalDataframe
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			constants (dict) : dict as {"column to add": "constant Value"}
	
		return:
			pandas dataframe		
		"""		
		if not 'Standings' in self.TeamsCatalog:
			return "No Opta Standings files was found in catalog"

		filtersk = createNewKey(path=kwargs, newKey='filters', datatype=dict())
		filtersk['Name'] = self.team


		dfs = []
		for i in self.TeamsCatalog['Standings']:
			ob = OptaStandings(i)
			df = ob.getStandings(**kwargs)
			dfs.append(df)

		return pd.concat(dfs)


	def getTeamData(self, **kwargs):
		"""
		Get data of team in a season

		params:
			fields (list) : optional fields to return , 
			removeFields (list): columns to exclude from finalDataframe
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			constants (dict) : dict as {"column to add": "constant Value"}
	
		return:
			pandas dataframe		
		"""		
		if not 'Squads' in self.TeamsCatalog:
			return "No Opta Squads files was found in catalog"

		filtersk = createNewKey(path=kwargs, newKey='filters', datatype=dict())
		filtersk['@short_club_name'] = self.team

		dfs = []
		for i in self.TeamsCatalog['Squads']:
			ob = OptaSquads(i)
			df = ob.getTeamsAndStadiums(**kwargs)
			dfs.append(df)

		return pd.concat(dfs)

	def getTeamOfficials(self, **kwargs):
		"""
		Get data of officials team in a season

		params:
			fields (list) : optional fields to return , 
			removeFields (list): columns to exclude from finalDataframe
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			constants (dict) : dict as {"column to add": "constant Value"}

		return:
			pandas dataframe		
		"""		
		if not 'Squads' in self.TeamsCatalog:
			return "No Opta Squads files was found in catalog"

		filtersk = createNewKey(path=kwargs, newKey='filters', datatype=dict())
		filtersk['@short_club_name'] = self.team

		dfs = []
		for i in self.TeamsCatalog['Squads']:
			ob = OptaSquads(i)
			df = ob.getOfficials(**kwargs)
			dfs.append(df)

		return pd.concat(dfs)


	def getTeamOfficialsChanges(self, **kwargs):
		"""
		Get data of officials team in a season

		params:
			fields (list) : optional fields to return , 
			removeFields (list): columns to exclude from finalDataframe
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			constants (dict) : dict as {"column to add": "constant Value"}	

		return:
			pandas dataframe		
		"""	
		if not 'Squads' in self.TeamsCatalog:
			return "No Opta Squads files was found in catalog"

		filtersk = createNewKey(path=kwargs, newKey='filters', datatype=dict())
		filtersk['Name'] = self.team

		dfs = []
		for i in self.TeamsCatalog['Squads']:
			ob = OptaSquads(i)
			df = ob.getOfficialsChanges(**kwargs)
			dfs.append(df)

		return pd.concat(dfs)


	def getTeamPlayers(self, **kwargs):
		"""
		Get data of players team in a season

		params:
			fields (list) : optional fields to return , 
			removeFields (list): columns to exclude from finalDataframe
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			constants (dict) : dict as {"column to add": "constant Value"}
	
		return:
			pandas dataframe		
		"""	
		if not 'Squads' in self.TeamsCatalog:
			return "No Opta Squads files was found in catalog"

		filtersk = createNewKey(path=kwargs, newKey='filters', datatype=dict())
		filtersk['@short_club_name'] = self.team

		dfs = []
		for i in self.TeamsCatalog['Squads']:
			ob = OptaSquads(i)
			df = ob.getPlayers(withStat= True, withTeam=True, **kwargs)
			dfs.append(df)

		return pd.concat(dfs)


	def getTeamPlayersChanges(self, **kwargs):
		"""
		Get data of players changes in team in a season

		params:
			fields (list) : optional fields to return , 
			removeFields (list): columns to exclude from finalDataframe
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			constants (dict) : dict as {"column to add": "constant Value"}
	
		return:
			pandas dataframe		
		"""		
		if not 'Squads' in self.TeamsCatalog:
			return "No Opta Squads files was found in catalog"

		filtersk = createNewKey(path=kwargs, newKey='filters', datatype=dict())
		filtersk['@short_club_name'] = self.team

		dfs = []
		for i in self.TeamsCatalog['Squads']:
			ob = OptaSquads(i)
			df = ob.getPlayersChanges(withStat= True, withTeam=True, **kwargs)
			dfs.append(df)

		return pd.concat(dfs)


	def getMatchsData(self, **kwargs):
		"""
		Get data of teams matchs in season

		params:
			fields (list) : optional fields to return , 
			removeFields (list): columns to exclude from finalDataframe
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			constants (dict) : dict as {"column to add": "constant Value"}
	
		return:
			pandas dataframe		
		"""
		if not 'results' in self.TeamsCatalog:
			return "No Opta results files was found in catalog"

		dfs = []
		for i in self.TeamsCatalog['results']:
			ob = OptaResults(i)
			df = ob.getMatchData(**kwargs)
			dfs.append(df)

		finalDF = pd.concat(dfs)
		finalDF = finalDF[(finalDF['Name_Away'] ==self.team) | (finalDF['Name_Home'] ==self.team)]

		return finalDF


	def getMatchsGoals(self, owner='own', **kwargs):
		"""
		Get data of all goals in matchs in season

		params:
			fields (list) : optional fields to return , 
			removeFields (list): columns to exclude from finalDataframe
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			constants (dict) : dict as {"column to add": "constant Value"}
	
		return:
			pandas dataframe		
		"""	
		if not 'results' in self.TeamsCatalog:
			return "No Opta results files was found in catalog"


		obj = 'owner' if owner =='own' else 'rival'
		filtersk = createNewKey(path=kwargs, newKey='filters', datatype=dict())
		filtersk[obj] = self.team			

		dfs = []
		for i in self.TeamsCatalog['results']:
			ob = OptaResults(i)
			df = ob.getGoals(**kwargs)
			dfs.append(df)

		return pd.concat(dfs)


	def getMatchsOfficials(self, **kwargs):
		"""
		Get data of referees in matchs of the team in season

		params:
			fields (list) : optional fields to return , 
			removeFields (list): columns to exclude from finalDataframe
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			constants (dict) : dict as {"column to add": "constant Value"}
	
		return:
			pandas dataframe		
		"""		
		if not 'results' in self.TeamsCatalog:
			return "No Opta results files was found in catalog"

		filters = kwargs['filters'] if 'filters' in kwargs else {}
		exclude = kwargs['exclude'] if 'exclude' in kwargs else {}
		fields = kwargs['fields'] if 'fields' in kwargs else None
		removeFields = kwargs['removeFields'] if 'removeFields' in kwargs else None
		constants = kwargs['constants'] if 'constants' in kwargs else {}

		dfs = []
		for i in self.TeamsCatalog['results']:
			ob = OptaResults(i)
			df = ob.getOfficials(
					filters=filters, exclude=exclude, 
					fields=fields, removeFields=removeFields, 
					constants=constants)
			dfs.append(df)

		finalDF = pd.concat(dfs)
		finalDF = finalDF[(finalDF['Name_Away'] ==self.team) | (finalDF['Name_Home'] ==self.team)]

		return finalDF

####################################################################################
########################             Start Class            ########################
########################            to work with            ########################
########################               Players              ########################
####################################################################################

class Players():
	"""
	Allows to interact with players data in every kind of opta file.
	
	Params:
		Player (str): Team about data is
		files (dict) : optional - filesTypes and location of every file with data of the team
		Catalog (object) : optional - instance of OptaCatalog class with direftory of files 
		location (str) : optional - folder with optafiles where data of team is located
	Attributes:
		team (str) : Team Name


	"""

	def __init__(self, player:str, **kwargs):
		self.player = str(player)
		self.PlayerCatalog = self.mapFiles(**kwargs)

	def mapFiles(self, **kwargs):
		"""
		locate the files about team. When instantiate with files uses this dict
		when instantitate with catalog filter the file team attribute of this class to get the team files
		wheren instatiate with locations, make the catalog and filter the team attribute
		"""
		files = kwargs['files'] if 'files' in kwargs else None
		Catalog = kwargs['Catalog'] if 'Catalog' in kwargs else None
		location = kwargs['location'] if 'location' in kwargs else None

		if files:
			return files
		
		elif Catalog:
			return Catalog.OptaPlayersID[self.player]

		elif location:
			catalog = OptaCatalog(location)
			player = catalog.OptaPlayersID[str(self.player)]
			return player

		else:
			raise Exception('files, Catalog or location should be passed at init class')


	def getEvents(self, **kwargs):
		"""
		Get events of team in all files asociated to team

		params:
			fields (list) : optional fields to return , 
			removeFields (list): columns to exclude from finalDataframe
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			constants (dict) : dict as {"column to add": "constant Value"}


		return:
			pandas dataframe
		
		"""
		if not 'F24' in self.PlayerCatalog:
			return "No Opta F24 files was found in catalog"

		filtersk = createNewKey(path=kwargs, newKey='filters', datatype=dict())
		kwargs['filters']['@player_id'] = self.player

		dfs = []
		for i in self.PlayerCatalog['F24']:
			df = OptaF24(i)
			dfs.append(df.getEvents(**kwargs))


		return pd.concat(dfs)



	def getQualifiedsEvents(self, **kwargs):
		"""
		Get events of team in all files asociated to team

		params:
			fields (list) : optional fields to return , 
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}


		return:
			pandas dataframe
		
		"""
		if not 'F24' in self.PlayerCatalog:
			return "No Opta F24 files was found in catalog"

		filtersk = createNewKey(path=kwargs, newKey='filters', datatype=dict())
		kwargs['filters']['@player_id'] = self.player

		dfs = []
		for i in self.PlayerCatalog['F24']:
			df = OptaF24(i)
			dfs.append(df.getQualifiedEvents(**kwargs))

		return pd.concat(dfs)



	def PlotPitchEvents(self, **kwargs):
		"""
		Show a footbaal pitch with events location
		
		params:
			agg (str) : kind of aggregation. Can be sum, min, max, mean, std, count, nunique
			qualified (bool) : when true retrieves dataframe with qualifiers else only events
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			marker (str) : optional - column name to set as marker type
			color (str) : optional - column name to set as color
			tag (str) : optional - column name to show as tag
			recolor (bool) : optoional allows to change colors settigns randomly
			title (str) : Optional -  Title of the chart
			colorsSet (list) : list of color in hexa to use as codes color
			shapesSet (list) : list of shapes to use as codes color
			

		return:
			dataframe
		"""
		if not 'F24' in self.PlayerCatalog:
			return "No Opta F24 files was found in catalog"

		marker = kwargs['marker'] if 'marker' in kwargs else ""
		recolor = kwargs['recolor'] if 'recolor' in kwargs else False
		color = kwargs['color'] if 'color' in kwargs else ""
		tag = kwargs['tag'] if 'tag' in kwargs else ""
		title =  kwargs['title'] if 'title' in kwargs else ""
		codeColors = kwargs['colorsSet'] if 'colorsSet' in kwargs else codes['colors']
		codeShapes = kwargs['shapesSet'] if 'shapesSet' in kwargs else codes['shapes']
	
		if recolor:	
			shuffle(codeShapes)
			shuffle(codeColors)

		qualified = kwargs['qualified'] if 'qualified' in kwargs else False
		
		data = self.getQualifiedsEvents(**kwargs) if qualified else self.getEvents(**kwargs)

		data = data.astype({"@x": float, "@y": float})
		
		markers = list(dict.fromkeys(data[marker].values)) if marker else [""]
		colors = list(dict.fromkeys(data[color].values)) if color else [""]

		fig, ax = drawOptaPitch()

		for i, row in data.iterrows():
			mar = row[marker] if marker else ""
			col = row[color] if color else ""
			t = row[tag] if tag else ""

			pointMarkers = codeShapes[markers.index(mar)]
			pointColors = codeColors[colors.index(col)]


			plt.scatter(x=row['@x'], y =row['@y'], s= 100, marker= pointMarkers, color= pointColors)
			ax.text(x=row['@x'], y =row['@y'], s= t)
			
		plt.axis("off")
		plt.title(title)
		plt.show()


	def PlotRankingEvents(self, groupBy:str, metric:str, agg:str, **kwargs):
		"""
		Make a horizontal bar chart with event data order desc grouping with groupBy param and aggregating a metric with agg param
		
		params:
			groupBy (str) : column name to group by the metric
			metric (str) : column name to summarize
			agg (str) : kind of aggregation. Can be sum, min, max, mean, std, count, nunique
			top (int) : number of rows to show default 50
			qualified (bool) : when true retrieves dataframe with qualifiers else only events
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
		
		return:
			dataframe
		"""
		top = kwargs['top'] if 'top' in kwargs else 50
		if not 'F24' in self.TeamsCatalog:
			return "No Opta F24 files was found in catalog"

		qualified = kwargs['qualified'] if 'qualified' in kwargs else False
		data = self.getQualifiedEvents(**kwargs) if qualified else self.getEvents(**kwargs)

		getRanking(df=data, groupBy=groupBy, metric=metric, agg=agg, top=top )



	def getMatchResultsPlayerStats(self, **kwargs):
		"""
		Get players stats in match results opta files

		params:
			owner (str) = own -> show commited eventes. rival-> show event suffered
			fields (list) : optional fields to return , 
			removeFields (list): columns to exclude from finalDataframe
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
	
		return:
			pandas dataframe
		
		"""		
		if not 'MatchResults' in self.PlayerCatalog:
			return "No Opta MatchResults files was found in catalog"

		filtersk = createNewKey(path=kwargs, newKey='filters', datatype=dict())
		kwargs['filters']['PlayerID'] = self.player

		dfs = []
		for i in self.PlayerCatalog['MatchResults']:
			ob = OptaMatchResults(i)
			df = ob.getPlayersWithStats(**kwargs)

			dfs.append(df)

		return pd.concat(dfs)


	def getMatchResultsBookings(self, **kwargs):
		"""
		Get bookings in match results opta files

		params:
			fields (list) : optional fields to return , 
			removeFields (list): columns to exclude from finalDataframe
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			constants (dict) : dict as {"column to add": "constant Value"}	
		return:
			pandas dataframe		
		"""		
		if not 'MatchResults' in self.PlayerCatalog:
			return "No Opta MatchResults files was found in catalog"

		removek = createNewKey(path=kwargs, newKey='removeFields', datatype=list())
		removes = ['Substitution', 'Stat', 'PlayerLineUp', 'Goal', 'Booking']
		removek = removek.extend(removes) 


		filtersk = createNewKey(path=kwargs, newKey='filters', datatype=dict())
		filtersk['PlayerID'] = self.player

		dfs = []
		for i in self.PlayerCatalog['MatchResults']:
			ob = OptaMatchResults(i)
			df = ob.getBookings(**kwargs)

			dfs.append(df)

		return pd.concat(dfs)


	def getMatchResultsGoal(self, **kwargs):
		"""
		Get goals in match results opta files

		params:
			fields (list) : optional fields to return , 
			removeFields (list): columns to exclude from finalDataframe
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			constants (dict) : dict as {"column to add": "constant Value"}	

		return:
			pandas dataframe		
		"""
		if not 'MatchResults' in self.PlayerCatalog:
			return "No Opta MatchResults files was found in catalog"

		filtersk = createNewKey(path=kwargs, newKey='filters', datatype=dict())
		filtersk['PlayerID_goal'] = str(self.player)

		dfs = []
		for i in self.PlayerCatalog['MatchResults']:
			ob = OptaMatchResults(i)
			df = ob.getGoals(**kwargs)
			dfs.append(df)

		return pd.concat(dfs)

	def getMatchResultsAssits(self, **kwargs):
		"""
		Get goals in match results opta files

		params:
			fields (list) : optional fields to return , 
			removeFields (list): columns to exclude from finalDataframe
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			constants (dict) : dict as {"column to add": "constant Value"}	

		return:
			pandas dataframe		
		"""
		if not 'MatchResults' in self.PlayerCatalog:
			return "No Opta MatchResults files was found in catalog"

		filtersk = createNewKey(path=kwargs, newKey='filters', datatype=dict())
		filtersk['PlayerID_assists'] = str(self.player)

		dfs = []
		for i in self.PlayerCatalog['MatchResults']:
			ob = OptaMatchResults(i)
			df = ob.getGoals(**kwargs)
			dfs.append(df)

		return pd.concat(dfs)

	def getMatchResultsSubstitutions(self, onOff="on", **kwargs):
		"""
		Get substitutions in match results opta files

		params:
			onOff (str) = on -> player enter the game. off player out off the game
			fields (list) : optional fields to return , 
			removeFields (list): columns to exclude from finalDataframe
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			constants (dict) : dict as {"column to add": "constant Value"}	
		return:
			pandas dataframe		
		"""		
		if not 'MatchResults' in self.PlayerCatalog:
			return "No Opta MatchResults files was found in catalog"

		obj = 'PlayerID_on' if onOff =='on' else 'PlayerID_off'
		filtersk = createNewKey(path=kwargs, newKey='filters', datatype=dict())
		filtersk[obj] = self.player

		dfs = []
		for i in self.PlayerCatalog['MatchResults']:
			ob = OptaMatchResults(i)
			df = ob.getSubstitutions(**kwargs)
			dfs.append(df)

		return pd.concat(dfs)

	def getPassStats(self, **kwargs):
		"""
		Get pass stats of player by game

		params:
			fields (list) : optional fields to return , 
			removeFields (list): columns to exclude from finalDataframe
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			constants (dict) : dict as {"column to add": "constant Value"}	
		return:
			pandas dataframe		
		"""		
		if not 'PassMatrix' in self.PlayerCatalog:
			return "No Opta PassMatrix files was found in catalog"

		filtersk = createNewKey(path=kwargs, newKey='filters', datatype=dict())
		filtersk['@player_id'] = self.player

		dfs = []
		for i in self.PlayerCatalog['PassMatrix']:
			ob = OptaPassMatrix(i)
			df = ob.getPlayerPassStats(**kwargs)
			dfs.append(df)

		return pd.concat(dfs)


	def getPassReceptors(self, owner=True, **kwargs):
		"""
		Get pass stats with receptors

		params:
			owner (str) = own -> show commited eventes. rival-> show event suffered
			fields (list) : optional fields to return , 
			removeFields (list): columns to exclude from finalDataframe
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			constants (dict) : dict as {"column to add": "constant Value"}	
	
		return:
			pandas dataframe		
		"""		
		if not 'PassMatrix' in self.PlayerCatalog:
			return "No Opta PassMatrix files was found in catalog"

		obj = 'SPP.@player_id' if owner else 'SP.@player_id'
		filtersk = createNewKey(path=kwargs, newKey='filters', datatype=dict())
		filtersk[obj] = self.player


		dfs = []
		for i in self.PlayerCatalog['PassMatrix']:
			ob = OptaPassMatrix(i)
			df = ob.getPlayerPass(**kwargs)
			dfs.append(df)

		return pd.concat(dfs)

	def getPassMatrixData(self, owner="own", **kwargs):
		"""
		Get data for make the pass matrix with position in pitch

		params:
			owner (str) = own -> show passes, teammates -> show receptions - default own
			fields (list) : optional fields to return , 
			removeFields (list): columns to exclude from finalDataframe
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			constants (dict) : dict as {"column to add": "constant Value"}	
	
		return:
			pandas dataframe		
		"""		
		if not 'PassMatrix' in self.PlayerCatalog:
			return "No Opta PassMatrix files was found in catalog"

		obj = 'player_id_origin' if owner =='own' else 'player_id_dest'
		filtersk = createNewKey(path=kwargs, newKey='filters', datatype=dict())
		filtersk[obj] = self.player

		dfs = []
		for i in self.PlayerCatalog['PassMatrix']:
			ob = OptaPassMatrix(i)
			df = ob.getPassMatrix(**kwargs)
			dfs.append(df)

		return pd.concat(dfs)


	def PlotBestTeammates(self, agg:str, **kwargs):
		"""
		Make a horizontal bar chart with event data order desc grouping with groupBy param and aggregating a metric with agg param
		
		params:
			agg (str) : kind of aggregation. Can be sum, min, max, mean, std, count, nunique
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			top (int) : number of rows to show default 50
		
		return:
			dataframe
		"""
		top = kwargs['top'] if 'top' in kwargs else 50
		if not 'PassMatrix' in self.PlayerCatalog:
			return "No Opta PassMatrix files was found in catalog"

		data = self.getPassMatrixData(**kwargs)

		getRanking(df=data, groupBy='@player_name_dest', metric='passes', agg=agg, top=top )



	def getSeasonStats(self, **kwargs):
		"""
		Get data from season stats opta file

		params:
		metrics (list): optional -  metrics to show in output. 
		transposed (bool): default false. when true shows metrics in columns, when false shows in rows
		constants (dict) : dict as {"column to add": "constant Value"}

	
		return:
			pandas dataframe		
		"""		
		if not 'SeasonStats' in self.PlayerCatalog:
			return "No Opta SeasonStats files was found in catalog"

		filtersk = createNewKey(path=kwargs, newKey='filters', datatype=dict())
		filtersk['PlayerID'] = str(self.player)


		metrics = kwargs['fields'] if 'fields' in kwargs else None

		dfs = []
		for i in self.PlayerCatalog['SeasonStats']:
			ob = OptaSeasonStats(i)
			df = ob.getPlayersStats(**kwargs)
			dfs.append(df)

		return pd.concat(dfs)



	def getPlayersData(self, **kwargs):
		"""
		Get data of players from opta Squads File

		params:
			fields (list) : optional fields to return , 
			removeFields (list): columns to exclude from finalDataframe
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			constants (dict) : dict as {"column to add": "constant Value"}
	
		return:
			pandas dataframe		
		"""		

		if not 'Squads' in self.PlayerCatalog:
			return "No Opta Squads files was found in catalog"

		filtersk = createNewKey(path=kwargs, newKey='filters', datatype=dict())
		filtersk['STP.@uID'] = 'p'+self.player

		fields = ["STP.@uID", "STP.Name", "Position", "@loan", "@country", "@short_club_name", 
		"birth_date", "birth_place", "country", "first_name", "first_nationality", "height", 
		"jersey_num", "join_date", "known_name", "last_name", "middle_name", "on_loan_from", 
		"preferred_foot", "real_position", "real_position_side", "weight"]

		fieldsk = createNewKey(path=kwargs, newKey='fields', datatype=list())
		kwargs['fields'] = kwargs['fields'] if len(kwargs['fields']) > 0 else fields

		dfs = []
		for i in self.PlayerCatalog['Squads']:
			ob = OptaSquads(i)
			df = ob.getPlayers(withStat= True, withTeam=True, **kwargs)
			dfs.append(df)

		return pd.concat(dfs)

	def getPlayersChanges(self, **kwargs):
		"""
		Get data of player if have been changed in season

		params:
			fields (list) : optional fields to return , 
			removeFields (list): columns to exclude from finalDataframe
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			constants (dict) : dict as {"column to add": "constant Value"}
	
		return:
			pandas dataframe		
		"""		
		if not 'Squads' in self.PlayerCatalog:
			return "No Opta Squads files was found in catalog"

		filtersk = createNewKey(path=kwargs, newKey='filters', datatype=dict())
		filtersk['PTP.@uID'] = 'p'+self.player

		fields = ['PTP.@uID', 'first_name', 'PTP.Name','middle_name', 'last_name','known_name',
		'Position', 'real_position', 'real_position_side', 'height', 'weight', 'preferred_foot',
		'jersey_num', '@loan', 'PT.Name', 'join_date', 'leave_date', 'new_team', 'birth_date', 
		'birth_place', 'country', 'first_nationality']


		fieldsk = createNewKey(path=kwargs, newKey='fields', datatype=list())
		kwargs['fields'] = kwargs['fields'] if len(kwargs['fields']) > 0 else fields

		dfs = []
		for i in self.PlayerCatalog['Squads']:
			ob = OptaSquads(i)
			df = ob.getPlayersChanges(withStat= True, withTeam=True, **kwargs)
			dfs.append(df)

		return pd.concat(dfs)

	def getMatchsGoals(self, **kwargs):
		"""
		Get data of all goals in matchs in season

		params:
			fields (list) : optional fields to return , 
			removeFields (list): columns to exclude from finalDataframe
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			constants (dict) : dict as {"column to add": "constant Value"}
	
		return:
			pandas dataframe		
		"""	
		if not 'results' in self.PlayerCatalog:
			return "No Opta results files was found in catalog"

		filtersk = createNewKey(path=kwargs, newKey='filters', datatype=dict())
		filtersk['@PlayerRef'] = 'p'+self.player

		dfs = []
		for i in self.PlayerCatalog['results']:
			ob = OptaResults(i)
			df = ob.getGoals(**kwargs)
			dfs.append(df)

		return pd.concat(dfs)

####################################################################################
########################             Start Class            ########################
########################            to work with            ########################
########################               Events               ########################
####################################################################################

class Events():
	"""
	Allows to interact with events in all F24 files.
	
	Params:
		event (str): Event ID to analize
		files (dict) : optional - filesTypes and location of every file with data of the team
		Catalog (object) : optional - instance of OptaCatalog class with direftory of files 
		location (str) : optional - folder with optafiles where data of team is located


	Attributes:
		team (str) : Team Name


	"""

	def __init__(self, event:str, **kwargs):
		self.event = str(event)
		self.f24Files = self.mapFiles(**kwargs)

	def mapFiles(self, **kwargs):
		"""
		locate the files about team. When instantiate with files uses this dict
		when instantitate with catalog filter the file team attribute of this class to get the team files
		wheren instatiate with locations, make the catalog and filter the team attribute
		"""
		files = kwargs['files'] if 'files' in kwargs else None
		Catalog = kwargs['Catalog'] if 'Catalog' in kwargs else None
		location = kwargs['location'] if 'location' in kwargs else None

		if files:
			return files
		
		elif Catalog:
			return Catalog.OptaFiles['F24']

		elif location:
			Catalog = OptaCatalog(location)
			f24s = Catalog.OptaFiles['F24']
			return f24s

		else:
			raise Exception('files, Catalog or location should be passed at init class')


	def getEvents(self, **kwargs):
		"""
		Get all events of this type in all files 

		params:
			fields (list) : optional fields to return , 
			removeFields (list): columns to exclude from finalDataframe
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			constants (dict) : dict as {"column to add": "constant Value"}

		return:
			pandas dataframe
		
		"""
		if len(self.f24Files) == 0:
			return "No Opta F24 files was found in catalog"


		filtersk = createNewKey(path=kwargs, newKey='filters', datatype=dict())
		kwargs['filters']['@type_id'] = self.event

		dfs = []
		for i in self.f24Files:
			df = OptaF24(i)
			dfs.append(df.getEvents(**kwargs))

		return pd.concat(dfs)



	def getQualifiedsEvents(self, **kwargs):
		"""
		Get events with qualifications

		params:
			fields (list) : optional fields to return , 
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}


		return:
			pandas dataframe
		
		"""
		if len(self.f24Files) == 0:
			return "No Opta F24 files was found in catalog"

		filtersk = createNewKey(path=kwargs, newKey='filters', datatype=dict())
		kwargs['filters']['@type_id'] = self.event

		dfs = []
		for i in self.f24Files:
			df = OptaF24(i)
			dfs.append(df.getQualifiedEvents(**kwargs))

		return pd.concat(dfs)


	def PlotPitchEvents(self, **kwargs):
		"""
		Show a footbaal pitch with events location
		
		params:
			agg (str) : kind of aggregation. Can be sum, min, max, mean, std, count, nunique
			qualified (bool) : when true retrieves dataframe with qualifiers else only events
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			marker (str) : optional - column name to set as marker type
			color (str) : optional - column name to set as color
			tag (str) : optional - column name to show as tag
			recolor (bool) : optoional allows to change colors settigns randomly
			title (str) : Optional -  Title of the chart
			colorsSet (list) : list of color in hexa to use as codes color
			shapesSet (list) : list of shapes to use as codes color
			

		return:
			dataframe
		"""
		if len(self.f24Files) == 0:
			return "No Opta F24 files was found in catalog"

		marker = kwargs['marker'] if 'marker' in kwargs else ""
		recolor = kwargs['recolor'] if 'recolor' in kwargs else False
		color = kwargs['color'] if 'color' in kwargs else ""
		tag = kwargs['tag'] if 'tag' in kwargs else ""
		title =  kwargs['title'] if 'title' in kwargs else ""
		codeColors = kwargs['colorsSet'] if 'colorsSet' in kwargs else codes['colors']
		codeShapes = kwargs['shapesSet'] if 'shapesSet' in kwargs else codes['shapes']
	
		if recolor:	
			shuffle(codeShapes)
			shuffle(codeColors)

		qualified = kwargs['qualified'] if 'qualified' in kwargs else False
		
		data = self.getQualifiedEvents(**kwargs) if qualified else self.getEvents(**kwargs)

		data = data.astype({"@x": float, "@y": float})
		
		markers = list(dict.fromkeys(data[marker].values)) if marker else [""]
		colors = list(dict.fromkeys(data[color].values)) if color else [""]

		fig, ax = drawOptaPitch()

		for i, row in data.iterrows():
			mar = row[marker] if marker else ""
			col = row[color] if color else ""
			t = row[tag] if tag else ""

			pointMarkers = codeShapes[markers.index(mar)]
			pointColors = codeColors[colors.index(col)]


			plt.scatter(x=row['@x'], y =row['@y'], s= 100, marker= pointMarkers, color= pointColors)
			ax.text(x=row['@x'], y =row['@y'], s= t)
			
		plt.axis("off")
		plt.title(title)
		plt.show()


	def PlotRankingEvents(self, groupBy:str, metric:str, agg:str, **kwargs):
		"""
		Make a horizontal bar chart with event data order desc grouping with groupBy param and aggregating a metric with agg param
		
		params:
			groupBy (str) : column name to group by the metric
			metric (str) : column name to summarize
			agg (str) : kind of aggregation. Can be sum, min, max, mean, std, count, nunique
			top (int) : number of rows to show default 50
			qualified (bool) : when true retrieves dataframe with qualifiers else only events
			filters (dict) : optional filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
			exclude (dict) : optional filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
		
		return:
			dataframe
		"""
		top = kwargs['top'] if 'top' in kwargs else 50
		if len(self.f24Files) == 0:
			return "No Opta F24 files was found in catalog"

		qualified = kwargs['qualified'] if 'qualified' in kwargs else False
		data = self.getQualifiedEvents(**kwargs) if qualified else self.getEvents(**kwargs)

		getRanking(df=data, groupBy=groupBy, metric=metric, agg=agg, top=top )


		