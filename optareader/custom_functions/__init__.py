import xmltodict
import json
from ..dictionary import *
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.patches import Arc



####################################################################################
########################              functions             ########################
########################              to enrich             ########################
########################               Catalog              ########################
####################################################################################

def pandasFromList(dfColumn, **Kwargs ):
	"""
	iterate again and fill an empty list.
	if value is list of dicts then we iterate on every value and add each value to empty list
	if value is dict then we add value to empty list
	
	param:
		dfColumn(list): list of values with dicts

	return:
		pandas Dataframe or None
	"""
	parentIndex = Kwargs['parentIndex'] if 'parentIndex' in Kwargs else None

	output = []
	for i, a in dfColumn.iteritems():
		if isinstance(a, list):
			for b in a:
				if parentIndex:
					b[parentIndex] = i
				output.append(b)

		if isinstance(a, dict):
			if parentIndex:
				a[parentIndex] = i
			output.append(a)

	# create a dataframe with the list and return it
	df = pd.DataFrame(output)
	return df



def tryToPanda(dfColumn, check=25, **Kwargs):
	"""
	gets a list and convert in a dataframe if it has dicts or list of dicts

	params:
		dfColumn (list): list of values with or without dicts
		check (int): optional. Rows to check for dicts

	return:
		pandas Dataframe or None
	"""

	# as a default we'll not do anything else than check the values
	doSomething = False
	
	cases = 0 # will increase this number in each value iteration
	for a in dfColumn:

		#stop iteration when reach the target value
		if cases == check:
			break
	
		# If any value is list or dict will change doSomething value and set cases to a break value
		if isinstance(a, list) or isinstance(a, dict):
			doSomething = True
			cases = check #to break
		else:
			cases = cases +1 #try many tames as check is seted

	# iterate again and fill an empty list.
	# if value is list of dicts then we iterate on every value and add each value to empty list
	# if value is dict then we add value to empty list
	if doSomething:
		df = pandasFromList(dfColumn, **Kwargs)
		return df



def RemoveLastStep(parent:str):
	parent = parent.split(".")
	parent.pop()
	parent = '.'.join(parent)
	return parent


def searchOnPanda(df = pd.DataFrame, outputDict = {}, ParentKey = '', ParentValue=''):
	"""
	search in any panda column and add the values to a dict as enrich catalog

	params:
		df (pd.DataFrame): dataframe for iterate over
		outputDict (dict): empty dict to be filled with the results
		ParentKey (string): key used for parent and be passed to child
		ParentValue (string): value used for parent and be passed to child

	return:
		None
	"""

	#iterate over all columns on dataframe
	for a in df:
		#set the key as parent.currentKey
		outputKey = "{}.{}".format(ParentKey, a)

		# if Parent is a str (first itereation only) value is seted as a list with two values 0 for parent and a list for each column with a df
		if isinstance(ParentValue, str):
			outputValue = [ParentValue, [a]]

		# if parent is list add the new value to the parent value in second value of main list
		elif isinstance(ParentValue, list):
			outputValue = [ParentValue[0]] + [ParentValue[1] + [a]]

		# fill the outputDict with the key and values
		parent = RemoveLastStep(ParentKey)
		outputDict[ParentKey] = {}
		outputDict[ParentKey]['path'] = ParentValue
		outputDict[ParentKey]['datatype'] = 'Dataframe'
		outputDict[ParentKey]['parent'] = parent

		
		outputDict[outputKey] = {}
		outputDict[outputKey]['path'] = outputValue
		outputDict[outputKey]['datatype'] = 'list'
		outputDict[outputKey]['parent'] = ParentKey

		# check if column could be pandas and start again
		new = tryToPanda(df[a])

		if isinstance(new, pd.DataFrame):
			searchOnPanda(new, outputDict=outputDict, ParentKey=outputKey, ParentValue = outputValue )


####################################################################################
########################              functions             ########################
########################              to pretty             ########################
########################                print               ########################
####################################################################################


def pretty(x):
	"""
	Print in console in a beauty way
	
	args x: anything to print

	return none -> print x
	"""
	if isinstance(x, dict):
		print(json.dumps(x, indent=4, sort_keys=False))
	else:
		print (x)


def write(x, outputFile):
	"""
	write in file in a beauty way
	
	args 
		x: anything to print
		outputFile: file in wich data is writen

	return none -> write x
	"""
	if isinstance(x, dict):
		with open(outputFile, 'w') as file:
			json.dump(x, file)
	else:
		print (x)


def createNewKey(path: dict, newKey:str, datatype:type):
	if newKey not in path:
		path[newKey] = datatype

	return path[newKey]


####################################################################################
########################              functions             ########################
########################             to work with           ########################
########################               Catalog              ########################
####################################################################################


def getJson(file:str):
	with open(file, 'rb') as f:
		try:
			dic = xmltodict.parse(f)
			return dic
		except:
			return "unrecongnized"


def clasiJson(catalog:dict, typesAndPath=dict):
	for a in typesAndPath:
		if a in catalog:
			return typesAndPath[a]
	return None


# def getJsonStruct(nodejson : dict, output:dict, test:dict, path=''):
def getJsonStruct(nodejson : dict, output:dict, path=''):
	if isinstance(nodejson, dict):
		for key, item in nodejson.items():
			runningPath = '' + path
			if isinstance(item, dict):
				runningPath = "{}['{}']".format(runningPath, key)
				# getJsonStruct(item, output, test, runningPath)
				getJsonStruct(item, output, runningPath)
		
			else:
				try:
					finalpath = "{}['{}']".format(path, key)
					# eval("test"+finalpath)
					parent = finalpath.replace("]", "").replace("'", "").split("[")[-2]
					output[finalpath] = "{}.{}".format(parent, key)
				except:
					pass
	else:
		return "not a valid json"

# def getJsonCatalog(nodejson : dict, output:dict, test:dict,  path=''):
def getJsonCatalog(nodejson : dict, output:dict,  path=''):
	if isinstance(nodejson, dict):
		for key, item in nodejson.items():
			runningPath = '' + path
		
			if isinstance(item, dict):
				runningPath = "{}['{}']".format(runningPath, key)
				getJsonCatalog(item, output, runningPath)
		
			else:
				try:
					finalpath = "{}['{}']".format(path, key)
					parent = finalpath.replace("]", "").replace("'", "").split("[")[-2]
					output["{}.{}".format(parent, key)] = finalpath
				except:
					pass
	else:
		return "not a valid json"


def getPrefix(path:str):
	"""
	Get a string from catalog and return a string of initial of each word in path to use as prefix
	
	params:
		path (str) : path from catalog as maintable.subtable.subsubtable
	
	return:
		str: all initials in path maintable.subtable.subsubtable -> mss
	"""

	s = path
	s = s.split(".")
	s = ['{}'.format(x[0]) for x in s]
	s = ''.join(s)+'.'
	return s

####################################################################################
########################              functions             ########################
########################             to work with           ########################
########################              dataframes            ########################
####################################################################################
def rowsToDF(dfColumn):
	"""
	Convert a list of dict or list of list of dict into a dataframe

	args:
		dfColumn (list) : list of dict or list of list of dicts

	return:
		dataframe: dataframe 
	"""
	lista = []
	for i in dfColumn:
		for a in i:
			if isinstance(a, dict):
				lista.append(a)
			elif isinstance(a, list):
				for b in a:
					lista.append(b)

	dfr = pd.DataFrame(lista)
	return dfr

def pruningDF(df, fields=None, filters=None, exclude=None, removeFields=None, constants = None, **kwargs ):
	"""
	select specific fields and specifics rows from dataframe
	
	params:
		fields (list) : fields to return , 
		filters (dict) : filter including rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
		exclude (dict) : filter excluding rows by column names and field value as {field1 : value, field2: [valuea, valueb]}
		removeFields (list) : fields of column names to exclude in dataframe to return
		constants (dict) : dict as {"column to add": "constant Value"}
	
	return:
		dataframe
	"""

	fields = fields if fields != None else list(df)
	removeFields = removeFields if removeFields != None else []
	filters = filters if filters != None else {}
	exclude = exclude if exclude != None else {}
	constants = constants if constants != None else {}

	#field to show in final DF
	fieldtoDF = []
	fieldtoDF = fields + list(filters)
	fieldtoDF = list(dict.fromkeys(fieldtoDF))

	originalDF =  df
	originalDF = originalDF[fieldtoDF]

	listerFilt = []
	for i in filters:
		val = filters[i]
		fil = [val] if not isinstance(val, list) else val
		listerFilt.append("df['{}'].isin({})".format(i, fil))

	for a in exclude:
		valexc = exclude[a]
		filExc = [valexc] if not isinstance(valexc, list) else valexc
		listerFilt.append("~df['{}'].isin({})".format(a, filExc))

	filts =  ((" & ").join(listerFilt))
	if len(filts) > 1:
		originalDF =  originalDF[eval(filts)]


	originalDF = originalDF.drop(removeFields, axis=1)

	for i in constants:
		originalDF[i] = constants[i]

	return originalDF


####################################################################################
########################              functions             ########################
########################             to work with           ########################
########################                plots               ########################
####################################################################################


def getRanking(df:pd.DataFrame, groupBy:str, metric:str, agg:str):
	"""
	Make a horizontal bar chart order desc grouping with groupBy param and aggregating a metric with agg param
	
	params:
		df (pd.DataFrame) : dataframe with data to plot
		groupBy (str) : column name to group by the metric
		metric (str) : column name to summarize
		agg (str) : kind of aggregation. Can be sum, min, max, mean, std, count, nunique
	
	return:
		dataframe
	"""
	if agg != 'count' and agg != 'nunique':
		df[metric] = pd.to_numeric(df[metric], downcast="float")

	grouped = df.groupby([groupBy])
	grouped = eval("grouped[metric].{}()".format(agg) )
	grouped = grouped.sort_values()
	grouped.plot(kind='barh')
	plt.title("{} {} by {}".format(agg, metric, groupBy))
	plt.show()


OPTA_PITCH = {
    #gross Lines
    "inicioCampo" : "plt.plot([0,0],[0,100], color='black')",
    "bandaIzq" : "plt.plot([0,100],[100,100], color='black')",
    "finCampo": "plt.plot([100,100],[100,0], color='black')",
    "bandaDer": "plt.plot([100,0],[0,0], color='black')",
    "medioCampo": "plt.plot([50,50],[0,100], color='black')",

    # Left Penalty Area
    "finAreaIzq": "plt.plot([17,17],[78.9,21.1], color='black')",
    "areaIzqbandaIzq": "plt.plot([0,17],[78.9,78.9], color='black')",
    "areaIzqbandaDer": "plt.plot([17,0],[21.1,21.1], color='black')",

    # Left 6-yard Box
    "subAreaIzqbandaIzq": "plt.plot([0,5.8],[63.2,63.2], color='black')",
    "finSubAreaIzq": "plt.plot([5.8,5.8],[63.2,36.8], color='black')",
    "subAreaizqbandaDer": "plt.plot([5.8,0],[36.8,36.8], color='black')",
    
    # Right Penalty Area
    "areaDerbandaIzq": "plt.plot([100,83],[78.9,78.9], color='black')",
    "finAreaDer": "plt.plot([83,83],[78.9,21.1], color='black')",
    "areaDerbandaDer": "plt.plot([83,100],[21.1,21.1], color='black')",

    # Right 6-yard Box 
    "subAreaDerbandaIzq": "plt.plot([100,94.2],[63.2,63.2], color='black')",
    "finSubAreaDer": "plt.plot([94.2,94.2],[63.2,36.8], color='black')",
    "subAreaDerbandaDer": "plt.plot([94.2,100],[36.8,36.8], color='black')",

    # Circles
    "centreCircle": "ax.add_patch(plt.Circle((50,50),9.15,color='black',fill=False))",
    "centreSpot": "ax.add_patch(plt.Circle((50,50),0.6,color='black'))",
    "leftPenSpot": "ax.add_patch(plt.Circle((11.5,50),0.6,color='black'))",
    "rightPenSpot": "ax.add_patch(plt.Circle((88.5,50),0.6,color='black'))",

    # Arcs
    "leftArc": " ax.add_patch(Arc((11.5,50),height=18.3,width=18.3,angle=0, theta1=310,theta2=50,color='black'))",
    "rightArc": " ax.add_patch(Arc((88.5,50),height=18.3,width=18.3,angle=0,theta1=130,theta2=230,color='black'))"
} 

def drawOptaPitch():
	"""
	Return a plot with lines representing game pitch
	
	params:

	return:
		plot 
	"""
	fig, ax = plt.subplots(figsize=(18,14))
	for i in OPTA_PITCH:
		eval(OPTA_PITCH[i])
	return fig,ax


codes = {
	"colors" : ["#5F1024",  "#B525BA", "#6B5DCA", "#CA271D", "#A03E8D", "#22816A", 
			"#E6FB9A", "#B76C4D", "#F3E6DF", "#C01871", "#E28B4C", "#61D2EA", "#65AE3F", 
			"#050271",  "#E4A6BA", "#15FC8B", "#8C4D2B", "#58572D", "#41CFBF", "#55238E"],
	"shapes" : ['o', 's','*', '^','.']


	}

