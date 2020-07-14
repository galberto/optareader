Esta librería esta construída para explotar datos de Opta. 
Las sigeuientes librerías son necesarias: pandas, xmltodict y matplotlib


Contiene las siguientes clases y métodos

## OptaCatalog
Lee todos los files existentes en un directorio y almacena en memoria información para acceder a ellos de manera rápida.

	showSummary()
		muestra los tipos y conteos de archivos encontrados

	getPlayersDict()
		muestra un dataframe con los jugadores con su respectivo ID

	OptaFiles -> diccionario con clave tipo de file y valor lista de archivos
	OptaTeams -> diccionario con clave equipo y valor lista de archivos 
	OptaPlayersID -> diccionario con clave jugador y valor lista de archivos 


## OptaFile
Lee un file, lo clasifica, genera un catálogo de datos y permite acceder a ellos.

	Catalog -> diccionario con key path y value datos para localizar el dato en el file

	giveMe
		Método que dado un path del catálogo devuelve llega al valor dentro del file

	giveMeDF
		Método que dado un path de un dataframe permite acceder a él y a sus padres

## OptaF24
hereda atributos de de OptaFile y agrega métodos propios sobre eventos

	getEvents
		muestra un dataframe con los eventos del file

	getQualifiedEvents
		muestra un dataframe con los eventos del file y sus calificadores

	PlotRanking
		Muestra un gráfico de barras con el ranking de un campo a traves de una métrica y su agregación

	PlotPitchEvents
		Muestra un campograma con la distribución espacial de eventos

## OptaF7
hereda atributos de de OptaFile y agrega métodos propios sobre resultados de partidos
	
	getPlayers
		devuelve un dataframe con los jugadores del partido

	getTeams
		devuelve un dataframe con los equipos del partido

	getBookings
		devuelve un dataframe con los sancionados del partido

	getGoals
		devuelve un dataframe con los goles del partido	

	getSubstitutions
		devuelve un dataframe con los cambios en el partido	

	getOfficials
		devuelve un dataframe con los entrenadores de los equipos

	getLineUps
		devuelve un dataframe con las formaciones de los equipos

	getSubstitutions
		devuelve un dataframe con las sustituciones por equipo

## OptaPassMatrix
hereda atributos de de OptaFile y agrega métodos propios sobre matriz de pases 

	getPlayerPassStats
		muestra un dataframe con las estadísticas de pases del equipo en un partido

	getPlayerPass
		muestra un dataframe con las estadísticas de pases de los jugadores y sus receptores en un partido 

	getPassMatrix
		devuelve un dataframe con los datos de pasadores y receptores y su posición media en el campo

	plotPassMatrix
		devuelve un campograma con los pases entre jugadores según su posición media en el campo

	PlotRanking
		Muestra un gráfico de barras con el ranking de un campo a traves de una métrica y su agregación

## OptaMatchResults
hereda atributos de de OptaFile y agrega métodos propios sobre resultados de partidos

	getJudges
		devuelve un dataframe con los árbitros del partido
	
	getPlayers
		devuelve un dataframe con los jugadores del partido

	getTeams
		devuelve un dataframe con los equipos del partido

	getBookings
		devuelve un dataframe con los sancionados del partido

	getGoals
		devuelve un dataframe con los goles del partido	

	getPlayersWithStats
		devuelve un dataframe con las estadísticas de los juegadores en el partido

	getSubstitutions
		devuelve un dataframe con los cambios en el partido	

	PlotRanking
		Muestra un gráfico de barras con el ranking de un campo a traves de una métrica y su agregación

## OptaSeasonStats
hereda atributos de de OptaFile y agrega métodos propios sobre las estadísticas de la temporada de un equipos

	getTeamStats
		devuelve un dataframe con los resultados de la temporada de un equipo

	getPlayersStats
		devuelve un dataframe con los resultados de la temporada de los jugadores del equipo

	PlotRanking
		Muestra un gráfico de barras con el ranking de un campo a traves de una métrica y su agregación

## OptaStandings
hereda atributos de de OptaFile y agrega métodos propios sobre los resultados de la temporada de un equipo

	getStandings
		devuelve un dataframe con los resultados de los equipos

	PlotRanking
		Muestra un gráfico de barras con el ranking de un campo a traves de una métrica y su agregación


## OptaSquads
hereda atributos de de OptaFile y agrega métodos propios sobre los equipos de una temporada y competición

	getTeamsAndStadiums
		devuelve un dataframe con datos de los equipos y los estadios

	getOfficials
		devuelve un dataframe con datos de los equipos técnicos

	getPlayersStats
		devuelve un dataframe con datos descriptivos de los jugadores

	getPlayersChanges
		devuelve un dataframe con datos descriptivos de los jugadores traspasados y datos de los traspasos

	getOfficialsChanges
		devuelve un dataframe con datos descriptivos de los técnicos traspasados y datos de los traspasos


## OptaResults
hereda atributos de de OptaFile y agrega métodos propios sobre los resultados de los partidos en la temporada en una competición

	getMatchData
		devuelve un dataframe con los datos de los partidos

	getGoals
		devuelve un dataframe con los datos de los goles en los partidos

	getOfficials
		devuelve un dataframe con los árbitros de los partidos


## OptaF28
hereda atributos de de OptaFile y agrega métodos propios sobre los resultados de posesión durante un partido

	getIntervals
		devuevelve un dataframe con datos de posesión por intervalos

	getLastX
		devuevelve un dataframe con datos de posesión por lastX

	getOverall
		devuevelve un dataframe con datos de posesión en el total del partido

	getBallPossession
		devuevelve un dataframe con datos de posesión de pelota

	getTerritorial
		devuevelve un dataframe con datos de posesión por lado del campo

	getTerritorialThird
		devuevelve un dataframe con datos de posesión por intervalos dividido en local, visitante y medio

	PlotPossession
		devuelve un gráfico de barras con la posesión


## Teams
Permite leer múltiples archivos de Opta y devuelve información de un equipo en particular

	getEvents
		muestra los datos de eventos con las funciones de OptaF24

	getQualifiedsEvents
		muestra los datos de eventos calificados con las funciones de OptaF24

	PlotPitchEvents
		muestra el campograma con los eventos

	PlotRankingEvents
		Muestra un gráfico de barras con el ranking de un campo a traves de una métrica y su agregación

	getPossession
		Devuelve un dataframe con todos los datos de posesión

	PlotAvgPossession
		Devuelve un gráfico con la posesión media en todos los files

	getMatchResultsPlayerStats
		Devuelve un dataframe con todos las estadísticas de los jugadores en todos los archivos match results del directorio  de un file F9

	PlotMatchResultsRanking
		Muestra un gráfico de barras con el ranking de un campo a traves de una métrica y su agregación de un file F9

	getMatchResultsBookings 
		Devuelve un dataframe con todos los sancionados del equipo  de un file F9

	getMatchResultsGoal
		Devuelve un dataframe con todos los goles del equipo  de un file F9

	getMatchResultsSubstitutions
		Devuelve un dataframe con todos los cambios del equipo en los partidos de un file F9

	getF7Substitutions
		Devuelve un dataframe con todos los cambios del equipo en los partidos de un file F7

	getF7Players
		Devuelve un dataframe con todos los jugadores del equipo en los partidos de un file F7
	
	getF7Bookings
		Devuelve un dataframe con todos los jugadores sancionados del equipo en los partidos de un file F7

	getF7Goal
		Devuelve un dataframe con todos los goles del equipo en los partidos de un file F7


	getPassStats
		Devuelve un dataframe con todos las estadísticas de pases de los jugadores

	PlotPassStats
		Muestra un gráfico de barras con el ranking de un campo a traves de una métrica y su agregación

	getPassReceptors
		Muestra un dataframe con todos los pasadores y receptores

	getPassMatrixData
		Muestra un dataframe con todos los pasadores y receptores y sus posiciones medias

	getPassMatrix
		Genera una matriz de pases con los pasadores en filas y los receptores en columnas según un tipo de agregación para la cantidad de pases

	getTeamSeasonStats
		Devuelve un dataframe con las estadísticas de las temporada de los equipos

	getTeamPlayersSeasonStats
		Devuelve un dataframe con las estadísticas de las temporada de los jugadores

	PlotTeamPlayersRank
		Muestra un gráfico de barras con el ranking de un campo a traves de una métrica y su agregación

	getStandings
		Muestra los resultados del equipo en la temporada

	getTeamData
		devuelve un dataframe con los datos del equipo

	getTeamOfficials
		devuelve un dataframe con los datos del equipo técnico del equipo

	getTeamOfficialsChanges
		devuelve un dataframe con los datos de los cambios de equipo técnico si los hubiera

	getTeamPlayers
		devuelve un dataframe con los datos de los jugadores del equipo

	getTeamPlayersChanges
		devuelve un dataframe con los datos de los cambios de jugadores si los hubiera

	getMatchsData
		devuelve un dataframe con los datos de los partidos de la temporada

	getMatchsGoals
		devuelve un dataframe con los datos de los goles de la temporada

	getMatchsOfficials
		devuelve un dataframe con los datos de los referis de cada partido del equipo


## Players
Permite leer múltiples archivos de Opta y devuelve información de un jugador

	getEvents
		muestra los datos de eventos con las funciones de OptaF24

	getQualifiedsEvents
		muestra los datos de eventos calificados con las funciones de OptaF24

	PlotPitchEvents
		muestra el campograma con los eventos

	PlotRankingEvents
		Muestra un gráfico de barras con el ranking de un campo a traves de una métrica y su agregación

	getMatchResultsPlayerStats
		Devuelve un dataframe con todos las estadísticas del jugador en todos los archivos match results del directorio

	getMatchResultsBookings
		devuelve un dataframe con todas las sanciones del jugador

	getMatchResultsGoal
		devuelve un dataframe con todas los goles del jugador

	getMatchResultsAssits
		devuelve un dataframe con todas las asistencias del jugador

	getMatchResultsSubstitutions
		devuelve un dataframe con todas las sustituciones del jugador

	getPassStats
		devuelve un dataframe con todas las estadísticas de pases del jugador por partido

	getPassReceptors
		devuelve los datos de los receptores de pases del jugador

	getPassMatrixData
		devuelve los datos de pases del jugador con posición media en la cancha

	PlotBestTeammates
		Devuelve el ranking de receptores más frecuentes del jugador

	getSeasonStats
		Devuelve un dataframe con las estadísticas de la temporada del jugador

	getPlayersData
		Devuelve un dataframe con los datos descriptivos del jugador

	getPlayersChanges
		devuelve datos de las transferencias del jugadores si las hubiera

	getMatchsGoals
		Devuelve los goles del jugador en la temporada


## Events
Permite leer múltiples archivos F24 de Opta y devuelve información de un evento

	getEvents
		muestra los datos del evento con las funciones de OptaF24

	getQualifiedsEvents
		muestra los datos del evento calificados con las funciones de OptaF24

	PlotPitchEvents
		muestra el campograma con los eventos

	PlotRankingEvents
		Muestra un gráfico de barras con el ranking de un campo a traves de una métrica y su agregación