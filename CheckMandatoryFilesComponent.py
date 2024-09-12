from pathlib import Path
import os

class CheckMandatoryFilesProcessing():

	def checkMandatoryFiles(iGTFSExplodedFeedFolder):
		print('===> check mandatory files existence...')
		# based on https://gtfs.org/reference/static/#file-requirements as of 2022-06-08

		# agency.txt			Required				Transit agencies with service represented in this dataset.
		# stops.txt				Required				Stops where vehicles pick up or drop off riders. Also defines stations and station entrances.
		# routes.txt			Required				Transit routes. A route is a group of trips that are displayed to riders as a single service.
		# trips.txt				Required				Trips for each route. A trip is a sequence of two or more stops that occur during a specific time period.
		# stop_times.txt		Required				Times that a vehicle arrives at and departs from stops for each trip.
		# calendar.txt			Conditionally required	Service dates specified using a weekly schedule with start and end dates. This file is required unless all dates of service are defined in calendar_dates.txt.
		# calendar_dates.txt	Conditionally required	Exceptions for the services defined in the calendar.txt. If calendar.txt is omitted, then calendar_dates.txt is required and must contain all dates of service.
		# shapes.txt			Conditionally required	Even though conditionally required this file is mandatory to perform a correct conversion to NeTEx

		# If one of these files doesn't exist the program terminates with an appropriate error message
		flagError=False
		cumulativeErrorMsg=""
		calendarExists=False
		calendarDatesExists=False

		# check for agency.txt existence
		if Path(iGTFSExplodedFeedFolder + os.sep + 'agency.txt').is_file():
			print("Mandatory file agency.txt exists in feed...")
		else:
			flagError=True
			cumulativeErrorMsg=cumulativeErrorMsg + "\n" + "ERROR: Mandatory file agency.txt NOT EXISTING in feed...terminate program..."

		# check for feed_info.txt existence
		if Path(iGTFSExplodedFeedFolder + os.sep + 'feed_info.txt').is_file():
			print("Mandatory file feed_info.txt exists in feed...")
		else:
			flagError=True
			cumulativeErrorMsg=cumulativeErrorMsg + "\n" + "ERROR: Mandatory file feed_info.txt NOT EXISTING in feed...terminate program..."

		# check for stops.txt existence
		if Path(iGTFSExplodedFeedFolder + os.sep + 'stops.txt').is_file():
			print("Mandatory file stops.txt exists in feed...")
		else:
			flagError=True
			cumulativeErrorMsg=cumulativeErrorMsg + "\n" + "ERROR: Mandatory file stops.txt NOT EXISTING in feed...terminate program..."

		# check for routes.txt existence
		if Path(iGTFSExplodedFeedFolder + os.sep + 'routes.txt').is_file():
			print("Mandatory file routes.txt exists in feed...")
		else:
			flagError=True
			cumulativeErrorMsg=cumulativeErrorMsg + "\n" + "ERROR: Mandatory file routes.txt NOT EXISTING in feed...terminate program..."

		# check for trips.txt existence
		if Path(iGTFSExplodedFeedFolder + os.sep + 'trips.txt').is_file():
			print("Mandatory file trips.txt exists in feed...")
		else:
			flagError=True
			cumulativeErrorMsg=cumulativeErrorMsg + "\n" + "ERROR: Mandatory file trips.txt NOT EXISTING in feed...terminate program..."

		# check for stop_times.txt existence
		if Path(iGTFSExplodedFeedFolder + os.sep + 'stop_times.txt').is_file():
			print("Mandatory file stop_times.txt exists in feed...")
		else:
			flagError=True
			cumulativeErrorMsg=cumulativeErrorMsg + "\n" + "ERROR: Mandatory file stop_times.txt NOT EXISTING in feed...terminate program..."			

		# check for calendar.txt existence
		if Path(iGTFSExplodedFeedFolder + os.sep + 'calendar.txt').is_file():
			print("Mandatory file calendar.txt exists in feed...")
			calendarExists=True

		# check for calendar_dates.txt existence
		if Path(iGTFSExplodedFeedFolder + os.sep + 'calendar_dates.txt').is_file():
			print("Mandatory file calendar_dates.txt exists in feed...")
			calendarDatesExists=True

		# check for shapes.txt existence
		if Path(iGTFSExplodedFeedFolder + os.sep + 'shapes.txt').is_file():
			print("Mandatory file shapes.txt exists in feed...")
		else:
			flagError=True
			cumulativeErrorMsg=cumulativeErrorMsg + "\n" + "ERROR: Mandatory file shapes.txt NOT EXISTING in feed...terminate program..."

		# either calendar.txt or calendar_dates.txt MUST exist
		if (calendarExists==False and calendarDatesExists==False):
			flagError=True
			cumulativeErrorMsg=cumulativeErrorMsg + "\n" + "ERROR: Either mandatory file calendar.txt or calendar_dates.txt MUST EXIST in feed...terminate program..."						
									

		if flagError:
			exit(cumulativeErrorMsg)





