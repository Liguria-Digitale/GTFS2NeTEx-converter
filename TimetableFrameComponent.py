import sqlite3
import os
import CreatePassingTimesSubComponent
import SupportUtilitiesSubComponent

class TimetableFrameProcessing():

	def processTimetableFrame(self, iGTFSExplodedFeedFolder, iNUTS, iDb, iAz, iVersion, iVAT):
		print('Processing TimetableFrame...')

		conn=sqlite3.connect(iGTFSExplodedFeedFolder + '/' +iDb + '.db')
		c=conn.cursor()

		opening_text="""<TimetableFrame id="epd:it:TimetableFrame_EU_PI_TIMETABLE:ita" version="1"><TypeOfFrameRef ref="epip:EU_PI_TIMETABLE" versionRef="1"/><vehicleJourneys>"""				
		closing_text="""</vehicleJourneys></TimetableFrame>"""

		# c.execute('''SELECT 
		# 				a.route_id, a.service_id, a.trip_id, a.direction_id, a.shape_id, a.starts_at, a.trip_headsign, a.duration,
		# 				"PT" || CAST(CAST(a.duration AS INTEGER)/3600 AS TEXT) || 
		# 				"H" || CAST(IIF(CAST(a.duration AS INTEGER)/60 < 60, 
		# 				               CAST(a.duration AS INTEGER)/60, 
		# 									CAST(a.duration AS INTEGER)/60 - 60 * (CAST(a.duration AS INTEGER)/3600)) AS TEXT) || 
		# 				"M0S"  AS duration_iso8616,						
		# 				IIF(a.route_type_decoded="lift", "unknown", a.route_type_decoded),
		# 				b.d_distance, (SELECT c.agency_id FROM tb_routes AS c WHERE c.route_id=a.route_id) AS agency_id,
		# 				spatt.space_patt,
		# 				a.departureTimeExtended, a.departureDayOffset
		# 			 FROM 
		# 				v_routes_trips_stops_stoptimes_3 AS a, 
		# 				v_shapes_gml_with_distance AS b,
	    #                 v_spatial_patterns AS spatt	
        #              WHERE 
	    #                 b.d_shape_id=a.shape_id
	    #                 AND 
		# 				a.trip_id=spatt.trip_id
        #              ORDER BY 
	    #                 a.route_id, a.trip_id;''')	
		# 
		c.execute('''SELECT 
						a.route_id, a.service_id, a.trip_id, a.direction_id, a.shape_id, a.starts_at, a.trip_headsign, a.duration,
						"PT" || CAST(CAST(a.duration AS INTEGER)/3600 AS TEXT) || 
						"H" || CAST(IIF(CAST(a.duration AS INTEGER)/60 < 60, 
						CAST(a.duration AS INTEGER)/60, 
						CAST(a.duration AS INTEGER)/60 - 60 * (CAST(a.duration AS INTEGER)/3600)) AS TEXT) || 
						"M0S"  AS duration_iso8616,						
						IIF(a.route_type_decoded="lift", "unknown", a.route_type_decoded) AS route_type_decoded,
						b.d_distance, (SELECT c.agency_id FROM tb_routes AS c WHERE c.route_id=a.route_id) AS agency_id,
						spatt.space_patt,
						a.min_stop_sequence, c.min_stop, c.departure_time_extended, c.departure_day_offset
					FROM 
						t_routes_trips_stops_stoptimes_3 AS a, 
						v_shapes_gml_with_distance AS b,
						v_spatial_patterns AS spatt,	
						v_extended_starting_time AS c
					WHERE 
						b.d_shape_id=a.shape_id
						AND 
						a.trip_id=spatt.trip_id
						AND
						a.route_id=c.route_id
						AND 
						a.trip_id=c.trip_id
						AND 
						a.min_stop_sequence=c.min_stop								
					ORDER BY 
						a.route_id, a.trip_id;''')					
		records=c.fetchall()

		numServiceJourneys=str(len(records))
		print(">>>>> Total ServiceJourneys is:  ", numServiceJourneys)

		outText=""

		cntServiceJourneys=0

		with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-TimetableFrame-temp.xml', 'w', encoding='utf-8') as f: f.write(opening_text)		

		for row in records:

			chunkText=""
			passingTimes_start=CreatePassingTimesSubComponent.CreatePassingTimesProcessing			
			passingTimes=passingTimes_start.createPassingTimes(self, iNUTS, row[9],  iAz, row[0], row[3], row[1], row[4], row[2], row[12], iGTFSExplodedFeedFolder, iDb, iVersion)

			cntServiceJourneys=cntServiceJourneys+1

			if (cntServiceJourneys % 50 == 0):
				print("--- processed " + str(cntServiceJourneys) + " out of " + numServiceJourneys + " ServiceJourneys....")

			serviceJourneyNameClean=""

			if(row[6]):
				serviceJourneyName=SupportUtilitiesSubComponent.StringUtilities
				serviceJourneyNameClean=serviceJourneyName.filterOutNotMultilingualChars(self, row[6])

			if isinstance(row[10], type(None)):			
				chunkText = """
					<ServiceJourney id="%s:ServiceJourney:%s%s:%s_%s_%s_%s" version="%s"> 
							<Name>%s</Name> 
							<TransportMode>%s</TransportMode>
							<DepartureTime>%s</DepartureTime> 
							<DepartureDayOffset>%s</DepartureDayOffset>
							<JourneyDuration>%s</JourneyDuration> 
							<dayTypes>
								<DayTypeRef ref="%s:%s:DayType:%s" version="%s"/>
							</dayTypes>
							<ServiceJourneyPatternRef ref="%s:ServiceJourneyPattern:%s%s:%s_%s_%s_%s_%s"/> 
							<OperatorRef ref="%s:Operator:%s:%s:%s" version="%s"/> 
							<passingTimes>%s</passingTimes>
						</ServiceJourney>""" % (				
						iNUTS, row[9], iAz, row[0], row[3], row[4], row[2], iVersion,
						serviceJourneyNameClean,	
						row[9],								
						row[15],
						row[16], 
						row[8],
						iNUTS, iAz, row[1], iVersion,						 
						iNUTS, row[9], iAz, row[0], row[3], row[4], row[12],
						iNUTS, iVAT, iAz,iAz, iVersion,
						passingTimes)
			else:
				chunkText = """
					<ServiceJourney id="%s:VehicleJourney:%s%s:%s_%s_%s_%s" version="%s"> 
							<Name>%s</Name> 
							<Distance>%s</Distance>
							<TransportMode>%s</TransportMode>
							<DepartureTime>%s</DepartureTime> 
							<DepartureDayOffset>%s</DepartureDayOffset>							
							<JourneyDuration>%s</JourneyDuration> 
							<dayTypes>
								<DayTypeRef ref="%s:DayType:%s:%s" version="%s"/>
							</dayTypes>
							<ServiceJourneyPatternRef ref="%s:ServiceJourneyPattern:%s%s:%s_%s_%s_%s" version="%s"/> 
							<OperatorRef ref="%s:Operator:%s:%s:%s" version="%s"/> 
							<passingTimes>%s</passingTimes>
						</ServiceJourney>""" % (				
						iNUTS, row[9], iAz, row[0], row[3], row[4], row[2], iVersion,
						serviceJourneyNameClean,									
						row[10],
						row[9],
						row[15],
						row[16], 
						row[8],
						iNUTS, iAz, row[1], iVersion,
						iNUTS, row[9], iAz, row[0], row[3], row[4], row[12], iVersion,
						iNUTS, iVAT, iAz, iAz, iVersion,
						passingTimes)
		
			with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-TimetableFrame-temp.xml', 'a', encoding='utf-8') as f: f.write((chunkText))

		#with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-TimetableFrame-temp.xml', 'w', encoding='utf-8') as f: f.write(opening_text)
		#with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-TimetableFrame-temp.xml', 'a', encoding='utf-8') as f: f.write((outText))
		with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-TimetableFrame-temp.xml', 'a', encoding='utf-8') as f: f.write(closing_text)

		c.close()

