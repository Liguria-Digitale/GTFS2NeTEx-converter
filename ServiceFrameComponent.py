import sqlite3
import os
#import codecs
import CreatePointsInSequenceSubComponent
import SupportUtilitiesSubComponent

acroMOT=""

class ServiceFrameProcessing():

	def processServiceFrame(self, iGTFSExplodedFeedFolder, iNUTS, iDb, iAz, iVersion, iVAT):
		print('===> Processing ServiceFrame...')

		acroVehicleType="it:"+acroMOT+iAz+":VehicleType:"+"001"+iAz

		conn=sqlite3.connect(iGTFSExplodedFeedFolder + os.sep +iDb + '.db')
		c=conn.cursor()

		serviceFrame_opening_text="""<ServiceFrame id="epd:"""+iNUTS+""":ServiceFrame_EU_PI_NETWORK:it" version="1"><TypeOfFrameRef ref="epip:EU_PI_NETWORK" versionRef="1"/>"""
		serviceFrame_closing_text="""</ServiceFrame>"""

		outRoutes_opening_text='''<routes>'''
		outRoutes_closing_text='''</routes>'''

		outLines_opening_text='''<lines>'''
		outLines_closing_text='''</lines>'''

		outScheduledStopPoints_opening_text='''<scheduledStopPoints>'''
		outScheduledStopPoints_closing_text='''</scheduledStopPoints>'''

		outStopAssignments_opening_text='''<stopAssignments>'''
		outStopAssignments_closing_text='''</stopAssignments>'''		

		outServiceLinks_opening_text='''<serviceLinks>'''
		outServiceLinks_closing_text='''</serviceLinks>'''

		outJourneyPatterns_opening_text='''<journeyPatterns>'''
		outJourneyPatterns_closing_text='''</journeyPatterns>'''

		# ROUTES SECTION
		c.execute('''SELECT route_id, agency_id, route_short_name, route_long_name, route_url, route_type_decoded FROM tb_routes''')
		records=c.fetchall()

		outRoutesText=""

		for row in records:	

			nameRouteClean=""

			if(row[3]):
				nameRoute=SupportUtilitiesSubComponent.StringUtilities
				nameRouteClean=nameRoute.filterOutNotMultilingualChars(self, row[3])

			outRoutesText= outRoutesText + """<Route id="%s:Route:%s%s:%s" version="%s"> 
										<Name>%s</Name> 
									</Route>""" % (	
		######## <Route id> <===	route_id row[0]										
			iNUTS, row[5], iAz, row[0], iVersion,
			nameRouteClean)
								

		# LINES SECTION
		c.execute('''SELECT route_id, agency_id, route_short_name, route_long_name, route_url, IIF(route_type_decoded="lift", "unknown", route_type_decoded) FROM tb_routes''')

		records=c.fetchall()

		outLinesText=""

		for row in records:

			nameLineClean=""
			if(row[3]):
				nameLine=SupportUtilitiesSubComponent.StringUtilities
				nameLineClean=nameLine.filterOutNotMultilingualChars(self, row[3])			

			outLinesText= outLinesText + """<Line id="%s:Line:%s%s:%s" version="%s"> 
										<Name>%s</Name> 
										<ShortName>%s</ShortName> 
										<Description></Description>
										<TransportMode>%s</TransportMode>
										<Url>%s</Url> 
										<PublicCode>%s</PublicCode>
										<PrivateCode>%s</PrivateCode>
										<OperatorRef ref="%s:Operator:%s:%s:%s" version="%s"/>
										<Monitored>false</Monitored>
									</Line>""" % (	
		######## <Line id> <===	route_id row[0]										
		######## <Name> <=== route_long_name=row[3]
		######## <ShortName> <=== route_short_name=row[2]
		######## <Url> <=== route_url=row[5] 
		######## <PublicCode> <=== route_short_name=row[2]
		######## <PrivateCode> <=== route_short_name=row[2]
		######## <OperatorRef> <=== AUTOMATIZZARE
			iNUTS, row[5], iAz, row[0], iVersion,
			nameLineClean,
			row[2],
			row[5],
			row[4], 
			row[2],
			row[0],
			iNUTS, iVAT, iAz, iAz, iVersion)								

		# SCHEDULED STOP POINTS SECTION
		c.execute('''SELECT stop_id, stop_name, stop_lat, stop_lon,
                     IIF(transport_mode_decoded IS NULL, "other", transport_mode_decoded)
                     FROM tb_stopplaces_extended;''')
		records=c.fetchall()

		outScheduledStopPointsText=""

		for row in records:

			idStopClean=""

			if(row[0]):
				idStop=SupportUtilitiesSubComponent.StringUtilities
				idStopClean=idStop.filterOutNotMultilingualChars(self, row[0])			

			nameStopClean=""

			if(row[1]):
				nameStop=SupportUtilitiesSubComponent.StringUtilities
				nameStopClean=nameStop.filterOutNotMultilingualChars(self, row[1])			

			outScheduledStopPointsText= outScheduledStopPointsText + """<ScheduledStopPoint id="%s:ScheduledStopPoint:%s%s:%s" version="%s">  
										<Name>%s</Name>  
										<Location>
											<Longitude>%s</Longitude> 
											<Latitude>%s</Latitude> 
											<Altitude>0</Altitude> 
											<Precision>6</Precision>
										</Location>
										<ShortName>%s</ShortName> 
										<Description>%s</Description> 
										<PublicCode>%s</PublicCode> 
										<PrivateCode>%s</PrivateCode> 
									</ScheduledStopPoint>""" % (	

			######## <ScheduledStopPoint id> <===	stop_id row[0]										
			######## <Name> <=== stop_name=row[1]
			######## <Longitude> <=== stop_lon=row[4]
			######## <Latitude> <=== stop_lon=row[3]
			######## <gml pos> <=== row[3] row[4]
			######## <ShortName> <=== stop_name=row[1]
			######## <Description> <=== stop_name=row[1]
			######## <PublicCode> <=== stop_id row[0]
			######## <PrivateCode> <=== stop_id row[0]
			######## <StopType> <=== AUTOMATIZZARE
				iNUTS, row[4], iAz, idStopClean, iVersion,
				nameStopClean,
				row[3],
				row[2],
				nameStopClean,
				nameStopClean,
				idStopClean,
				idStopClean)										 

################################################

		# STOP ASSIGNMENTS SECTION
		c.execute('''SELECT stop_id, stop_name, stop_lat, stop_lon,
                     IIF(transport_mode_decoded IS NULL, "other", transport_mode_decoded)
                     FROM tb_stopplaces_extended;''')
		records=c.fetchall()

		outStopAssignmentsText=""

		for row in records:

			idStopClean=""

			if(row[0]):
				idStop=SupportUtilitiesSubComponent.StringUtilities
				idStopClean=idStop.filterOutNotMultilingualChars(self, row[0])			

			nameStopClean=""

			if(row[1]):
				nameStop=SupportUtilitiesSubComponent.StringUtilities
				nameStopClean=nameStop.filterOutNotMultilingualChars(self, row[1])			

			outStopAssignmentsText= outStopAssignmentsText + """<PassengerStopAssignment order="1" id="%s:ScheduledStopPoint:%s%s:%s" version="%s">  
										<ScheduledStopPointRef ref="%s:ScheduledStopPoint:%s%s:%s" version="%s" />
										<StopPlaceRef ref="%s:StopPlace:%s%s:%s" version="%s" />
										<QuayRef ref="%s:Quay:%s%s:%s" version="%s" />										
									</PassengerStopAssignment>""" % (	
				iNUTS, row[4], iAz, idStopClean, iVersion,
				iNUTS, row[4], iAz, idStopClean, iVersion,
				iNUTS, row[4], iAz, idStopClean, iVersion,
				iNUTS, row[4], iAz, idStopClean, iVersion)

##################################################

		# SERVICE LINKS SECTION
	
		c.execute('''SELECT 
						a.route_id, a.route_type_decoded, a.direction_id, a.shape_id_partial, 
						CAST(a.delta AS INTEGER) AS link_length, a.gml_field_partial, a.stop_id, a.stop_id_next, b.transport_mode_decoded, c.transport_mode_decoded
                     FROM 
					    v_trips_stoptimes_shapes_4_NEW AS a,
						 tb_stopplaces_extended AS b,
						 tb_stopplaces_extended AS c					 
                     WHERE 
					    link_length >= 0
						 AND
						 a.stop_id=b.stop_id
						 AND
						 a.stop_id_next=c.stop_id''')							 				
		records=c.fetchall()

		numServiceLinks=str(len(records))

		outServiceLinksText=""
		cntServiceLinks=0

		for row in records:
			cntServiceLinks=cntServiceLinks+1

			idFromStopClean=""

			if(row[6]):
				idFromStop=SupportUtilitiesSubComponent.StringUtilities
				idFromStopClean=idFromStop.filterOutNotMultilingualChars(self, row[6])

			idToStopClean=""

			if(row[7]):
				idToStop=SupportUtilitiesSubComponent.StringUtilities
				idToStopClean=idToStop.filterOutNotMultilingualChars(self, row[7])				

			if (cntServiceLinks % 100 == 0):
				print("--- processed " + str(cntServiceLinks) + " out of " + numServiceLinks + " ServiceLinks....")	

			if isinstance(row[4], type(None)):
				outServiceLinksText= outServiceLinksText + """<ServiceLink id="%s:ServiceLink:%s%s:%s_%s_%s" version="%s"> 
							<gml:LineString gml:id="%s"> 
								<gml:posList>%s</gml:posList> 
							</gml:LineString>
							<FromPointRef ref="%s:ScheduledStopPoint:%s%s:%s" version="%s"/>  
							<ToPointRef ref="%s:ScheduledStopPoint:%s%s:%s" version="%s"/> 
						</ServiceLink>""" % (	
			######## COMPLETE ARGS DESCRIPTION
				iNUTS, row[1], iAz, row[0],row[2], row[3], iVersion,
				row[3], 
				row[5], 
				iNUTS, row[8], iAz, idFromStopClean, iVersion,
				iNUTS, row[9], iAz, idToStopClean, iVersion)	  
			else:
				outServiceLinksText= outServiceLinksText + """<ServiceLink id="%s:ServiceLink:%s%s:%s_%s_%s" version="%s"> 
							<Distance>%s</Distance>
							<gml:LineString gml:id="%s"> 
								<gml:posList>%s</gml:posList> 
							</gml:LineString>
							<FromPointRef ref="%s:ScheduledStopPoint:%s%s:%s" version="%s"/>  
							<ToPointRef ref="%s:ScheduledStopPoint:%s%s:%s" version="%s"/> 
						</ServiceLink>""" % (	
			######## COMPLETE ARGS DESCRIPTION
				iNUTS, row[1], iAz, row[0],row[2], row[3], iVersion,
				row[4], 
				row[3], 
				row[5], 
				iNUTS, row[8], iAz, idFromStopClean, iVersion,
				iNUTS, row[9], iAz, idToStopClean, iVersion)

		print("ServiceLink phase passed...")
		print("Now starting ServiceJourneyPattern phase...")
		# JOURNEY PATTERNS SECTION

		# c.execute('''SELECT DISTINCT
        #                IIF(a.route_type_decoded="lift", "unknown", a.route_type_decoded), a.route_id, a.direction_id, a.shape_id, a.route_long_name,
        #               (SELECT CAST(b.d_distance AS INTEGER) FROM v_shapes_gml_with_distance AS b WHERE b.d_shape_id=a.shape_id) AS distance,
        #                a.space_patt
        #              FROM t_trips_stoptimes_shapes_2_SPATT AS a
        #              GROUP BY a.route_id, a.direction_id, a.shape_id, a.space_patt, distance
        #              ORDER BY a.route_id, a.direction_id''')			

		c.execute('''SELECT 
                       route_type_decoded, route_id, direction_id, shape_id, route_long_name, distance, space_patt
                     FROM t_trips_stoptimes_shapes_3_SPATT''')					 		 
					 # row[0]: route_type_decoded
					 # row[1]: route_id
					 # row[2]: direction_id
					 # row[3]: shape_id
					 # row[4]: route_long_name
					 # row[5]: distance
					 # row[6]: space_patt		
					 # IIF(a.route_type_decoded="lift", "unknown", a.route_type_decoded)												
		records=c.fetchall()

		numJourneyPatterns=str(len(records))
		print("Found " + numJourneyPatterns + " JourneyPatterns...")
		outJourneyPatternsText=""
		cntJourneyPatterns=0

		# external loop (master service journey)
		for row in records:
			pointsSequence_start=CreatePointsInSequenceSubComponent.CreatePointsInSequenceProcessing
			cntJourneyPatterns=cntJourneyPatterns+1

			nameRouteLongNameClean=""

			if (row[4]):
				nameRouteLongName=SupportUtilitiesSubComponent.StringUtilities
				nameRouteLongNameClean=nameRouteLongName.filterOutNotMultilingualChars(self, row[4])			

			if (cntJourneyPatterns % 50 == 0):
				print("--- processed " + str(cntJourneyPatterns) + " out of " + numJourneyPatterns + " JourneyPatterns....")				
			
			#trip_id is not passed
			pointSequence=pointsSequence_start.createPointsInSequence(self, iNUTS, acroMOT, iAz, row[1], row[2], row[6], row[3], iGTFSExplodedFeedFolder, iDb, iVersion)

			#print("TYPE ROW[9] is: " + str(type(row[9])))
			if isinstance(row[5], type(None)):
				outJourneyPatternsText= outJourneyPatternsText + """<ServiceJourneyPattern id="%s:ServiceJourneyPattern:%s%s:%s_%s_%s_%s" version="%s">
							<Name>%s</Name> 
							<RouteView>
								<LineRef ref="%s:Line:%s%s:%s" version="%s"/> 
							</RouteView>
							</ServiceJourneyPattern>""" % (	
				iNUTS, row[0], iAz, row[1], row[2], row[3], row[6], iVersion,
				nameRouteLongNameClean,		
				iNUTS, row[0], iAz, row[1], iVersion)				
			else:
				outJourneyPatternsText= outJourneyPatternsText + """<ServiceJourneyPattern id="%s:ServiceJourneyPattern:%s%s:%s_%s_%s_%s" version="%s">
							<Name>%s</Name> 
							<Distance>%s</Distance>							
							<RouteView>
								<LineRef ref="%s:Line:%s%s:%s" version="%s"/> 
							</RouteView>
							<pointsInSequence>%s</pointsInSequence>
							</ServiceJourneyPattern>""" % (	
				iNUTS, row[0], iAz, row[1], row[2], row[3], row[6], iVersion,
				nameRouteLongNameClean,
				row[5],		
				iNUTS, row[0], iAz, row[1], iVersion, 
				pointSequence)

		with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-ServiceFrame-temp.xml', 'w', encoding='utf-8') as f: f.write(serviceFrame_opening_text)
		# routes
		with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-ServiceFrame-temp.xml', 'a', encoding='utf-8') as f: f.write(outRoutes_opening_text)
		with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-ServiceFrame-temp.xml', 'a', encoding='utf-8') as f: f.write(outRoutesText)
		with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-ServiceFrame-temp.xml', 'a', encoding='utf-8') as f: f.write(outRoutes_closing_text)
		# lines
		with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-ServiceFrame-temp.xml', 'a', encoding='utf-8') as f: f.write(outLines_opening_text)
		with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-ServiceFrame-temp.xml', 'a', encoding='utf-8') as f: f.write(outLinesText)
		with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-ServiceFrame-temp.xml', 'a', encoding='utf-8') as f: f.write(outLines_closing_text)
		# scheduledStopPoints
		with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-ServiceFrame-temp.xml', 'a', encoding='utf-8') as f: f.write(outScheduledStopPoints_opening_text)
		with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-ServiceFrame-temp.xml', 'a', encoding='utf-8') as f: f.write(outScheduledStopPointsText)
		with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-ServiceFrame-temp.xml', 'a', encoding='utf-8') as f: f.write(outScheduledStopPoints_closing_text)
		# serviceLinks
		with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-ServiceFrame-temp.xml', 'a', encoding='utf-8') as f: f.write(outServiceLinks_opening_text)
		with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-ServiceFrame-temp.xml', 'a', encoding='utf-8') as f: f.write(outServiceLinksText)
		with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-ServiceFrame-temp.xml', 'a', encoding='utf-8') as f: f.write(outServiceLinks_closing_text)

		with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-ServiceFrame-temp.xml', 'a', encoding='utf-8') as f: f.write(outStopAssignments_opening_text)
		with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-ServiceFrame-temp.xml', 'a', encoding='utf-8') as f: f.write(outStopAssignmentsText)
		with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-ServiceFrame-temp.xml', 'a', encoding='utf-8') as f: f.write(outStopAssignments_closing_text)

		# journeyPatterns
		with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-ServiceFrame-temp.xml', 'a', encoding='utf-8') as f: f.write(outJourneyPatterns_opening_text)
		with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-ServiceFrame-temp.xml', 'a', encoding='utf-8') as f: f.write(outJourneyPatternsText)
		with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-ServiceFrame-temp.xml', 'a', encoding='utf-8') as f: f.write(outJourneyPatterns_closing_text)

		with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-ServiceFrame-temp.xml', 'a', encoding='utf-8') as f: f.write(serviceFrame_closing_text)
