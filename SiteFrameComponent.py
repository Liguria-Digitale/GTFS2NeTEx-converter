import sqlite3
import os
import SupportUtilitiesSubComponent

acroMOT=""

class SiteFrameProcessing():

	def processSiteFrame(self, iGTFSExplodedFeedFolder, iNUTS, iDb, iAz, iVersion):
		print('===> Processing SiteFrame...')

		conn=sqlite3.connect(iGTFSExplodedFeedFolder + '/' +iDb + '.db')
		c=conn.cursor()

		c.execute('''SELECT stop_id, stop_name, stop_lat, stop_lon,
                            IIF(transport_mode_decoded IS NULL, "other", transport_mode_decoded),
                            IIF(stop_place_type_decoded IS NULL, "other", stop_place_type_decoded),
							CASE transport_mode_decoded
								WHEN 'bus' THEN 'busPlatform'
								WHEN 'rail' THEN 'railPlatform'
							    WHEN 'metro' THEN 'metroPlatform'							
								WHEN 'ferry' THEN 'ferryLanding'
								ELSE 'other'
							END AS quay_type							
                     FROM tb_stopplaces_extended;''')
		records=c.fetchall()

		numStops=str(len(records))
		print("Stops.txt => total rows are:  ", len(records))
		#print("Printing each row")

		opening_text="""<SiteFrame id="epd:it:SiteFrame_EU_PI_STOP:it" version="1"><TypeOfFrameRef ref="epip:EU_PI_STOP" versionRef="1"/><stopPlaces>"""
		closing_text="""</stopPlaces></SiteFrame>"""

		outText=""
		cntStops=0

		for row in records:
			cntStops=cntStops+1
			if (cntStops % 10 == 0):
				print("--- processed " + str(cntStops) + " out of " + numStops + " stops....")	

			idStopClean=""

			if(row[0]):
				idStop=SupportUtilitiesSubComponent.StringUtilities
				idStopClean=idStop.filterOutNotMultilingualChars(self, row[0])
			
			nameStopClean=""

			if(row[1]):
				nameStop=SupportUtilitiesSubComponent.StringUtilities
				nameStopClean=nameStop.filterOutNotMultilingualChars(self, row[1])

			outText= outText + """
					<StopPlace id="%s:StopPlace:%s%s:%s" version="%s">
						<Name>%s</Name>
						<ShortName></ShortName>
						<PrivateCode>%s</PrivateCode>
						<Centroid>
							<Location>
								<Longitude>%s</Longitude>
								<Latitude>%s</Latitude>								
							</Location>
						</Centroid>
						<AccessModes>foot</AccessModes>
						<levels>
							<Level id="%s:Level:%s%s:%s_Lvl_G0" version="%s">
								<Name>Ground </Name>
								<PublicCode>G</PublicCode>
							</Level>
						</levels>
						<PublicCode></PublicCode>
						<TransportMode>%s</TransportMode>
						<StopPlaceType>%s</StopPlaceType>
						<quays>
							<Quay id="%s:Quay:%s%s:%s" version="%s">
								<Name>%s</Name>
								<Centroid>
									<Location>
										<Longitude>%s</Longitude>
										<Latitude>%s</Latitude>	
									</Location>
								</Centroid>
								<LevelRef ref="%s:Level:%s%s:%s_Lvl_G0" version="%s"/>
								<QuayType>%s</QuayType>
							</Quay>
						</quays>
					</StopPlace>""" % (				
		######## <StopPlace id>, <PrivateCode> <=== stop_id=row[0]
		######## <Name> <=== stop_name=row[2]
		######## <ShortName> <=== ???
		######## <PrivateCode> <=== stop_name=row[0] VERIFICARE
		######## <???> <=== stop_desc=row[3]
		######## <gml:pos> <=== stop_lat=row[4]
		######## <gml:pos> <=== stop_lon=row[5]
		######## <???> <=== zone_id=row[6]
		######## <AccessModes> <=== ???
		######## <PublicCode> <=== ???
		######## <TransportMode> <=== ???
		######## <StopPlaceType> <=== f(location_type_id=row[7]) VERIFICARE
		######## <???> <=== parent_station=row[8]
			iNUTS, row[4], iAz, idStopClean, iVersion,
			nameStopClean, 
			idStopClean, 
			row[3], 
			row[2],

			iNUTS, row[4], iAz, idStopClean, iVersion,

			row[4],
			row[5],

			iNUTS, row[4], iAz, idStopClean, iVersion,
			nameStopClean,
			row[3], 
			row[2],		
			iNUTS, row[4], iAz, idStopClean, iVersion,
			row[6]
			)

		with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-SiteFrame-temp.xml', 'w', encoding='utf-8') as f: f.write((opening_text))
		with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-SiteFrame-temp.xml', 'a', encoding='utf-8') as f: f.write((outText))
		with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-SiteFrame-temp.xml', 'a', encoding='utf-8') as f: f.write((closing_text))

		c.close()

