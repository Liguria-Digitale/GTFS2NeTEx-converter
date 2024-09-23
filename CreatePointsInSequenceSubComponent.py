import sqlite3
import os
import SupportUtilitiesSubComponent

acroMOT=""

class CreatePointsInSequenceProcessing():

	def createPointsInSequence(self, iAcroNUTS, iAcroMOT, iAcroAzienda, iRoute_id, iDirection_id, iSpacePattern_id, iShape_id, innerGTFSExplodedFeedFolder, innerDb, iVersion):		
		# print(iAcroMOT, iAcroAzienda, iRoute_id, iDirection_id, iShape_id)
		#print("In classe...")
		innerConn=sqlite3.connect(innerGTFSExplodedFeedFolder + os.sep +innerDb + '.db')
		iC=innerConn.cursor()

		sqlStat='''SELECT DISTINCT a.route_id, a.direction_id, a.stop_sequence, a.stop_id, a.shape_id, a.shape_id_partial, a.route_type_decoded, 
		                a.shape_dist_traveled_next, a.space_patt, a.pickup_type, a.drop_off_type,
						b.transport_mode_decoded
					FROM t_trips_stoptimes_shapes_2_SPATT AS a, tb_stopplaces_extended AS b
					WHERE 
					   a.route_id="%s" AND a.direction_id="%s" AND a.space_patt="%s" AND a.shape_id="%s" 
					   AND
					   a.stop_id=b.stop_id 
					   AND
					   a.shape_dist_traveled_next IS NOT NULL
					ORDER BY a.stop_sequence;''' % (iRoute_id, iDirection_id, iSpacePattern_id, iShape_id)						
					
		#print(sqlStat)
		iC.execute(sqlStat)

		iRecords=iC.fetchall()
	#print("Stops.txt => total rows are:  ", len(records))

		outText=""

		for iRow in iRecords:

			idStopClean=""

			if(iRow[10]=="0"): # drop_off_type
				isForAlighting="true"
			else:
				isForAlighting="false"

			if(iRow[9]=="0"): # pickup_type
				isForBoarding="true"
			else:
				isForBoarding="false"				

			if(iRow[2]==1 and iRow[7]!=0.0): # stop_sequence - shape_dist_traveled
				isForAlighting="false"
				isForBoarding="true"
			elif(iRow[2]!=1 and iRow[7]==0.0): # stop_sequence - shape_dist_traveled
				isForAlighting="true"
				isForBoarding="false"

			if(iRow[3]):
				idStop=SupportUtilitiesSubComponent.StringUtilities
				idStopClean=idStop.filterOutNotMultilingualChars(self, iRow[3])

			if(iRow[7]!=0.0): # shape_dist_traveled
			#print(row[0], " - ", row[1], " - ", row[2], " - ", row[3], " - ", row[4], " - ", row[5], row[6], " - ", row[7])
			#print(iAcroMOT, iAcroAzienda, iRow[0], iRow[1], iRow[4], iRow[5], iRow[2], iRow[2], iAcroMOT, iAcroAzienda, iRow[3], iAcroMOT, iAcroAzienda, iRow[0], iRow[1], iRow[4])
				outText= outText + """<StopPointInJourneyPattern id="%s:StopPointInJourneyPattern:%s%s:%s_%s_%s_%s_%s" order="%s" version="%s">  
						<ScheduledStopPointRef ref="%s:ScheduledStopPoint:%s%s:%s" version="%s"/> 
						<OnwardServiceLinkRef ref="%s:ServiceLink:%s%s:%s_%s_%s" version="%s"/> 
						<ForAlighting>%s</ForAlighting>
						<ForBoarding>%s</ForBoarding>
					</StopPointInJourneyPattern>""" % (		
					iAcroNUTS, iRow[11], iAcroAzienda, iRow[0], iRow[1], iRow[4], iRow[2], iRow[8], iRow[2], iVersion,					
					iAcroNUTS, iRow[11], iAcroAzienda, idStopClean, iVersion,
					iAcroNUTS, iRow[6], iAcroAzienda, iRow[0], iRow[1], iRow[5], iVersion,					
					isForAlighting,
					isForBoarding)	
			else:
				outText= outText + """<StopPointInJourneyPattern id="%s:StopPointInJourneyPattern:%s%s:%s_%s_%s_%s_%s" order="%s" version="%s">  
						<ScheduledStopPointRef ref="%s:ScheduledStopPoint:%s%s:%s" version="%s"/> 
						<ForAlighting>%s</ForAlighting>
						<ForBoarding>%s</ForBoarding>
					</StopPointInJourneyPattern>""" % (		
					iAcroNUTS, iRow[11], iAcroAzienda, iRow[0], iRow[1], iRow[4], iRow[2], iRow[8], iRow[2], iVersion,
					iAcroNUTS, iRow[11], iAcroAzienda, idStopClean, iVersion,					
					isForAlighting,
					isForBoarding)	
	
#		iC.close()
		#print("OUTTEXT-pts..." + outText)
		return outText

		iC.close()

