import sqlite3
import os

acroMOT=""
acroOperator="xxx"

class CreatePassingTimesProcessing():

	def createPassingTimes (self, iAcroNUTS, iAcroMOT, iiAz, iRoute_id, iDirection_id, iService_id, iShape_id, iTrip_id, iSpacePattern_id, innerGTFSExplodedFeedFolder, innerDb, iVersion):

		innerConn=sqlite3.connect(innerGTFSExplodedFeedFolder + os.sep +innerDb + '.db')
		iCP=innerConn.cursor()
		sqlStat='''SELECT a.route_id, a.direction_id, a.shape_id, a.service_id, a.trip_id, a.departure_time_extended, 
		               a.arrival_time_extended, a.stop_sequence, substr('00'||CAST(a.stop_sequence AS TEXT),-2) AS stop_seq,
					   b.transport_mode_decoded, a.departure_day_offset, a.arrival_day_offset 
		            FROM 
					 v_routes_trips_stops_stoptimes_1 AS a, tb_stopplaces_extended AS b
					WHERE 
					   a.route_id="%s" 
					   AND a.direction_id="%s" 
					   AND a.shape_id="%s" 
					   AND a.service_id="%s" 
					   AND a.trip_id="%s"
					   AND a.stop_id=b.stop_id 
					ORDER BY stop_sequence;
				''' % (iRoute_id, iDirection_id, iShape_id, iService_id, iTrip_id)
		iCP.execute(sqlStat)

		iRecords=iCP.fetchall()

		outText=""

		for iRow in iRecords:
			outText= outText + """<TimetabledPassingTime id="%s:TimetabledPassingTime:%s%s:%s_%s_%s_%s:passingTimes:%s" version="%s">
									<StopPointInJourneyPatternRef ref="%s:StopPointInJourneyPattern:%s%s:%s_%s_%s_%s_%s" version="%s"/>   
									<ArrivalTime>%s</ArrivalTime>
									<ArrivalDayOffset>%s</ArrivalDayOffset>
									<DepartureTime>%s</DepartureTime>
									<DepartureDayOffset>%s</DepartureDayOffset>									
									</TimetabledPassingTime>""" % (		
									iAcroNUTS, iRow[9], iiAz, iRow[0], iRow[1], iRow[2], iRow[4], iRow[8], iVersion,
									iAcroNUTS, iRow[9], iiAz, iRow[0], iRow[1], iRow[2], iRow[7], iSpacePattern_id, iVersion,									
									iRow[6],
									iRow[11],
									iRow[5],
									iRow[10]
									)	

		return outText

		iCP.close()


