import sqlite3
import os
import SupportUtilitiesSubComponent

acroMOT=""

class ServiceCalendarFrameProcessing():

	def processServiceCalendarFrame(self, iGTFSExplodedFeedFolder, iNUTS, iDb, iAz, iStartDay, iEndDay, iStartDayCut, iEndDayCut, iVersion):
		print('===> Processing ServiceCalendarFrame...')

		conn=sqlite3.connect(iGTFSExplodedFeedFolder + os.sep +iDb + '.db')
		c=conn.cursor()

		# 
		c.execute('''SELECT service_id, date_start_min, date_end_max, daysOfWeek, validDayBits FROM srv_calendar_dates_summary''')
		records=c.fetchall()
		#print("Stops.txt => total rows are:  ", len(records))

		opening_text="""<ServiceCalendarFrame id="epd:%s:ServiceCalendarFrame_EU_PI_CALENDAR:ita" version="1"><TypeOfFrameRef ref="epip:EU_PI_CALENDAR" versionRef="1"/>""" % (iNUTS)
		serviceCalendar_opening_text="""<ServiceCalendar id="it:ServiceCalendar:C01" version="%s"> 
							""" % (iVersion) # da personalizzare su Azienda???
		
		fmtStartDayCut=""
		fmtStartDayCut_0=SupportUtilitiesSubComponent.StringUtilities
		fmtStartDayCut=fmtStartDayCut_0.formatShortDay2UTC(self, iStartDayCut)

		fmtEndDayCut=""
		fmtEndDayCut_0=SupportUtilitiesSubComponent.StringUtilities
		fmtEndDayCut=fmtEndDayCut_0.formatShortDay2UTC(self, iEndDayCut)

		serviceCalendar_mid_1_text="""<Name>Calendario ordinario</Name>
								<FromDate>%s</FromDate>
								<ToDate>%s</ToDate>
							""" % (fmtStartDayCut, fmtEndDayCut)
		dayTypes_opening_text="""<dayTypes>"""		
		dayTypes_closing_text="""</dayTypes>"""	

		operatingPeriods_opening_text="""<operatingPeriods>"""		
		operatingPeriods_closing_text="""</operatingPeriods>"""	

		dayTypeAssignments_opening_text="""<dayTypeAssignments>"""		
		dayTypeAssignments_closing_text="""</dayTypeAssignments>"""	

		serviceCalendar_closing_text="""</ServiceCalendar>"""
		closing_text="""</ServiceCalendarFrame>"""

		outDayTypesText=""

		# dayTypes section
		for row in records:

			outDayTypesText= outDayTypesText + """<DayType id="%s:DayType:%s%s:%s" version="%s">
										<Name>%s</Name>
										<Description>%s</Description>
										<properties>
											<PropertyOfDay>
												<DaysOfWeek>%s</DaysOfWeek>
												<HolidayTypes></HolidayTypes>
											</PropertyOfDay>
										</properties>
									</DayType>""" % (				
		######## <DayType id>, <DayType><Name>, <DayType><Description> <=== service_id=row[0]
		######## <DayType><properties><PropertyOfDay><DaysOfWeek> <=== daysOfWeek=row[3]
			iNUTS, acroMOT, iAz, row[0], iVersion,
			row[0], 
			row[0], 
			row[3])

		c.execute('''SELECT service_id, date_start_min, date_end_max, daysOfWeek, validDayBits FROM srv_calendar_dates_summary''')
		recordsOperatingPeriods=c.fetchall()

		outOperatingPeriodsText=""
		
		for row in recordsOperatingPeriods:

			longValidDayBits=""
			cutStartDay=row[1][0:4] + "-" + row[1][4:6] + "-" + row[1][6:8] 
			cutEndDay=row[2][0:4] + "-" + row[2][4:6] + "-" + row[2][6:8] 	
			longValidDayBits=row[4]
			cutValidDayBits=longValidDayBits[longValidDayBits.index('1'):longValidDayBits.rfind('1')+1]

			outOperatingPeriodsText=outOperatingPeriodsText + """<UicOperatingPeriod id="%s:UicOperatingPeriod:%s%s:%s" version="%s">
										<FromDate>%sT00:00:00</FromDate> 
										<ToDate>%sT23:59:59</ToDate>
										<ValidDayBits>%s</ValidDayBits>
									</UicOperatingPeriod>""" % (	
		######## <UicOperatingPeriod id> <=== service_id=row[0]
		######## <UicOperatingPeriod><ValidDaysBits> <=== validDaysBits=row[4]
			iNUTS, acroMOT, iAz, row[0], iVersion,
			# iStartDay, 
			# iEndDay, 
			# row[4])	 
			cutStartDay, 
			cutEndDay, 
			cutValidDayBits)				

		c.execute('''SELECT service_id, date_start_min, date_end_max, daysOfWeek, validDayBits FROM srv_calendar_dates_summary''')
		recordsDayTypeAssignment=c.fetchall()

		outDayTypeAssignmentsText=""
		#for row in recordsOperatingPeriods:
		for row in recordsDayTypeAssignment:			
				outDayTypeAssignmentsText= outDayTypeAssignmentsText + """<DayTypeAssignment version="%s" order="1" id="%s:DayTypeAssignment:%s%s:2022-01-01_%s">
										<OperatingPeriodRef ref="%s:UicOperatingPeriod:%s%s:%s" version="%s"/>
										<DayTypeRef version="%s" ref="%s:DayType:%s%s:%s"/>
									</DayTypeAssignment>""" % (				
		######## <DayTypeAssignment id>, <OperatingPeriodRef>, <DayTypeRef> <=== service_id=row[0]
			iVersion, iNUTS, acroMOT, iAz, row[0], 
			iNUTS, acroMOT, iAz, row[0], iVersion,
			iVersion, iNUTS, acroMOT, iAz, row[0])	 

		with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-ServiceCalendarFrame-temp.xml', 'w', encoding='utf-8') as f: f.write(opening_text)
		# dayTypes section
		with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-ServiceCalendarFrame-temp.xml', 'a', encoding='utf-8') as f: f.write(serviceCalendar_opening_text)
		with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-ServiceCalendarFrame-temp.xml', 'a', encoding='utf-8') as f: f.write(serviceCalendar_mid_1_text)
		with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-ServiceCalendarFrame-temp.xml', 'a', encoding='utf-8') as f: f.write(dayTypes_opening_text)
		with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-ServiceCalendarFrame-temp.xml', 'a', encoding='utf-8') as f: f.write(outDayTypesText)
		with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-ServiceCalendarFrame-temp.xml', 'a', encoding='utf-8') as f: f.write(dayTypes_closing_text)

		# operatingPeriod section
		with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-ServiceCalendarFrame-temp.xml', 'a', encoding='utf-8') as f: f.write(operatingPeriods_opening_text)
		with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-ServiceCalendarFrame-temp.xml', 'a', encoding='utf-8') as f: f.write(outOperatingPeriodsText)
		with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-ServiceCalendarFrame-temp.xml', 'a', encoding='utf-8') as f: f.write(operatingPeriods_closing_text)

		# dayTypeAssignment section
		with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-ServiceCalendarFrame-temp.xml', 'a', encoding='utf-8') as f: f.write(dayTypeAssignments_opening_text)
		with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-ServiceCalendarFrame-temp.xml', 'a', encoding='utf-8') as f: f.write(outDayTypeAssignmentsText)
		with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-ServiceCalendarFrame-temp.xml', 'a', encoding='utf-8') as f: f.write(dayTypeAssignments_closing_text)

		with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-ServiceCalendarFrame-temp.xml', 'a', encoding='utf-8') as f: f.write(serviceCalendar_closing_text)
		with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-ServiceCalendarFrame-temp.xml', 'a', encoding='utf-8') as f: f.write(closing_text)

		c.close()
