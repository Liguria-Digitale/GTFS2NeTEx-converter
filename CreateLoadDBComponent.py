from pathlib import Path
import sys
import sqlite3
import pandas as pd
import os
from math import radians, cos, sin, asin, sqrt
from haversine import haversine, Unit

class StartImportProcess():

  def calculateHaversineDistanceForShapes(self, innerGTFSExplodedFeedFolder, innerDb):
    innerConn=sqlite3.connect(innerGTFSExplodedFeedFolder + '/' + innerDb + '.db')
    
    iCP=innerConn.cursor()
    
    iCP.execute('''CREATE TABLE tb_shapes_temp 
                AS 
                SELECT
                  shape_id,
                  shape_pt_sequence,
                  shape_pt_lat, shape_pt_lon,
                  IIF(shape_pt_sequence != 1, LAG (shape_pt_lat,1,0) OVER (ORDER BY shape_id, shape_pt_sequence), '0') AS prev_lat,
                  IIF(shape_pt_sequence != 1, LAG (shape_pt_lon,1,0) OVER (ORDER BY shape_id, shape_pt_sequence), '0') AS prev_lon
                FROM tb_shapes
                ORDER BY shape_id, shape_pt_sequence 
                ''')  

    innerConn.commit()    
    
    iCP.execute('''ALTER TABLE tb_shapes ADD COLUMN partialDistance REAL DEFAULT 0.0''')
    innerConn.commit()

    ############## BEGIN calculate partial distances between shape points
    iCP.execute('''SELECT shape_id, shape_pt_sequence, shape_pt_lat, shape_pt_lon, prev_lat, prev_lon FROM tb_shapes_temp WHERE shape_pt_sequence != 1''')
    data=iCP.fetchall()
    totalShapes=len(data)
    print(">>>>> Total temporary shapes is:  ", str(totalShapes))
    shapeCounter=0
    for row in data:
      dist_traveled=""
      current_shape_id=row[0]
      current_shape_pt_sequence=row[1]
      current_lat1=float(row[2])
      current_lon1=float(row[3])   
      current_lat2=float(row[4])
      current_lon2=float(row[5])
      if current_shape_pt_sequence != 0:

        # current_lat1, current_lon1, current_lat2, current_lon2=map(radians, [current_lat1, current_lon1, current_lat2, current_lon2])
        # deltalat=current_lat1-current_lat2
        # deltalon=current_lon1-current_lon2
        # a = sin(deltalat/2)**2 + cos(current_lat1) * cos(current_lat2) * sin(deltalon/2)**2
        # c = 2 * asin(sqrt(a)) 
        # r = 6371 # Radius of earth in kilometers. Use 3956 for miles. Determines return value units.
        # current_distance=c * r   
        # current_distance_str=str(current_distance) 

        ############ use Haversine package https://pypi.org/project/haversine/ (pip install haversine)
        pos1=(current_lat1, current_lon1)
        pos2=(current_lat2, current_lon2)
        current_distance=1000.0*haversine(pos1, pos2, unit=Unit.KILOMETERS) # calculate distance in METERS
        #######################################################################################       
        #current_distance_str=str(current_distance*1000)                
##################################################################################################
        sqlStat='''     
                     UPDATE tb_shapes 
                     SET partialDistance='''+str(current_distance)+'''
                     WHERE 
                       shape_id="'''+current_shape_id+'''" AND shape_pt_sequence='''+str(current_shape_pt_sequence)
        #print(sqlStat)
        iCP.execute(sqlStat)
        innerConn.commit()

        shapeCounter=shapeCounter+1
        if (shapeCounter % 100 == 0):
          print("Calculate partial distances for shape "+str(shapeCounter)+" of "+str(totalShapes)+"...")

# populate the empty tb_shapes.shape_dist_traveled column

    sqlStat='''CREATE VIEW v_shapes_temp AS SELECT DISTINCT shape_id, shape_pt_sequence, partialDistance, 
                (SUM(partialDistance) OVER (PARTITION BY shape_id ORDER BY shape_id, shape_pt_sequence)) AS shape_dist_traveled
                FROM tb_shapes  
                GROUP BY shape_id, shape_pt_sequence, partialDistance ORDER BY shape_id, shape_pt_sequence'''
    iCP.execute(sqlStat)   

    sqlStat='''UPDATE tb_shapes
                SET shape_dist_traveled=(
                SELECT shape_dist_traveled FROM v_shapes_temp 
                WHERE 
                  tb_shapes.shape_id=v_shapes_temp.shape_id 
                  AND 
                  tb_shapes.shape_pt_sequence=v_shapes_temp.shape_pt_sequence)'''        

    iCP.execute(sqlStat)
    innerConn.commit()                       

    ############## END calculate partial distances between shape points

#######################################################################################################################################                
  # createSyntheticTbCalendarDates creates an embryonal tb_calendar_dates table popolated with just service_id and date (format 'yyyymmdd', 
  # furtherly update in the implicit isCalendarBased=FALSE branch in importGTFSFiles()) 

  def createSyntheticTbCalendarDates(self, innerGTFSExplodedFeedFolder, innerDb, innerCalendarBased, innerCalendarDatesBased, innerStartDay, innerEndDay):
    print("innerDb: "+innerDb+" --- innerCalendarBased="+str(innerCalendarBased)+" --- innerCalendarDatesBased="+str(innerCalendarDatesBased))
    innerConn=sqlite3.connect(innerGTFSExplodedFeedFolder + os.sep + innerDb + '.db')
    #iCP=innerConn.cursor()
    
    iCP=innerConn.cursor()
    
    iCP.execute('''CREATE TABLE tb_calendar_dates_temp 
                (service_id text,
                date text,
                exception_type text DEFAULT "1",
                monday text,
                tuesday text,
                wednesday text,
                thursday text, 
                friday text,
                saturday text,
                sunday text,
                isDayAvailable text,
                weekDay_string text,  
                weekDay_slot text,                             
                dayUTC text,
                flag_delete text DEFAULT "N")''')  

    # keep a copy of the original tb_calendar_dates table; the new tb_calendar_dates will be created from scratch: tb_calendar_dates_originale data will be used to update just those days
    if innerCalendarDatesBased==True:
      iCP.execute('''ALTER TABLE tb_calendar_dates
                    RENAME TO tb_calendar_dates_original
                ''')
    innerConn.commit()                 

    iCP.execute('''CREATE TABLE tb_calendar_dates
                (service_id text,
                date text,
                exception_type text DEFAULT "1"
                )''')


# external loop 

    iCP.execute('''SELECT service_id, start_date, end_date, monday, tuesday, wednesday, thursday, friday, saturday, sunday FROM tb_calendar''')
    data=iCP.fetchall()
    for row in data:
        service_id=""
        service_id=row[0]
    #    print('service_id=',service_id)  
        start_interval=row[1][0:4]+"-"+row[1][4:6]+"-"+row[1][6:8]
        end_interval=row[2][0:4]+"-"+row[2][4:6]+"-"+row[2][6:8]  
      # print("from "+start_interval+" to:"+end_interval)
        iCP.execute('''     
                    WITH RECURSIVE dates(date) AS 
                    (
                    VALUES("'''+start_interval+'''")
                    UNION ALL
                    SELECT date(date, '+1 day')
                    FROM dates
                    WHERE date < "'''+end_interval+'''"
                  ) 
                  INSERT INTO tb_calendar_dates_temp (service_id, date, monday, tuesday, wednesday, thursday, friday, saturday, sunday)
                  SELECT "''' +row[0]+'''", SUBSTR(date,1,4) || SUBSTR(date,6,2) || SUBSTR(date,9,2), "'''+row[3]+'''",  "'''+row[4]+'''","'''+row[5]+'''","'''+row[6]+'''","'''+row[7]+'''","'''+row[8]+'''","'''+row[9]+'''" FROM dates ORDER BY date;''')

        res=iCP.fetchall()

    innerConn.commit()      
   # print(res)

      ######## update tb_calendar_dates_temp non-CSV derived fields #######

      # update dayUTC
    iCP.execute('''UPDATE tb_calendar_dates_temp 
                      SET dayUTC=strftime('%Y-%m-%dT%H:%M:%S', substr(date,1,4)||"-"||substr(date,5,2)||"-"||substr(date,7,2))
                    ''') 
    innerConn.commit()           
              
              # update isDayAvailable to a NeTEx-ready token
    iCP.execute('''UPDATE tb_calendar_dates_temp 
                    SET isDayAvailable=
                      CASE exception_type 
                          WHEN "1" THEN  "true"
                          ELSE "false" 
                      END
                      ''') 
    innerConn.commit()  

              # update weekDay_slot to a NeTEx-ready token
    iCP.execute('''UPDATE tb_calendar_dates_temp 
                      SET weekDay_slot=strftime('%w',dayUTC) 
                      ''') 
    innerConn.commit()


    iCP.execute('''UPDATE tb_calendar_dates_temp 
                      SET flag_delete=(
                      CASE 
                          WHEN (weekDay_slot="0" AND sunday="0") THEN "Y"
                          WHEN (weekDay_slot="1" AND monday="0") THEN  "Y"
                          WHEN (weekDay_slot="2" AND tuesday="0") THEN  "Y"
                          WHEN (weekDay_slot="3" AND wednesday="0") THEN  "Y"
                          WHEN (weekDay_slot="4" AND thursday="0") THEN  "Y"
                          WHEN (weekDay_slot="5" AND friday="0") THEN  "Y"
                          WHEN (weekDay_slot="6" AND saturday="0") THEN "Y"
                          ELSE "F"
                      END)
                      ''') 
    innerConn.commit()

    # force tb_calendar_dates_temp to match the oroginal tb_calendar_dates_original exceptions
    if innerCalendarDatesBased==True:

####################################################################################################  

      print("PRE-STEP exception_type=1")

      iCP.execute('''UPDATE tb_calendar_dates_temp 
                      SET flag_delete="F" 
                      FROM tb_calendar_dates_original
                      WHERE 
                        tb_calendar_dates_original.service_id=tb_calendar_dates_temp.service_id AND
                        tb_calendar_dates_original.date=tb_calendar_dates_temp.date AND
                        tb_calendar_dates_original.exception_type="1"
                  ''')
      innerConn.commit()

      print("PRE-STEP exception_type=2") 

      iCP.execute('''UPDATE tb_calendar_dates_temp 
                      SET flag_delete="Y" 
                      FROM tb_calendar_dates_original
                      WHERE 
                        tb_calendar_dates_original.service_id=tb_calendar_dates_temp.service_id AND
                        tb_calendar_dates_original.date=tb_calendar_dates_temp.date AND
                        tb_calendar_dates_original.exception_type="2"
                  ''')
      innerConn.commit()

      print("END-STEPS 1-2")
####################################################################################################

    iCP.execute('''DELETE FROM tb_calendar_dates_temp WHERE flag_delete="Y"''')
    innerConn.commit()

    iCP.execute('''INSERT INTO tb_calendar_dates (service_id, date)
                      SELECT service_id, date FROM tb_calendar_dates_temp
                      ''') 
    innerConn.commit()

    iCP.close()

###################################################
#  def  importGTFSFiles(self, iGTFSExplodedFeedFolder, iDb, iStartDay, iEndDay):    
  def  importGTFSFiles(self, iGTFSExplodedFeedFolder, iDb):      
    conn=sqlite3.connect(iGTFSExplodedFeedFolder + os.sep +iDb + '.db')
    c=conn.cursor()

    isCalendarFile=False
    isCalendarDatesFile=False
    isCalendarBased=False
    isCalendarDatesBased=False
    isShapeDistanceTraveledPopulated=False

    # import agency.txt
    if Path(iGTFSExplodedFeedFolder + os.sep + 'agency.txt').is_file():
        print("===> agency.txt exists in " + iGTFSExplodedFeedFolder + ". Importing...")
        c.execute('''CREATE TABLE tb_agency 
                    (agency_id text,
                    agency_name text,
                    agency_url text,
                    agency_timezone text,
                    agency_lang text,
                    agency_phone text,
                    agency_fare_url text,
                    agency_email text)''')

        usecolsAgency=["agency_id","agency_name","agency_url","agency_timezone","agency_lang","agency_phone","agency_fare_url","agency_email"]
        agency=pd.read_csv(iGTFSExplodedFeedFolder + os.sep + 'agency.txt', usecols=lambda x: x in usecolsAgency)
        agency.to_sql('tb_agency', conn, if_exists='append', index=False)
    else:
          print("WARNING: file agency.txt NOT existing in "  + iGTFSExplodedFeedFolder)

    # import stops.txt
    if Path(iGTFSExplodedFeedFolder + os.sep + 'stops.txt').is_file():
        print("===> stops.txt exists in " + iGTFSExplodedFeedFolder + ". Importing...")
        c.execute('''CREATE TABLE tb_stops 
                    (stop_id text,
                    stop_code text,                
                    stop_name text,
                    stop_desc text,
                    stop_lat text,
                    stop_lon text,
                    zone_id text,
                    location_type text,
                    stop_url text,
                    stop_timezone text,
                    wheelchair_boarding text,
                    platform_code text,
                    level_id text,
                    parent_station text)''')

        c.execute('''CREATE INDEX idx_tb_stops ON tb_stops (stop_id)''')

        usecolsStops=["stop_id","stop_code","stop_name","stop_desc","stop_lat","stop_lon","zone_id","location_type","stop_url","stop_timezone","wheelchair_boarding","platform_code","level_id","parent_station"]
        stops=pd.read_csv(iGTFSExplodedFeedFolder + os.sep + 'stops.txt', usecols=lambda x: x in usecolsStops)
        # stops=pd.read_csv(iGTFSExplodedFeedFolder + os.sep + 'stops.txt')
        stops.to_sql('tb_stops', conn, if_exists='append', index=False)
    else:
       print("WARNING: file stops.txt NOT existing in " + iGTFSExplodedFeedFolder )    

    # import calendar.txt
    if Path(iGTFSExplodedFeedFolder + os.sep + 'calendar.txt').is_file():
        print("===> calendar.txt exists in " + iGTFSExplodedFeedFolder + ". Importing...")
        c.execute('''CREATE TABLE tb_calendar 
                    (service_id text,
                    monday text,
                    tuesday text,
                    wednesday text,
                    thursday text,
                    friday text,
                    saturday text,
                    sunday text,
                    start_date text, 
                    end_date text)''')

        usecolsCalendar=["service_id","monday","tuesday","wednesday","thursday","friday","saturday","sunday","start_date","end_date"]
        calendar=pd.read_csv(iGTFSExplodedFeedFolder + os.sep + 'calendar.txt', usecols=lambda x: x in usecolsCalendar)
        calendar.to_sql('tb_calendar', conn, if_exists='append', index=False)
        isCalendarFile=True        
    else:
       print("WARNING: file calendar.txt NOT existing in " + iGTFSExplodedFeedFolder ) 

    # import calendar_dates.txt
    if Path(iGTFSExplodedFeedFolder + os.sep + 'calendar_dates.txt').is_file():
        print("===> calendar_dates.txt exists in " + iGTFSExplodedFeedFolder + ". Importing...")
        c.execute('''CREATE TABLE tb_calendar_dates 
                    (service_id text,
                    date text,
                    exception_type text)''')

        usecolsCalendarDates=["service_id","date","exception_type"]
        calendar_dates=pd.read_csv(iGTFSExplodedFeedFolder + os.sep + 'calendar_dates.txt', sep=',', dtype={'a': str}, low_memory=False, usecols=lambda x: x in usecolsCalendarDates)
        calendar_dates.to_sql('tb_calendar_dates', conn, if_exists='append', index=False)
        isCalendarDatesFile=True        
    else:
       print("WARNING: file calendar_dates.txt NOT existing in " + iGTFSExplodedFeedFolder )  

    # import feed_info.txt
    if Path(iGTFSExplodedFeedFolder + os.sep + 'feed_info.txt').is_file():
        print("===> feed_info.txt exists in " + iGTFSExplodedFeedFolder + ". Importing...")
        c.execute('''CREATE TABLE tb_feed_info 
                    (feed_publisher_name text,
                    feed_publisher_url text,
                    feed_lang text,
                    feed_start_date text,
                    feed_end_date text,
                    feed_version text,
                    default_lang text,
                    feed_contact_email text,
                    feed_contact_url text,
                    feed_id text)''')

        usecolsFeedInfo=["feed_publisher_name","feed_publisher_url","feed_lang","feed_start_date","feed_end_date","feed_version","default_lang","feed_contact_email","feed_contact_url","feed_id"]
        feed_info=pd.read_csv(iGTFSExplodedFeedFolder + os.sep + 'feed_info.txt', usecols=lambda x: x in usecolsFeedInfo)
        feed_info.to_sql('tb_feed_info', conn, if_exists='append', index=False)
    else:
          print("WARNING: file feed_info.txt NOT existing in " + iGTFSExplodedFeedFolder)  

    # import routes.txt
    if Path(iGTFSExplodedFeedFolder + os.sep + 'routes.txt').is_file():
        print("===> routes.txt exists in " + iGTFSExplodedFeedFolder + ". Importing...")
        c.execute('''CREATE TABLE tb_routes 
                    (route_id text,
                    agency_id text,
                    route_short_name text,
                    route_long_name text,
                    route_type text,
                    route_url text,
                    route_desc text,
                    route_color text,
                    route_text_color text,
                    route_sort_order text)''')

        c.execute('''CREATE INDEX idx_tb_routes ON tb_routes (route_id)''')

        usecolsRoutes=["route_id","agency_id","route_short_name","route_long_name","route_type","route_url","route_desc","route_color","route_text_color","route_sort_order"]
        routes=pd.read_csv(iGTFSExplodedFeedFolder + os.sep + 'routes.txt', usecols=lambda x: x in usecolsRoutes)
        routes.to_sql('tb_routes', conn, if_exists='append', index=False)
    else:
       print("WARNING: file routes.txt NOT existing in " + iGTFSExplodedFeedFolder) 

    # import shapes.txt
    if Path(iGTFSExplodedFeedFolder + os.sep + 'shapes.txt').is_file():
        print("===> shapes.txt exists in " + iGTFSExplodedFeedFolder + ". Importing...")
        c.execute('''CREATE TABLE tb_shapes 
                    (shape_id text,
                    shape_pt_lat text,
                    shape_pt_lon text,
                    shape_pt_sequence integer,
                    shape_dist_traveled real DEFAULT 0.0)''')

        c.execute('''CREATE INDEX idx_tb_shapes
                      ON tb_shapes (shape_id, shape_pt_sequence)''')

        usecolsShapes=["shape_id","shape_pt_lat","shape_pt_lon","shape_pt_sequence","shape_dist_traveled"]
        shapes=pd.read_csv(iGTFSExplodedFeedFolder + os.sep + 'shapes.txt', usecols=lambda x: x in usecolsShapes)
        shapes.to_sql('tb_shapes', conn, if_exists='append', index=False)
        ################ check shape_dist_traveled populated #################



        #################################################################################
    else:
       print("WARNING: file shapes.txt NOT existing in " + iGTFSExplodedFeedFolder)  
       sys.exit(0)

    # import stop_times.txt
    if Path(iGTFSExplodedFeedFolder + os.sep + 'stop_times.txt').is_file():
        print("===> stop_times.txt exists in " + iGTFSExplodedFeedFolder + ". Importing...")
        c.execute('''CREATE TABLE tb_stop_times 
                    (trip_id text,
                    arrival_time text,
                    departure_time text,
                    stop_id text,
                    stop_sequence integer,
                    stop_headsign text,
                    pickup_type text,
                    drop_off_type text,
                    shape_dist_traveled real,
                    timepoint text)''')

        c.execute('''CREATE INDEX idx_tb_stop_times ON tb_stop_times (stop_id, trip_id)''')

        #stop_times=pd.read_csv(iGTFSExplodedFeedFolder + '/' + 'stop_times.txt')
        usecolsStopTimes=["trip_id","arrival_time","departure_time","stop_id","stop_sequence","stop_headsign","pickup_type","drop_off_type","shape_dist_traveled","timepoint"]
        stop_times=pd.read_csv(iGTFSExplodedFeedFolder + os.sep + 'stop_times.txt', sep=',', dtype={'a': str}, low_memory=False, usecols=lambda x: x in usecolsStopTimes)
        
        stop_times.to_sql('tb_stop_times', conn, if_exists='append', index=False)
    else:
          print("WARNING: file stop_times.txt NOT existing in " + iGTFSExplodedFeedFolder) 

    # import transfers.txt
    if Path(iGTFSExplodedFeedFolder + os.sep + 'transfers.txt').is_file():
        print("===> transfers.txt exists in " + iGTFSExplodedFeedFolder + ". Importing...")
        c.execute('''CREATE TABLE tb_transfers 
                    (from_stop_id text,
                    to_stop_id text,
                    transfer_type text,
                    min_transfer_time text,
                    from_route_id text,
                    to_route_id text)''')

        usecolsTransfers=["from_stop_id","to_stop_id","transfer_type","min_transfer_time","from_route_id","to_route_id"]
        transfers=pd.read_csv(iGTFSExplodedFeedFolder + os.sep + 'transfers.txt', usecols=lambda x: x in usecolsTransfers)
        transfers.to_sql('tb_transfers', conn, if_exists='append', index=False)
    else:
          print("WARNING: file transfers.txt NOT existing in " + iGTFSExplodedFeedFolder)  

    # import trips.txt
    if Path(iGTFSExplodedFeedFolder + os.sep + 'trips.txt').is_file():
        print("===> trips.txt exists in " + iGTFSExplodedFeedFolder + ". Importing...")
        c.execute('''CREATE TABLE tb_trips 
                    (route_id text,
                    service_id text,
                    trip_id text,
                    trip_headsign text,
                    trip_short_name text,
                    direction_id text,
                    block_id text,                
                    shape_id text,
                    wheelchair_accessible text,
                    bikes_allowed text)''')

        c.execute('''CREATE INDEX idx_tb_trips ON tb_trips (trip_id, route_id)''')

        usecolsTrips=["route_id","service_id","trip_id","trip_headsign","trip_short_name","direction_id","block_id","shape_id","wheelchair_accessible","bikes_allowed"]
        trips=pd.read_csv(iGTFSExplodedFeedFolder + os.sep + 'trips.txt', usecols=lambda x: x in usecolsTrips)
        trips.to_sql('tb_trips', conn, if_exists='append', index=False)
    else:
          print("WARNING: file trips.txt NOT existing in " + iGTFSExplodedFeedFolder)                                    

    print("All GTFS text files imported...")

    print("#############################################################################")
    print("Post-processing support database...")

    # force NULL direction_id valueas in tb_trips to "0" ("outbound")
    c.execute('''UPDATE tb_trips SET direction_id="0" WHERE direction_id IS NULL''')
    conn.commit()

    # insert field route_type_decoded in tb_routes

    c.execute('''ALTER TABLE tb_routes ADD COLUMN route_type_decoded text''')
    conn.commit()

    c.execute('''UPDATE tb_routes 
                SET route_type_decoded=
                CASE route_type 
                    WHEN "0" THEN  "tram"                
                    WHEN "1" THEN  "metro"
                    WHEN "2" THEN  "rail"
                    WHEN "3" THEN  "bus"
                    WHEN "4" THEN  "water"
                    WHEN "5" THEN  "cableway"
                    WHEN "6" THEN  "lift"
                    WHEN "7" THEN  "funicular"
                    WHEN "11" THEN  "trolleyBus"  
                    WHEN "1100" THEN "air"                
                    ELSE "unknown" 
                END
                ''') 
    conn.commit()

    # insert field route_type_decoded in tb_routes

    c.execute('''ALTER TABLE tb_trips ADD COLUMN direction_id_decoded text''')
    conn.commit()

    c.execute('''UPDATE tb_trips 
                SET direction_id_decoded=
                CASE direction_id 
                    WHEN "0" THEN  "outbound" 
                    WHEN "1" THEN "inbound"              
                    ELSE "outbound" 
                END
                ''') 
    conn.commit()

    # SRV-GENERAL_INFO - Create the general info table
    print("Creating general info table...")

    c.execute('''CREATE TABLE srv_general_info 
                (valid_from text,
                valid_to text)''')
    conn.commit()  

    # CALENDAR - Create and populate a general srv_calendar for 2022
    # derived from https://gist.github.com/exocomet/fb4a588c7eb081f62ce3c8acb268293b
    # Create
    print("Creating and populating support calendar...")

    c.execute('''CREATE TABLE IF NOT EXISTS srv_calendar (
      d date UNIQUE NOT NULL, 
      dayofweek INT NOT NULL,
      weekday TEXT NOT NULL,
      quarter INT NOT NULL,
      year INT NOT NULL,
      month INT NOT NULL,
      day INT NOT NULL,
      bitvalue TEXT NOT NULL DEFAULT "0",
      dayCompact TEXT NOT NULL DEFAULT "abc",
      weekDay_slot TEXT NOT NULL DEFAULT "xyz"
    )''')
    conn.commit() 


#################################### C H E C K #################################################
    c.execute('''CREATE TABLE IF NOT EXISTS srv_dates_range (
      candidate_date TEXT NOT NULL 
    )''')
    conn.commit() 
###################################### C H E C K ###############################################################
    # check if the GTFS feed is calendar-based (tb_calendar is empty AND tb_calendar_dates has occurrencies)
    # or calendar_dates-based (tb_calendar has occurrencies AND tb_calendar_dates is empty)
    if isCalendarFile==True:
      c.execute('''SELECT COUNT(*) FROM tb_calendar''')
      data=c.fetchall()
      for row in data:
            result=row[0]
            if result==0:
              print('CALENDARWISE: the feed may be calendar-dates-based')
            else:
              isCalendarBased=True
              print('CALENDARWISE: the feed is calendar-based')            
          #print(result, service_id)

      conn.commit()

#####################################################################
    if isCalendarBased:              
      c.execute('''INSERT INTO srv_dates_range(candidate_date) SELECT start_date FROM tb_calendar''')
      conn.commit() 
      c.execute('''INSERT INTO srv_dates_range(candidate_date) SELECT end_date FROM tb_calendar''')
      conn.commit()    

      c.execute('''SELECT MIN(candidate_date), MAX(candidate_date) FROM srv_dates_range''')
      records=c.fetchall()
      for row in records:
        if(row[0]):
          earliestStartDate=row[0]
          earliestStartDate0101=earliestStartDate[0:4]+"-01-01" 
          print("<<<<<<<<<<<<<<<<<<< Earliest srv_dates_range.candidate_date: ", earliestStartDate, " ======> ", earliestStartDate0101)
        if(row[1]):
          latestStartDate=row[1]
          latestStartDate3112=latestStartDate[0:4]+"-12-31" 
          print("<<<<<<<<<<<<<<<<<<< Latest srv_dates_range.candidate_date: ", latestStartDate, " ======> ", latestStartDate3112)                     
####################################################################         

    # check if the GTFS feed is calendar-dates-based (tb_calendar_dates has occurrencies)
    if isCalendarDatesFile==True:
      c.execute('''SELECT COUNT(*) FROM tb_calendar_dates''')
      data=c.fetchall()
      for row in data:
            result=row[0]
            if result==0:
              print('CALENDARWISE: the feed IS NOT calendar-dates-based')
              c.execute('''DROP TABLE tb_calendar_dates''')           
            else:
              isCalendarDatesBased=True
              print('CALENDARWISE: the feed is calendar-dates-based')  

    conn.commit()

#####################################################################
    print('++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
    print('++++++++++++++++++STATUS CALENDAR BASED:', isCalendarBased)
    print('++++++++++++++++++STATUS CALENDAR DATES BASED:', isCalendarDatesBased)
    print('++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
######################################################################
    if isCalendarDatesBased:       
      c.execute('''INSERT INTO srv_dates_range(candidate_date) SELECT date FROM tb_calendar_dates''')
      conn.commit()                     
####################################################################   

      c.execute('''SELECT MIN(candidate_date), MAX(candidate_date) FROM srv_dates_range''')
      records=c.fetchall()
      for row in records:
        if(row[0]):
          earliestStartDate=row[0]
          earliestStartDate0101=earliestStartDate[0:4]+"-01-01" 
          print("<<<<<<<<<<<<<<<<<<< Earliest srv_dates_range.candidate_date: ", earliestStartDate, " ======> ", earliestStartDate0101)
        if(row[1]):
          latestStartDate=row[1]
          latestStartDate3112=latestStartDate[0:4]+"-12-31" 
          print("<<<<<<<<<<<<<<<<<<< Latest srv_dates_range.candidate_date: ", latestStartDate, " ======> ", latestStartDate3112) 

############################# C H E C K #########################################

############################################# C H E C K #######################################################################

    # Populate service calendar

    c.execute('''INSERT
      OR ignore INTO srv_calendar (d, dayofweek, weekday, quarter, year, month, day)
    SELECT *
    FROM (
      WITH RECURSIVE dates(d) AS (
        VALUES(?)
        UNION ALL
        SELECT date(d, '+1 day')
        FROM dates
        WHERE d < ?
      )
      SELECT d,
        (CAST(strftime('%w', d) AS INT) + 6) % 7 AS dayofweek,
        CASE
          (CAST(strftime('%w', d) AS INT) + 6) % 7
          WHEN 0 THEN 'Monday'
          WHEN 1 THEN 'Tuesday'
          WHEN 2 THEN 'Wednesday'
          WHEN 3 THEN 'Thursday'
          WHEN 4 THEN 'Friday'
          WHEN 5 THEN 'Saturday'
          ELSE 'Sunday'
        END AS weekday,
        CASE
          WHEN CAST(strftime('%m', d) AS INT) BETWEEN 1 AND 3 THEN 1
          WHEN CAST(strftime('%m', d) AS INT) BETWEEN 4 AND 6 THEN 2
          WHEN CAST(strftime('%m', d) AS INT) BETWEEN 7 AND 9 THEN 3
          ELSE 4
        END AS quarter,
        CAST(strftime('%Y', d) AS INT) AS year,
        CAST(strftime('%m', d) AS INT) AS month,
        CAST(strftime('%d', d) AS INT) AS day
      FROM dates
    )''', (earliestStartDate0101, latestStartDate3112))    

    conn.commit()  

    # populate dayCompact in a "yyyymmdd" format and weekday_slot in a Sunday 0-based sequence

    c.execute('''UPDATE srv_calendar 
    SET
      bitvalue="0",
        dayCompact=strftime('%Y%m%d',d),
        weekday_slot=
        CASE weekday
          WHEN "Monday" THEN "1"
          WHEN "Tuesday" THEN "2"
          WHEN "Wednesday" THEN "3"
          WHEN "Thursday" THEN "4"
          WHEN "Friday" THEN "5"
          WHEN "Saturday" THEN "6"			
          ELSE "0"
        END
    ''')
    conn.commit() 

    # CALENDAR_TEMP - create a compact copy of calendar, to be used dynamically to get a bitstring of validity for a single tb_calendar_dates.service_id
    # create
    print("creating and populating dynamic support calendar...")

    c.execute('''CREATE TABLE IF NOT EXISTS srv_calendar_temp (
      dayCompact TEXT NOT NULL,
      weekday TEXT NOT NULL,
      weekDay_slot TEXT NOT NULL,  
      bitvalue TEXT NOT NULL
    )''')
    conn.commit() 

    # insert further info in srv_calendar_temp...
    c.execute('''INSERT INTO srv_calendar_temp(dayCompact,weekday,weekDay_slot, bitvalue) SELECT dayCompact,weekday,weekDay_slot, bitvalue FROM srv_calendar''')
    conn.commit() 

    # update departure/arrival times in tb_stop_times adding leading-zeros in hours
    c.execute('''UPDATE tb_stop_times 
                  SET
                  departure_time=SUBSTR("0000000"||departure_time,LENGTH(departure_time),8),
                  arrival_time=SUBSTR("0000000"||arrival_time,LENGTH(arrival_time),8)
                ''') 
    conn.commit() 

#######################################################################################################################################################
    print("adding column to tb_stop_times to support OpenTripPlanner-compliant timetabled passing times for after midnight arrival/departures...")
    c.execute('''ALTER TABLE tb_stop_times ADD COLUMN arrival_time_extended text''')
    conn.commit()
    c.execute('''ALTER TABLE tb_stop_times ADD COLUMN arrival_day_offset text''')
    conn.commit()
    c.execute('''ALTER TABLE tb_stop_times ADD COLUMN departure_time_extended text''')
    conn.commit()
    c.execute('''ALTER TABLE tb_stop_times ADD COLUMN departure_day_offset text''')
    conn.commit()

    print("updating tb_stop_times accordingly...")
    c.execute('''UPDATE tb_stop_times
                  SET arrival_time_extended=(
                    CASE
                      WHEN (arrival_time > "24:00:00") THEN 
                        (IIF(LENGTH((CAST(SUBSTR(arrival_time,1,2) AS INT)-24)) == 1, '0' || (CAST(SUBSTR(arrival_time,1,2) AS INT)-24), 
                        (CAST(SUBSTR(arrival_time,1,2) AS INT)-24)) || ':' || SUBSTR(arrival_time,4,2) || ':' || SUBSTR(arrival_time,7,2))
                      ELSE arrival_time
                    END),
                    arrival_day_offset=(
                    CASE
                      WHEN (arrival_time > "24:00:00") THEN "1"
                      ELSE "0"
                    END),
                    departure_time_extended=(
                    CASE
                      WHEN (departure_time > "24:00:00") THEN 
                        (IIF(LENGTH((CAST(SUBSTR(departure_time,1,2) AS INT)-24)) == 1, '0' || (CAST(SUBSTR(departure_time,1,2) AS INT)-24), 
                        (CAST(SUBSTR(departure_time,1,2) AS INT)-24)) || ':' || SUBSTR(departure_time,4,2) || ':' || SUBSTR(departure_time,7,2))
                      ELSE departure_time
                    END),
                    departure_day_offset=(
                    CASE
                      WHEN (departure_time > "24:00:00") THEN "1"
                      ELSE "0"
                    END)''') 
    conn.commit()     
########################################################################################################################################################
  # check if tb_stop_times has the column shape_distance_traveled popolated
    c.execute('''SELECT COUNT(DISTINCT(shape_dist_traveled)) FROM tb_stop_times''')
    data=c.fetchall()
    for row in data:
          result=row[0]
#          if result==0 => column shape_dist_traveled exists in stop_times.txt but IS NULL
#          if result==1 => column shape_dist_traveled DOESNT exists in stop_times.txt so is always zero (0.0)
          if (result==0 or result==1):            
            print('DISTANCE TRAVELED NOT FILLED in stop_times.txt: exit program...')
            sys.exit(0)

    conn.commit() 

#######################################################################################
   # check if tb_shapes has the column shape_distance_traveled popolated
   # isShapeDistanceTraveledPopulated
    c.execute('''SELECT COUNT(DISTINCT(shape_dist_traveled)) FROM tb_shapes''')
    data=c.fetchall()
    for row in data:
          result=row[0]
#          if result==0 => column shape_dist_traveled exists in shapes.txt but IS NULL
#          if result==1 => column shape_dist_traveled DOESNT exists in shapes.txt so is always zero (0.0)
          if (result==0 or result==1):            
            print('DISTANCE TRAVELED NOT FILLED in shapes.txt: need to calculate it from scratch...')
            StartImportProcess.calculateHaversineDistanceForShapes(self, iGTFSExplodedFeedFolder, iDb)
          else:
            isShapeDistanceTraveledPopulated=True
            print('Distance traveled populated..check max value per-shape...')            
        #print(result, service_id)

    conn.commit()   

################################### BEGINNING BLOCK <=> isCalendarBased=False    

    # create 2 views aimed to get a service-based dictionary to populate structures of <ServiceCalendarFrama> 

    if ((isCalendarBased==True and isCalendarDatesBased==False) or (isCalendarBased==True and isCalendarDatesBased==True)): 
      print("iDb: " + iDb)
#      StartImportProcess.createSyntheticTbCalendarDates(self, iGTFSExplodedFeedFolder, iDb, isCalendarBased, isCalendarDatesBased, iStartDay, iEndDay)
      StartImportProcess.createSyntheticTbCalendarDates(self, iGTFSExplodedFeedFolder, iDb, isCalendarBased, isCalendarDatesBased, earliestStartDate0101, latestStartDate3112)      

      print("extending table tb_calendar_dates...")

    c.execute('''ALTER TABLE tb_calendar_dates ADD COLUMN isDayAvailable text''')
    conn.commit()
    c.execute('''ALTER TABLE tb_calendar_dates ADD COLUMN weekDay_string text''')
    conn.commit()
    c.execute('''ALTER TABLE tb_calendar_dates ADD COLUMN weekDay_slot text''')
    conn.commit()
    c.execute('''ALTER TABLE tb_calendar_dates ADD COLUMN dayUTC text''')
    conn.commit()

      ######## update tb_calendar_dates non-CSV derived fields #######

      # update dayUTC
    c.execute('''UPDATE tb_calendar_dates 
                  SET dayUTC=strftime('%Y-%m-%dT%H:%M:%S', substr(date,1,4)||"-"||substr(date,5,2)||"-"||substr(date,7,2))
                  ''') 
    conn.commit()           

    print("creating and populating support table srv_calendar_dates...")
          
          # update isDayAvailable to a NeTEx-ready token
    c.execute('''UPDATE tb_calendar_dates 
                  SET isDayAvailable=
                  CASE exception_type 
                      WHEN "1" THEN  "true"
                      ELSE "false" 
                  END
                  ''') 
    conn.commit()  

          # update weekDay_slot to a NeTEx-ready token
    c.execute('''UPDATE tb_calendar_dates 
                  SET weekDay_slot=strftime('%w',dayUTC) 
                  ''') 
    conn.commit()

          # update weekday_string to a NeTEx-ready token
    c.execute('''UPDATE tb_calendar_dates 
                  SET weekDay_string=
                  CASE strftime('%w',dayUTC) 
                      WHEN "0" THEN "Sunday"
                      WHEN "1" THEN  "Monday"
                      WHEN "2" THEN  "Tuesday"
                      WHEN "3" THEN  "Wednesday"
                      WHEN "4" THEN  "Thursday"
                      WHEN "5" THEN  "Friday"
                      ELSE "Saturday" 
                  END
                  ''') 
    conn.commit()

#####################################

    c.execute('''CREATE VIEW v_calendar_dates_1 
                      AS
                      SELECT DISTINCT 
                              service_id, 
                              min(date) AS date_start, 
                              max(date) AS date_end, 
                              weekDay_string, 
                              weekDay_slot 
                              FROM tb_calendar_dates  
                              GROUP BY service_id, weekDay_slot 
                              ORDER BY service_id, weekDay_slot
                  ''') 
    conn.commit()    

    c.execute('''CREATE VIEW v_calendar_dates_2 
                      AS
                      SELECT DISTINCT 
                              service_id, 
                              MIN(date_start) AS date_start_min, 
                              MAX(date_end) AS date_end_max, 
                              GROUP_CONCAT(weekDay_string, " ") AS daysOfWeek 
                              FROM v_calendar_dates_1 
                              GROUP BY service_id ORDER BY service_id;   
                  ''') 
    conn.commit()       

# create view with distance traveled per shape
    if (isShapeDistanceTraveledPopulated==True):
      c.execute('''CREATE VIEW v_distance_traveled_per_shape 
                        AS
                        SELECT shape_id, MAX(shape_dist_traveled) AS distance FROM tb_shapes GROUP BY shape_id
                    ''') 
      conn.commit()  
    else:
      c.execute('''CREATE VIEW v_distance_traveled_per_shape 
                        AS
                        SELECT shape_id, SUM(partialDistance) AS distance FROM tb_shapes GROUP BY shape_id
                    ''') 
      conn.commit()  

      #update general info table
    c.execute('''INSERT INTO srv_general_info(valid_from, valid_to) 
                  SELECT min(dayUTC), max(dayUTC) from tb_calendar_dates''')          
    conn.commit() 

      # SRV_CALENDAR_DATES_SUMMARY - summary table as materialization of v_calendar_dates_2
      # create
    c.execute('''CREATE TABLE IF NOT EXISTS srv_calendar_dates_summary (
        service_id TEXT NOT NULL,
        date_start_min TEXT NOT NULL,
        date_end_max TEXT NOT NULL,  
        daysOfWeek TEXT NOT NULL,
        validDayBits TEXT NOT NULL
      )''')
    conn.commit() 

      # insert
    c.execute('''INSERT INTO srv_calendar_dates_summary(service_id,date_start_min,date_end_max,daysOfWeek, validDayBits) 
                      SELECT service_id,date_start_min,date_end_max,daysOfWeek, "zzz" FROM v_calendar_dates_2''')
    conn.commit()     

  # populate tb_calendar_dates_summary.validDayBits through dynamic manipulation of calendar_temp
  # external loop: get single tb_calendar_dates_summary.service_id values
          # internal loop: 
          #        update calendar_temp.bitvalue with correspondent tb_calendar_dates.exception_type (same day)
          #        calculate concatenation of calendar_temp.bitvalues(s) 
          #        update tb_calendar_dates_summary.validDayBitsS
          #        reset calendar_temp.bitvalue(s) to zero

    c.execute('''SELECT service_id FROM srv_calendar_dates_summary''')
    data=c.fetchall()
    for row in data:
        service_id=""
        service_id=row[0]
    #    print('service_id=',service_id)
        c.execute('''UPDATE srv_calendar_temp
                      SET bitvalue=tb_calendar_dates.exception_type
                      FROM tb_calendar_dates
                      WHERE
                        tb_calendar_dates.date=srv_calendar_temp.dayCompact
                        AND
                        tb_calendar_dates.service_id=?
                  ''', [service_id])            
        conn.commit()

        c.execute('''SELECT GROUP_CONCAT(bitvalue,"") FROM srv_calendar_temp''')
        data1=c.fetchall()
        for row1 in data1:
          result=""
          result=row1[0]
        #print(result, service_id)

        c.execute('''UPDATE srv_calendar_dates_summary SET validDayBits=? WHERE service_id=?''', (str(result), str(service_id)))

        conn.commit()

        c.execute('''UPDATE srv_calendar_temp SET bitvalue="0"''')    
        conn.commit()


################################### END BLOCK <=> isCalendarBased=False    

    print("Base views creation...")
    # create view v_routes_trips_stops_stoptimes_1
    c.execute('''CREATE VIEW v_routes_trips_stops_stoptimes_1
                AS
                SELECT
                r.route_id, r.route_short_name, r.route_long_name, r.route_type, 
                t.service_id, t.trip_id, t.direction_id, t.trip_headsign, t.trip_short_name, t.shape_id,
                st.departure_time, st.arrival_time, st.stop_id, st.stop_sequence, st.stop_headsign, r.route_type_decoded,
                st.arrival_time_extended, st.arrival_day_offset, st.departure_time_extended, st.departure_day_offset
                FROM
                tb_routes AS r, tb_trips AS t, tb_stops AS s, tb_stop_times AS st
                WHERE
                r.route_id=t.route_id
                AND
                s.stop_id=st.stop_id
                AND
                st.trip_id=t.trip_id;   
                  ''') 

    conn.commit()  

    # create view v_routes_trips_stops_stoptimes_2
    c.execute('''CREATE VIEW v_routes_trips_stops_stoptimes_2
                AS
                SELECT DISTINCT route_id, direction_id, trip_id, MIN(stop_sequence) AS stop_start_seq, MAX(stop_sequence) AS stop_end_seq, shape_id, route_type_decoded 
                FROM v_routes_trips_stops_stoptimes_1 
                GROUP BY route_id, direction_id, shape_id 
                ORDER BY route_id, direction_id;   
                  ''') 

    conn.commit()  

    # create view with compact gml element for shape sequences
    c.execute('''CREATE VIEW v_shapes_xy_sequenced
                  AS
                  SELECT DISTINCT
                    shape_id,
                    GROUP_CONCAT(shape_pt_lat || " " || shape_pt_lon, " ") AS gml_field
                  FROM tb_shapes
                  GROUP BY shape_id
                  ORDER BY shape_id;   
                  ''') 

    conn.commit()   

        # create view with compact gml element AND distance traveled for shape sequences
    c.execute('''CREATE VIEW v_shapes_gml_with_distance
                AS
                SELECT 
                a.shape_id AS d_shape_id, 
                a.distance AS d_distance, 
                b.gml_field AS d_gml_field
                FROM v_distance_traveled_per_shape AS a
                INNER JOIN v_shapes_xy_sequenced AS b WHERE a.shape_id=b.shape_id
                ORDER BY a.shape_id; 
                  ''') 

    conn.commit()  

    # create helper view to address the correct starting sequence of the trip (see START-RA and AMTEXT cases) in TimetableFrame.py
    c.execute('''CREATE VIEW v_extended_starting_time
                  AS
                  SELECT DISTINCT route_id, trip_id, MIN(stop_sequence) min_stop, departure_time_extended, departure_day_offset
                  FROM v_routes_trips_stops_stoptimes_1  
                  GROUP BY route_id, trip_id
                  ORDER BY trip_id ASC;   
                  ''')      

    # c.execute('''CREATE VIEW v_routes_trips_stops_stoptimes_3
    #               AS
    #               SELECT DISTINCT 
    #               route_id, direction_id, service_id, trip_id, 
    #               MIN(departure_time) AS starts_at, MIN(stop_sequence) AS stop_start_seq, 
    #               MAX(arrival_time) AS arrives_at, MAX(stop_sequence) AS stop_end_seq, 
    #               (SUBSTR(SUBSTR("0000000"||MAX(arrival_time),LENGTH(MAX(arrival_time)),8),1,2)*3600 + 
    #                 SUBSTR(SUBSTR("0000000"||MAX(arrival_time),LENGTH(MAX(arrival_time)),8),4,2)*60 + 
    #                 SUBSTR(SUBSTR("0000000"||MAX(arrival_time),LENGTH(MAX(arrival_time)),8),7,2))-
    #                 (SUBSTR(SUBSTR("0000000"||MIN(departure_time),LENGTH(MIN(departure_time)),8),1,2)*3600 + 
    #                 SUBSTR(SUBSTR("0000000"||MIN(departure_time),LENGTH(MIN(departure_time)),8),4,2)*60 + 
    #                 SUBSTR(SUBSTR("0000000"||MIN(departure_time),LENGTH(MIN(departure_time)),8),7,2)) AS duration,
    #               shape_id, trip_headsign, route_type_decoded,
    #               MIN(departure_time_extended) AS departureTimeExtended, MIN(departure_day_offset) AS departureDayOffset 
    #               FROM v_routes_trips_stops_stoptimes_1 
    #               GROUP BY route_id, trip_id 
    #               ORDER BY route_id, trip_id;''')    

    # create helper table to address the correct starting sequence of the trip (see START-RA and AMTEXT cases) in TimetableFrame.py
    c.execute('''CREATE TABLE t_routes_trips_stops_stoptimes_3
                  AS
                  SELECT DISTINCT 
                                    route_id, direction_id, service_id, trip_id, 
                                    MIN(departure_time) AS starts_at, MIN(stop_sequence) AS stop_start_seq, 
                                    MAX(arrival_time) AS arrives_at, MAX(stop_sequence) AS stop_end_seq, 
                                    (SUBSTR(SUBSTR("0000000"||MAX(arrival_time),LENGTH(MAX(arrival_time)),8),1,2)*3600 + 
                                      SUBSTR(SUBSTR("0000000"||MAX(arrival_time),LENGTH(MAX(arrival_time)),8),4,2)*60 + 
                                      SUBSTR(SUBSTR("0000000"||MAX(arrival_time),LENGTH(MAX(arrival_time)),8),7,2))-
                                      (SUBSTR(SUBSTR("0000000"||MIN(departure_time),LENGTH(MIN(departure_time)),8),1,2)*3600 + 
                                      SUBSTR(SUBSTR("0000000"||MIN(departure_time),LENGTH(MIN(departure_time)),8),4,2)*60 + 
                                      SUBSTR(SUBSTR("0000000"||MIN(departure_time),LENGTH(MIN(departure_time)),8),7,2)) AS duration,
                                    shape_id, trip_headsign, route_type_decoded,
                                    MIN(stop_sequence) AS min_stop_sequence
                                    FROM v_routes_trips_stops_stoptimes_1 
                                    GROUP BY route_id, trip_id 
                                    ORDER BY route_id, trip_id; 
                  ''')  

# 2023-06-08 section added to fix ServiceLink bug    
    conn.commit()

    print("NEW views creation...")

    c.execute('''CREATE VIEW v_trips_stoptimes_shapes_1_NEW
                  AS						
                  SELECT DISTINCT
                      r.route_id, r.route_long_name, r.route_type_decoded, st.trip_id, st.stop_id, st.shape_dist_traveled, st.stop_sequence, 
                      t.service_id, t.trip_id, t.direction_id, t.trip_headsign, t.shape_id, st.pickup_type, st.drop_off_type
                  FROM
                      tb_routes AS r, tb_trips AS t, tb_stop_times AS st
                  WHERE
                      r.route_id=t.route_id
                    AND
                      st.trip_id=t.trip_id
                  GROUP BY
                      r.route_id, st.trip_id, st.stop_id, st.shape_dist_traveled, st.stop_sequence, 
                      t.service_id, t.trip_id, t.direction_id, t.trip_headsign, t.shape_id
                  ORDER BY
                    r.route_id, st.trip_id, st.stop_id, st.stop_sequence;''')
    conn.commit()

    c.execute('''CREATE VIEW v_trips_stoptimes_shapes_2_NEW
                  AS
                  SELECT DISTINCT 
                     route_id, route_long_name, route_type_decoded,trip_id, service_id, direction_id, stop_id, 
                     shape_dist_traveled, LAG(shape_dist_traveled, -1, 0.0) OVER (ORDER BY trip_id, stop_sequence) AS shape_dist_traveled_next,  
                     stop_sequence, trip_headsign, shape_id, (shape_id || '_' || stop_sequence) || '_' || stop_id AS shape_id_partial  
                  FROM v_trips_stoptimes_shapes_1_NEW 
                  GROUP BY route_id, trip_id, stop_id, shape_dist_traveled, stop_sequence, trip_headsign, shape_id
                   ORDER BY route_id, trip_id, shape_id, stop_sequence;''')                   
    conn.commit()

    c.execute('''CREATE VIEW v_trips_stoptimes_shapes_3_NEW
                  AS
                  SELECT DISTINCT 
                    route_id, route_long_name, route_type_decoded, direction_id, stop_id, shape_dist_traveled, 
                    shape_dist_traveled_next, stop_sequence, shape_id, shape_id_partial
                  FROM v_trips_stoptimes_shapes_2_NEW
                  GROUP BY route_id, route_long_name, route_type_decoded, direction_id, stop_id, shape_dist_traveled, shape_dist_traveled_next, stop_sequence, shape_id, shape_id_partial
                  ORDER BY route_id, shape_id, direction_id, stop_sequence;''')                  
    conn.commit()

    c.execute('''CREATE VIEW v_trips_stoptimes_shapes_4_NEW
                  AS
                  SELECT 
                    a.route_id, route_long_name, route_type_decoded, a.direction_id, a.stop_id, LAG(stop_id, -1, "") OVER (ORDER BY shape_id, stop_sequence) AS stop_id_next, a.shape_dist_traveled, a.shape_dist_traveled_next, (a.shape_dist_traveled_next-a.shape_dist_traveled) AS delta,
                    a.stop_sequence,  a.shape_id, a.shape_id_partial,  
                   (SELECT DISTINCT GROUP_CONCAT(b.shape_pt_lat || " " || b.shape_pt_lon, " ")                   
                  FROM tb_shapes AS b 
                  WHERE 
                    b.shape_id=a.shape_id
                    AND 
                    b.shape_dist_traveled >= a.shape_dist_traveled
                    AND
                    b.shape_dist_traveled <= a.shape_dist_traveled_next
                    AND 
                    a.shape_dist_traveled_next != 0.0
                  GROUP BY b.shape_id
                  ORDER BY b.shape_id, b.shape_pt_sequence) AS gml_field_partial 
                  FROM v_trips_stoptimes_shapes_3_NEW AS a;''')
    conn.commit()

    # c.execute('''CREATE VIEW v_spatial_patterns
    #               AS
    #               SELECT 
    #               trip_id,
    #               (MIN(stop_sequence) || "-" || MAX(stop_sequence) || "-" || COUNT(stop_id)) AS space_patt FROM tb_stop_times GROUP BY trip_id ORDER BY trip_id''')

    c.execute('''CREATE VIEW v_spatial_patterns
                  AS
                  SELECT 
                  trip_id,
                  (MIN(stop_sequence) || "-" || MAX(stop_sequence) || "-" || COUNT(stop_id) || "-" || SUM(pickup_type) || "-" || SUM(drop_off_type)) AS space_patt FROM tb_stop_times GROUP BY trip_id ORDER BY trip_id''')

    conn.commit()

    print("SPATT view creation...")    

    c.execute('''CREATE TABLE t_trips_stoptimes_shapes_1_SPATT
                  AS SELECT 
                       v1.route_id, v1.route_long_name, v1.route_type_decoded, v1.trip_id, v1.stop_id, v1.shape_dist_traveled, v1.stop_sequence, 
                       v1.service_id, v1.direction_id, v1.trip_headsign, v1.shape_id, v_spatt.space_patt, v1.pickup_type, v1.drop_off_type
                     FROM 
                       v_trips_stoptimes_shapes_1_NEW AS v1
                     INNER JOIN
                       v_spatial_patterns AS v_spatt
                     ON v1.trip_id=v_spatt.trip_id
                     ORDER BY v1.route_id, v1.trip_id, v1.stop_sequence;''')
    conn.commit()

    c.execute('''CREATE VIEW v_trips_stoptimes_shapes_2_SPATT
                 AS
                 SELECT DISTINCT 
                   route_id, route_long_name, route_type_decoded,trip_id, service_id, direction_id, stop_id, 
                   shape_dist_traveled, LAG(shape_dist_traveled, -1, 0.0) OVER (ORDER BY trip_id, stop_sequence) AS shape_dist_traveled_next,  
                   stop_sequence, trip_headsign, shape_id, (shape_id || '_' || stop_sequence) || '_' || stop_id AS shape_id_partial, space_patt, pickup_type, drop_off_type  
                 FROM t_trips_stoptimes_shapes_1_SPATT       
                 GROUP BY route_id, trip_id, stop_id, shape_dist_traveled, stop_sequence, trip_headsign, shape_id
                 ORDER BY route_id, trip_id, shape_id, stop_sequence;''')
    conn.commit()

    c.execute('''CREATE TABLE t_trips_stoptimes_shapes_2_SPATT
                  AS SELECT 
                       route_id, route_long_name, route_type_decoded, trip_id, service_id, direction_id, stop_id, 
                       shape_dist_traveled, shape_dist_traveled_next, 
                       stop_sequence, trip_headsign, shape_id, shape_id_partial, space_patt, pickup_type, drop_off_type 
                     FROM 
                       v_trips_stoptimes_shapes_2_SPATT;''')

    # c.execute('''CREATE TABLE t_trips_stoptimes_shapes_3_SPATT 
    #               AS						
    #               SELECT DISTINCT
    #                    IIF(a.route_type_decoded="lift", "unknown", a.route_type_decoded) AS route_type_decoded, a.route_id, a.direction_id, a.shape_id, a.route_long_name,
    #                    a.space_patt
    #                  FROM t_trips_stoptimes_shapes_2_SPATT AS a
    #                  GROUP BY a.route_id, a.direction_id, a.shape_id, a.space_patt
    #                  ORDER BY a.route_id, a.direction_id''')

    c.execute('''CREATE TABLE t_trips_stoptimes_shapes_3_SPATT 
                  AS						
                  SELECT DISTINCT
                       IIF(a.route_type_decoded="lift", "unknown", a.route_type_decoded) AS route_type_decoded, a.route_id, a.direction_id, a.shape_id, a.route_long_name,
                       a.space_patt, a.service_id
                     FROM t_trips_stoptimes_shapes_2_SPATT AS a
                     GROUP BY a.route_id, a.direction_id, a.shape_id, a.space_patt, a.service_id
                     ORDER BY a.route_id, a.direction_id''')
    conn.commit()

    c.execute('''ALTER TABLE t_trips_stoptimes_shapes_3_SPATT ADD COLUMN distance text''')
    conn.commit()

    c.execute('''UPDATE t_trips_stoptimes_shapes_3_SPATT
                  SET distance=(SELECT CAST(b.d_distance AS INTEGER) FROM v_shapes_gml_with_distance AS b WHERE b.d_shape_id=shape_id)''')
    conn.commit()

    c.execute('''CREATE VIEW v_trips_stoptimes_shapes_3_SPATT
                  AS
                  SELECT DISTINCT
	                  route_id, route_long_name, route_type_decoded, direction_id, stop_id, 
	                  shape_dist_traveled, shape_dist_traveled_next, stop_sequence, shape_id, shape_id_partial, space_patt
                  FROM v_trips_stoptimes_shapes_2_SPATT
                  GROUP BY route_id, route_long_name, route_type_decoded, direction_id, stop_id, shape_dist_traveled, shape_dist_traveled_next, stop_sequence, shape_id, shape_id_partial, space_patt
                  ORDER BY route_id, shape_id, direction_id, stop_sequence;''')                  
    conn.commit()   
       
    print("Stop place views creation...")

    c.execute('''CREATE TABLE tb_stopplace_types 
                    (transport_mode text,
                    stop_place_type text)
              ''')
    conn.commit()  

    c.execute('''INSERT INTO tb_stopplace_types (transport_mode, stop_place_type)
                 VALUES
                 ("tram","tramStation"),                 
                 ("rail","railStation"),
                 ("metro","metroStation"),
                 ("funicular","other"),
                 ("water","other"),
                 ("ferry", "ferryStop"),
                 ("air","airport"),
                 ("bus","onstreetBus"),
                 ("cableway","other"),
                 ("lift","liftStation"),
                 ("trolleybus","other")                                                                                                                                        
              ''')
    conn.commit() 

    print("...v_stopplaces_extended_1 creation...")

    c.execute('''CREATE VIEW v_stopplaces_extended_1
                 AS
                 SELECT tb_stops.stop_id, tb_stops.stop_name, tb_stops.stop_lat, tb_stops.stop_lon,
                 (SELECT DISTINCT route_type_decoded FROM t_trips_stoptimes_shapes_1_SPATT WHERE tb_stops.stop_id=stop_id) AS transport_mode_decoded
                 FROM tb_stops;
              ''')    
    conn.commit() 

    print("...tb_stopplaces_extended_0...")

    c.execute('''CREATE TABLE tb_stopplaces_extended_0
                 AS
                 SELECT stop_id, stop_name, stop_lat, stop_lon, transport_mode_decoded
                FROM v_stopplaces_extended_1;
              ''')    
    conn.commit()                    

    print("...v_stopplaces_extended_2 creation...")

    c.execute('''CREATE VIEW v_stopplaces_extended_2
                 AS
                 SELECT stop_id, stop_name, stop_lat, stop_lon, transport_mode_decoded,
                 (SELECT a.stop_place_type FROM tb_stopplace_types AS a WHERE a.transport_mode=transport_mode_decoded) AS stop_place_type_decoded
                FROM tb_stopplaces_extended_0;
              ''')    
    conn.commit() 

    print("...tb_stopplaces_extended...")

    c.execute('''CREATE TABLE tb_stopplaces_extended
                 AS
                 SELECT stop_id, stop_name, stop_lat, stop_lon, transport_mode_decoded, stop_place_type_decoded
                FROM v_stopplaces_extended_2;
              ''')    
    conn.commit() 

    print("...tb_stopplaces_extended_multi needed to detect stops served by different MOTs...")

    c.execute('''CREATE TABLE tb_stopplaces_extended_multi
                 AS
                 SELECT DISTINCT stop_id, route_type_decoded
                FROM t_trips_stoptimes_shapes_1_SPATT
                GROUP BY stop_id, route_type_decoded;
              ''')    
    conn.commit() 

    print("...updating tb_stopplaces_extended for stops served by different MOTs...")

    c.execute('''UPDATE tb_stopplaces_extended 
                  SET transport_mode_decoded="other", stop_place_type_decoded="other" 
                  WHERE
                  stop_id IN (SELECT DISTINCT b.stop_id FROM tb_stopplaces_extended_multi AS b GROUP BY b.stop_id HAVING COUNT(b.stop_id)>1);
              ''')    
    conn.commit() 

    print("==============> Support database ready...")

    c.close()