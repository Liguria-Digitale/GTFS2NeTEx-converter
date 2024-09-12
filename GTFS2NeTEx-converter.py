from pathlib import Path
import sqlite3
import argparse
import textwrap
import os
import glob
import CheckMandatoryFilesComponent
import CreateExtraFramesElementsComponent
import CreateLoadDBComponent 
import ResourceFrameComponent
import SiteFrameComponent
import ServiceCalendarFrameComponent
import ServiceFrameComponent
import TimetableFrameComponent
import SupportUtilitiesSubComponent
import gzip
import shutil
import datetime
import time

class StartConversionProcess():

  parser=argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)

  parser.add_argument('--folder', dest='folder', action='store', required=True, help=textwrap.dedent('''\
                                                                        Name of the GTFS expanded relative subfolder. 
                                                                        No blanks allowed. 
                                                                        Recommended examples for Liguria: 
                                                                        GTFS-RL-RT, 
                                                                        GTFS-RL-TPLLINEA,
                                                                        GTFS-RL-AMT, 
                                                                        GTFS-RL-AMTEXT,
                                                                        GTFS-RL-ATC,
                                                                        GTFS-RL-TRENITALIA'''))
  parser.add_argument('--NUTS', dest='nuts', action='store', required=True, help=textwrap.dedent('''\
                                                                        Combination of country code/NUTS Level2. 
                                                                        Examples for Italy: 
                                                                        IT:ITC1 (Piemonte), 
                                                                        IT:ITC2 (Valle d''Aosta), 
                                                                        IT:ITC3 (Liguria), 
                                                                        IT:ITC4 (Lombardia), 
                                                                        IT:ITF1 (Abruzzo), 
                                                                        IT:ITF2 (Molise), 
                                                                        IT:ITF3 (Campania), 
                                                                        IT:ITF4 (Puglia), 
                                                                        IT:ITF5 (Basilicata), 
                                                                        IT:ITF6 (Calabria), 
                                                                        IT:ITG1 (Sicilia), 
                                                                        IT:ITG2 (Sardegna), 
                                                                        IT:ITH1 (Provincia Autonoma di Bolzano/Bozen), 
                                                                        IT:ITH2 (Provincia Autonoma di Trento), 
                                                                        IT:ITH3 (Veneto), 
                                                                        IT:ITH4 (Friuli-Venezia Giulia), 
                                                                        IT:ITH5 (Emilia-Romagna), 
                                                                        IT:ITI1 (Toscana), 
                                                                        IT:ITI2 (Umbria), 
                                                                        IT:ITI3 (Marche), 
                                                                        IT:ITI4 (Lazio),
                                                                        IT:ITZZ (Extra-Regio NUTS 1) '''))
  parser.add_argument('--db', dest='db', action='store', required=True, help=textwrap.dedent('''\
                                                                        Name of the working database. 
                                                                        No blanks allowed. 
                                                                        Recommended examples for Liguria: 
                                                                        RT, 
                                                                        TPLLINEA,
                                                                        AMT, 
                                                                        AMTEXT,
                                                                        ATC,
                                                                        TRENITALIA''')) 
  parser.add_argument('--az', dest='az', action='store', required=True, help=textwrap.dedent('''\
                                                                        Acronym of the Transport Agency. 
                                                                        No blanks allowed. 
                                                                        Recommended examples for Liguria: 
                                                                        RT,
                                                                        TPLLINEA,
                                                                        AMT, 
                                                                        AMTEXT,
                                                                        ATC,
                                                                        TRENITALIA''')) 
  parser.add_argument('--vat', dest='vat', action='store', required=True, help=textwrap.dedent('''\
                                                                        11-chars VAT Number (Partita IVA) of the Transport Agency. 
                                                                        No blanks allowed. 
                                                                        Examples of correct values: 
                                                                        12345678903,
                                                                        44444444440,
                                                                        null,
                                                                        00000000000
                                                                        Examples of wrong values: 
                                                                        123,
                                                                        00000+00000,
                                                                        xxxxxxxxxxxx,
                                                                        123456789012
                                                                        00000000001'''))                                                                         
  parser.add_argument('--version', dest='version', action='store', required=True, help=textwrap.dedent('''\
                                                                        NeTEx file version for the Agency in the year:
                                                                        reccomended syntax [yy][Agency acronym][2-digits progressive in the year]
                                                                        Avoid blanks or special chars
                                                                        Examples:
                                                                        23AMT01,
                                                                        23ATC02,
                                                                        23TPERBO02
                                                                        '''))

  args=parser.parse_args()  

  #global iFolder, iNUTS, iDb, iAz, iStartDay, iEndDay, GTFS_exploded_feed_folder, iVersion

  global iFolder, iNUTS, iDb, iAz, GTFS_exploded_feed_folder, iVAT, iVersion

  iFolder=args.folder
  iNUTS=args.nuts
  iDb=args.db
  iAz=args.az
  iVAT=args.vat
  iVersion=args.version

  GTFS_exploded_feed_folder=iFolder

  if os.path.exists(GTFS_exploded_feed_folder ):
    print('Exploded GTFS feed folder is: ', GTFS_exploded_feed_folder)
  else:
    print('Cannot find a directory named: ', GTFS_exploded_feed_folder)
    exit()

###############################################
  VATNumber=iVAT

  VATCheck=SupportUtilitiesSubComponent.StringUtilities
  VATCheckClean=VATCheck.controllaPIVA(VATNumber)
  if VATCheckClean == False:
    exit()
  else:
    print('VAT number is well-formed.')

##############################################
  CheckMandatoryFilesComponent_start=CheckMandatoryFilesComponent.CheckMandatoryFilesProcessing
  CheckMandatoryFilesComponent_start.checkMandatoryFiles(GTFS_exploded_feed_folder)

  Path(GTFS_exploded_feed_folder + os.sep +iDb + '.db').touch()

  def sequenceComponents(self):
        print("Start processing feed...")

        iNUTSDash=iNUTS.replace(":","-")
#        print("iNUTSDash: ", iNUTSDash)
        
        startTime=time.time()
     
        # importing GTFS files and loading support database
        CreateLoadDB_component_start=CreateLoadDBComponent.StartImportProcess
        
        CreateLoadDB_component_start.importGTFSFiles(self, GTFS_exploded_feed_folder, iDb)        

        conn=sqlite3.connect(GTFS_exploded_feed_folder + '/' +iDb + '.db')
        c=conn.cursor()

		    # read start day (cut) and end day (cut) from 
        c.execute('''SELECT MIN(date_start_min), MAX(date_end_max) FROM srv_calendar_dates_summary''')
        records=c.fetchall()
        for row in records:
          if(row[0]):
            iStartDayCut=row[0]
            print("iStartDayCut: ", iStartDayCut)
          if(row[1]):
            iEndDayCut=row[1]
            print("iEndDayCut: ", iEndDayCut)     

        # read earliest day and latest day for synthetic calendar
        c.execute('''SELECT MIN(candidate_date), MAX(candidate_date) FROM srv_dates_range''')
        records=c.fetchall()
        for row in records:
          if(row[0]):
            iStartDay0=row[0]
            iStartDay=iStartDay0[0:4]+"-01-01" 
            print(">>>>>>>>>>>>>>>>>>>>>>>>>>> Earliest date for synthetic calendar: ", iStartDay0, " ======> ", iStartDay)
          if(row[1]):
            iEndDay0=row[1]
            iEndDay=iEndDay0[0:4]+"-12-31" 
            print(">>>>>>>>>>>>>>>>>>>>>>>>>>> Latest date for synthetic calendar: ", iEndDay0, " ======> ", iEndDay)               
        
        print("NeTEx version for the Agency: ", iVersion)   

        # create extra-frames element files
        CreateExtraFramesElements_component_start=CreateExtraFramesElementsComponent.CreateExtraFramesElementsProcessing
        CreateExtraFramesElements_component_start.processExtraFramesElements(self, GTFS_exploded_feed_folder, iNUTS, iAz, iStartDay, iEndDay, iStartDayCut, iEndDayCut)           

        # creating NeTEx ResourceFrame
        ResourceFC_component_start=ResourceFrameComponent.ResourceFrameProcessing
        ResourceFC_component_start.processResourceFrame(self, GTFS_exploded_feed_folder, iNUTS, iDb, iAz, iStartDayCut, iEndDayCut, iVersion, iVAT)
        # creating NeTEx SiteFrame        
        SiteFC_component_start=SiteFrameComponent.SiteFrameProcessing
        SiteFC_component_start.processSiteFrame(self, GTFS_exploded_feed_folder, iNUTS, iDb, iAz, iVersion)        
        # creating NeTEx ServiceCalendarFrame        
        ServiceCalendarFC_component_start=ServiceCalendarFrameComponent.ServiceCalendarFrameProcessing
        ServiceCalendarFC_component_start.processServiceCalendarFrame(self, GTFS_exploded_feed_folder, iNUTS, iDb, iAz, iStartDay, iEndDay, iStartDayCut, iEndDayCut, iVersion)         
        # creating NeTEx ServiceFrame        
        ServiceFC_component_start=ServiceFrameComponent.ServiceFrameProcessing
        ServiceFC_component_start.processServiceFrame(self, GTFS_exploded_feed_folder, iNUTS, iDb, iAz, iVersion, iVAT)      
        #creating NeTEx TimetableFrame        
        TimetableFC_component_start=TimetableFrameComponent.TimetableFrameProcessing
        TimetableFC_component_start.processTimetableFrame(self, GTFS_exploded_feed_folder, iNUTS, iDb, iAz, iVersion, iVAT)       

        # sequencing the temporary XML files in a single output NeTEx (XML) file
        dataFull=""
        dataPreXMLText=""
        dataResourceFrameXMLText=""
        dataSiteFrameXMLText=""
        dataServiceCalendarFrameXMLText=""
        dataServiceFrameXMLText=""
        dataTimetableFrameXMLText=""
        dataPostXMLText=""   

        tempExtraFramesElementsPreXMLData=GTFS_exploded_feed_folder + os.sep + iAz + '-ExtraFramesPreXMLText-temp.xml'  
        tempResourceFrameXMLData=GTFS_exploded_feed_folder + os.sep + iAz + '-ResourceFrame-temp.xml'
        tempSiteFrameXMLData=GTFS_exploded_feed_folder + os.sep + iAz + '-SiteFrame-temp.xml'
        tempServiceCalendarFrameXMLData=GTFS_exploded_feed_folder + os.sep + iAz + '-ServiceCalendarFrame-temp.xml'
        tempServiceFrameXMLData=GTFS_exploded_feed_folder + os.sep + iAz + '-ServiceFrame-temp.xml'
        tempTimetableFrameXMLData=GTFS_exploded_feed_folder + os.sep + iAz + '-TimetableFrame-temp.xml'
        tempExtraFramesElementsPostXMLData=GTFS_exploded_feed_folder + os.sep + iAz + '-ExtraFramesPostXMLText-temp.xml'  

        finalXMLFile=GTFS_exploded_feed_folder + os.sep + iNUTSDash + '-' + iAz + '-NeTEx_L1.xml'  
        finalGZFile=finalXMLFile + '.gz'        

        with open(tempExtraFramesElementsPreXMLData) as fp:  
          dataPreXMLText=fp.read()
        with open(tempResourceFrameXMLData) as fp:  
          dataResourceFrameXMLText=fp.read()         
        with open(tempSiteFrameXMLData) as fp:  
          dataSiteFrameXMLText=fp.read()  
        with open(tempServiceCalendarFrameXMLData) as fp:  
          dataServiceCalendarFrameXMLText=fp.read()  
        with open(tempServiceFrameXMLData) as fp:  
          dataServiceFrameXMLText=fp.read()  
        with open(tempTimetableFrameXMLData) as fp:  
          dataTimetableFrameXMLText=fp.read()
        with open(tempExtraFramesElementsPostXMLData) as fp:  
          dataPostXMLText=fp.read()          

        print("Queing temporary files...")

        #dataFull += '\n'
        dataFull += dataPreXMLText + '\n'
        dataFull += dataResourceFrameXMLText + '\n'
        dataFull += dataSiteFrameXMLText + '\n'
        dataFull += dataServiceCalendarFrameXMLText + '\n'
        dataFull += dataServiceFrameXMLText + '\n'
        dataFull += dataTimetableFrameXMLText + '\n'     
        dataFull += dataPostXMLText    

        with open(finalXMLFile, 'w', encoding='utf-8') as fp:
          fp.write(dataFull)   
        
        with open(finalXMLFile, 'rb') as f_in:         
          with gzip.open(finalGZFile, 'wb') as f_out:
            print("Compressing XML file...")
            shutil.copyfileobj(f_in, f_out)  

        # clean up temporary files
        print("Clean up temporary xml files...")
        fileList=glob.glob(GTFS_exploded_feed_folder + os.sep + '*-temp.xml')  
        for filePath in fileList:
          try:
            os.remove(filePath)
          except:
            print("Error while deleting file : ", filePath)   

        endTime=time.time()

        tempTime=endTime-startTime
        hours=tempTime//3600
        tempTime=tempTime-3600*hours
        minutes=tempTime//60
        seconds=tempTime-60*minutes

        print("Time elapsed: "+ '%02d:%02d:%02d' %(hours,minutes,seconds))                             

def main():  
  app=StartConversionProcess()
  app.sequenceComponents()
if __name__ == '__main__':
  main()