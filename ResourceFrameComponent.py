import sqlite3
import os
import SupportUtilitiesSubComponent

acroMOT=""

class ResourceFrameProcessing():

	def processResourceFrame(self, iGTFSExplodedFeedFolder, iNUTS, iDb, iAz, iStartDayCut, iEndDayCut, iVersion, iVAT):
		print('===> Processing ResourceFrame...')
		conn=sqlite3.connect(iGTFSExplodedFeedFolder + '/' +iDb + '.db')
		c=conn.cursor()

		# import agency.txt
		c.execute('''SELECT agency_id, agency_name, agency_url, agency_timezone, agency_lang, agency_phone, agency_fare_url, agency_email FROM tb_agency''')
		records=c.fetchall()
		print("Agency.txt => total rows are:  ", len(records))
		#print("Printing each row")

		leftText="""<ResourceFrame id="epd:it:ResourceFrame_EU_PI_COMMON:ita" version="1"><organisations>"""
		innerText=""

		for row in records:

			nameAgencyClean=""
			if(row[1]):
				nameAgency=SupportUtilitiesSubComponent.StringUtilities
				nameAgencyClean=nameAgency.filterOutNotMultilingualChars(self, row[1])

			innerText= innerText + """
									<Operator id="%s:Operator:%s:%s:%s" version="%s">  
										<Name>%s</Name> 
										<ShortName></ShortName>
										<ContactDetails>
											<Email>%s</Email> 
											<Phone>%s</Phone> 
											<Url>%s</Url> 
										</ContactDetails>
										<OrganisationType>operator</OrganisationType>
										<Address id="%s:Address:%s%s:1"> 
											<CountryName>Italia</CountryName>
											<Street></Street>
											<Town></Town>
											<PostCode></PostCode>
										</Address>
									</Operator>""" % (
			######## <Authority id> <=== ???
			######## <Name> <=== agency_name=row[1]
			######## <ShortName> <=== ???
			######## <Email> <=== ???
			######## <Phone> <=== agency_phone=row[5]
			######## <Url> <=== agency_url=row[2]
			######## <Address id> <=== ???
			######## <CountryName> <=== ???
			######## <Street> <=== ???
			######## <Town> <=== ???
			######## <Postcode> <=== ???
			######## <???> <=== agency_timezone=row[3]
			######## <???> <=== agency_lang=row[4]
			######## <???> <=== agency_fare_url=row[6]		
			#   row.agency_name, row.agency_phone, row.agency_url)
				# iNUTS, acroMOT, iAz, iAz, iVersion,
				# nameAgencyClean,
				# row[7],
				# row[5],
				# row[2],
				# iNUTS, acroMOT, iAz,
#				iNUTS, acroMOT, iAz, row[0], iVersion,
				iNUTS, iVAT, iAz, iAz, iVersion,				
				nameAgencyClean,
				row[7],
				row[5],
				row[2],
				iNUTS, acroMOT, iAz)

			rightText="""</organisations></ResourceFrame>"""

			with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-ResourceFrame-temp.xml', 'w', encoding='utf-8') as f: f.write((leftText))
			with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-ResourceFrame-temp.xml', 'a', encoding='utf-8') as f: f.write((innerText))
			with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-ResourceFrame-temp.xml', 'a', encoding='utf-8') as f: f.write((rightText))			

			c.close()


