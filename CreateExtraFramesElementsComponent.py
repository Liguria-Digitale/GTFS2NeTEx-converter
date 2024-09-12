import time 
import os
import SupportUtilitiesSubComponent

class CreateExtraFramesElementsProcessing():

	def processExtraFramesElements(self, iGTFSExplodedFeedFolder, iNUTS, iAz, iStartDay, iEndDay, iStartDayCut, iEndDayCut):
		print('===> Processing extra-frames elements...')

		#UTCTime=time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime())+time.strftime("%Z", time.gmtime())
		UTCTime=time.strftime('%Y-%m-%dT%H:%M:%S'+'Z')		

		fmtStartDayCut=""
		fmtStartDayCut_0=SupportUtilitiesSubComponent.StringUtilities
		fmtStartDayCut=fmtStartDayCut_0.formatShortDay2UTC(self, iStartDayCut)

		fmtEndDayCut=""
		fmtEndDayCut_0=SupportUtilitiesSubComponent.StringUtilities
		fmtEndDayCut=fmtEndDayCut_0.formatShortDay2UTC(self, iEndDayCut)

		preXMLText= """<?xml version="1.0" encoding="UTF-8"?>
				<!-- NeTEX italian Profile -->
				<PublicationDelivery xmlns:gml="http://www.opengis.net/gml/3.2" xmlns:siri="http://www.siri.org.uk/siri" version="1.10" xmlns="http://www.netex.org.uk/netex" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.netex.org.uk/netex ../../../netex-italian-profile-main/xsd/NeTEx_publication_EPIP.xsd">
					<PublicationTimestamp>%s</PublicationTimestamp>
					<ParticipantRef>%s-RAP</ParticipantRef>
					<Description>%s %s NeTEx Italian Profile</Description>
					<dataObjects>
						<CompositeFrame id="epd:it:CompositeFrame_EU_PI_STOP_OFFER:ita" version="1">
							<ValidBetween>
								<FromDate>%sT00:00:00.000+02:00</FromDate>
								<ToDate>%sT23:59:59.999+02:00</ToDate>
							</ValidBetween>
							<TypeOfFrameRef ref="epip:EU_PI_LINE_OFFER" versionRef="1"/>
							<!--- ======= CODESPACEs======== -->
							<codespaces>
								<Codespace id="ita">
									<Xmlns>ita</Xmlns>
									<XmlnsUrl>http://www.ita.it</XmlnsUrl>
									<Description>Italian Profile</Description>
								</Codespace>
							</codespaces>
							<!--- =======FRAME DEFAULTS======== -->
							<FrameDefaults>
								<DefaultCodespaceRef ref="ita"/>
							</FrameDefaults>
							<frames>""" % (UTCTime, iNUTS, iNUTS, iAz, fmtStartDayCut, fmtEndDayCut)

		postXMLText="""</frames>
					</CompositeFrame>
				</dataObjects>
			</PublicationDelivery>
		"""

		with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-ExtraFramesPreXMLText-temp.xml', 'w', encoding='utf-8') as f: f.write((preXMLText))
		with open(iGTFSExplodedFeedFolder + os.sep + iAz + '-ExtraFramesPostXMLText-temp.xml', 'w', encoding='utf-8') as f: f.write((postXMLText))


