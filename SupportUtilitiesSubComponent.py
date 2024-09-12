import re

class StringUtilities():

	def filterOutNotMultilingualChars(self, iString):

		specialCharsAmpersand="&"
		specialCharsGreaterThan=">"
		specialCharsLessThan="<"
		specialCharsApostrophe="'"
		specialCharsDeUMin="ü"
		specialCharsDeOMin="ö"
		specialCharsDeOMaj="Ö"

		outString=iString

		sCData="<![CDATA["
		eCData="]]>"

		if((specialCharsAmpersand in iString) or (specialCharsGreaterThan in iString) or (specialCharsLessThan in iString) 
			or (specialCharsApostrophe in iString) or (specialCharsDeUMin in iString) or (specialCharsDeOMin in iString) or (specialCharsDeOMaj in iString)):
			outString=sCData + iString + eCData
		else:
			outString=iString
		

		return outString


	def formatShortDay2UTC(self, iString):

		outString=iString[0:4] + "-" + iString[4:6] + "-" + iString[6:8] 

		return outString


	def controllaPIVA(partita_iva):

# Controllo partita IVA.
# Author: Gabriele Muciaccia <gabrielemuciacciaATonenetbeyond.org>
# Version: 2016-08-21
# Parameters:
# partita_iva: stringa vuota o partita IVA di 11 cifre.
# Return: stringa vuota se il codice di controllo della partita IVA
# e' corretto oppure se e' la stringa vuota, altrimenti un
# messaggio di errore..

		outString=True

		IVA_REGEXP = "^[0-9]{11}$"
		ORD_ZERO = ord('0')

		if 0 == len(partita_iva):
			print('Wrong')
			outString=False
			return outString
		if 11 != len(partita_iva):
			print('La lunghezza della partita IVA non è corretta; dovrebbe essere lunga esattamente 11 caratteri.\n')
			outString=False
			return outString
		match = re.match(IVA_REGEXP, partita_iva)
		if not match:
			print('La partita IVA contiene dei caratteri non ammessi: dovrebbe contenere solo cifre.\n')
		s = 0
		for i in range(0, 10, 2):
			s += ord(partita_iva[i]) - ORD_ZERO
		for i in range(1, 10, 2):
			c = 2 * (ord(partita_iva[i]) - ORD_ZERO)
			if c > 9:
				c -= 9
			s += c

		if (10 - s%10)%10 != ord(partita_iva[10]) - ORD_ZERO:
			print('La partita IVA non è valida: il codice di controllo non corrisponde.')
			outString=False
			return outString

		
	