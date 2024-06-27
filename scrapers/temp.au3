#include <Inet.au3>
#include <json.au3>
;~ #include <Array.au3>
#include <String.au3>
#include <File.au3>
#include <Date.au3>
#include <math.au3>



Local $HomeString, $AwayTeam1, $Momentum, $Markets, $MarketList[0][20], $aScoreArray[0][10], $EventID[0]
$Update = 0

$OutFiles = "C:\Temp\Tennis_CSV_Output"


While 1

	;$Update = $Update + 1

	Local $aMyTable

	;Read BA Output files to an array
	$aFileList = _FileListToArray($OutFiles, "*")

	;If new files found, process them
	If UBound($aFileList) > 0 Then

		For $x = 1 To UBound($aFileList) - 1
			$MarketName = StringReplace($aFileList[$x], "Output_", "")
			$MarketName = StringReplace($MarketName, ".csv", "")
			_ArraySearch($MarketList, $MarketName)
			If @error Then
				_ArrayAdd($MarketList, $MarketName)
			EndIf
			FileDelete("C:\Temp\Tennis_CSV_Output\" & $aFileList[$x])
		Next

		$URL = "https://api.sofascore.com/api/v1/sport/tennis/events/live"
		$data = _INetGetSource($URL)
		;Json_Dump($data)
		;Sleep(999999999)
		$object = json_decode($data)
		Local $Count = Json_Get($object, '.events')
		;MsgBox("", "", "Wait")

		For $d = UBound($MarketList) - 1 To 0 Step -1

			;If we dont have a EventId already, find it
			If $MarketList[$d][1] = "" Then

				Local $aFinalArray[0][5]

				For $i = 0 To UBound($Count) - 1


					$HomeTeam = Json_Get($object, '.events' & '[' & $i & '].homeTeam.name')
					$AwayTeam1 = Json_Get($object, '.events' & '[' & $i & '].awayTeam.name')

					Local $SplitTeams = _ReturnTeams()
					;_ArrayDisplay($SplitTeams)
					$Matched = 0

					For $t = 0 To UBound($SplitTeams) - 1
						If StringInStr($MarketList[$d][0], $SplitTeams[$t]) Then
							$Matched = $Matched + 1
						EndIf
					Next


					_ArrayAdd($aFinalArray, $MarketList[$d][0] & "|" & $HomeTeam & " v " & $AwayTeam1 & "|" & $i & "||" & $Matched)
					;_ArrayDisplay($aFinalArray)
				Next


				If UBound($aFinalArray) > 0 Then

					For $f = 0 To UBound($aFinalArray) - 1
						$FuzzyResult = _StringFuzzyCompareDamLevDist((StringReplace($aFinalArray[$f][0], " - Match Odds", "")), _StringReplaceAccent($aFinalArray[$f][1]))
						$aFinalArray[$f][3] = $FuzzyResult
					Next


					;Sort results array to find closest match. If only one match, check fuzzy result is OK
					_ArraySort($aFinalArray, 0, 0, 0, 3)
					;_ArrayDisplay($aFinalArray)

					If $aFinalArray[0][4] > 0 And $aFinalArray[0][3] < 20 Then
						$MarketList[$d][1] = Json_Get($object, '.events' & '[' & $aFinalArray[0][2] & '].id')
					EndIf
				EndIf

			EndIf
		Next
	EndIf


	;Loop through known markets and pull the stats

	;If we have markets, process them
	If UBound($MarketList) > 0 Then

		For $f = UBound($MarketList) - 1 To 0 Step -1
			If $MarketList[$f][1] = "" Then
				;If no ID matched then remove the market
				_ArrayDelete($MarketList, $f)
			EndIf
		Next

		;Create a new temp file
		FileDelete("C:\Temp\Sofascore_Tennis_Build.csv")
		FileWriteLine("C:\Temp\Sofascore_Tennis_Build.csv", "1")
		$File = FileOpen("C:\Temp\Sofascore_Tennis_Build.csv", 1)

		For $i = UBound($MarketList) - 1 To 0 Step -1

			$URL = "https://api.sofascore.com/api/v1/event/" & $MarketList[$i][1]
			$data = _INetGetSource($URL)
			;Json_Dump($data)
			;Sleep(9999999)
			$object = json_decode($data)

			If Json_Get($object, '.event.status.description') <> "Ended" Then

				;MsgBox("", "", "Wait")

				;Calls function to filter out unwanted match types (Women, U19 etc)
				;If _MatchCheck(Json_Get($object, '.events.tournament.name')) = "Yes" Then

				$Description = Json_Get($object, '.event.status.description ')

				If StringInStr($Description, "1st") Then $Set = 1
				If StringInStr($Description, "2nd") Then $Set = 2
				If StringInStr($Description, "3rd") Then $Set = 3
				If StringInStr($Description, "4th") Then $Set = 4
				If StringInStr($Description, "5th") Then $Set = 5

				;MsgBox("","",$Set)

				$HomeBuildString = $MarketList[$i][0] & ",*,*,1,E,SetNo," & $Set & ","
				$AwayBuildString = $MarketList[$i][0] & ",*,*,2,"


				_GetStats()
				;_GetPressure()

				;MsgBox("", "", $HomeBuildString)
				;MsgBox("", "", $AwayBuildString)

				FileWriteLine($File, $HomeBuildString)
				FileWriteLine($File, $AwayBuildString)
				ConsoleWrite(_NowTime() & " " & $MarketList[$i][0] & @CRLF)
			Else
				;Remove from market list and stop gathering data if match ended
				_ArrayDelete($MarketList, $i)
			EndIf

		Next

		FileClose($File)
		;Copy & Overwrite our temp file to master file in one hit (prevents file read/write contention between BA & script)
		FileCopy("C:\Temp\Sofascore_Tennis_Build.csv", "C:\Temp\Sofascore_Tennis_Final.csv", 1)
	EndIf

	Sleep(15000) ;00 seconds

WEnd

Func _GetStats()

	$URL = "https://api.sofascore.com/api/v1/event/" & $MarketList[$i][1] & "/statistics"
	;$URL = "https://api.sofascore.com/api/v1/event/" & Json_Get($object, '.events' & '[' & $MarketList[$i][1] & '].id') & "/statistics"
	$data3 = _INetGetSource($URL)

	;Json_Dump($data3)
	;MsgBox("","", "Wait")
	$object1 = json_decode($data3)

	If $object1 <> "" Then



		$HomeTeamAllFirstServeIn = 0
		$AwayTeamAllFirstServeIn = 0
		$HomeTeamAllFirstServePoints = 0
		$AwayTeamAllFirstServePoints = 0
		$HomeTeamAllSecondServeIn = 0
		$AwayTeamAllSecondServeIn = 0
		$HomeTeamAllSecondServePoints = 0
		$AwayTeamAllSecondServePoints = 0
		$HomeTeamOppReceivePoints = 0
		$AwayTeamOppReceivePoints = 0
		$aHomeBPAgainst = 0
		$aAwayBPAgainst = 0


		$HomeTeamAllFirstServeIn = Between(Json_Get($object1, '.statistics[0].groups[0].statisticsItems[2].home'))
		;MsgBox("","",$HomeTeamAllFirstServeIn)
		$AwayTeamAllFirstServeIn = Between(Json_Get($object1, '.statistics[0].groups[0].statisticsItems[2].away'))

		$HomeTeamAllFirstServePoints = Between(Json_Get($object1, '.statistics[0].groups[0].statisticsItems[4].home'))
		$AwayTeamAllFirstServePoints = Between(Json_Get($object1, '.statistics[0].groups[0].statisticsItems[4].away'))

		$HomeTeamAllSecondServeIn = Between(Json_Get($object1, '.statistics[0].groups[0].statisticsItems[3].home'))
		$AwayTeamAllSecondServeIn = Between(Json_Get($object1, '.statistics[0].groups[0].statisticsItems[3].away'))

		$HomeTeamAllSecondServePoints = Between(Json_Get($object1, '.statistics[0].groups[0].statisticsItems[5].home'))
		$AwayTeamAllSecondServePoints = Between(Json_Get($object1, '.statistics[0].groups[0].statisticsItems[5].away'))

		$HomeTeamOppReceivePoints = Json_Get($object1, '.statistics[0].groups[2].statisticsItems[1].away')
		$AwayTeamOppReceivePoints = Json_Get($object1, '.statistics[0].groups[2].statisticsItems[1].home')

		$aHomeBPAgainst = _StringBetween(Json_Get($object1, '.statistics[0].groups[0].statisticsItems[6].home'), "/", " ")
		$aAwayBPAgainst = _StringBetween(Json_Get($object1, '.statistics[0].groups[0].statisticsItems[6].away'), "/", " ")

		$HomeBPSaved = Between(Json_Get($object1, '.statistics[0].groups[0].statisticsItems[6].home'))
		$AwayBPSaved = Between(Json_Get($object1, '.statistics[0].groups[0].statisticsItems[6].away'))



		;_ArrayDisplay($aHomeBPAgainst)

		If UBound($aHomeBPAgainst) > 0 Then
			$HomeBPAgainst = $aHomeBPAgainst[0]
		Else
			$HomeBPAgainst = "0"
		EndIf

		If UBound($aAwayBPAgainst) > 0 Then
			$AwayBPAgainst = $aAwayBPAgainst[0]
		Else
			$AwayBPAgainst = "0"
		EndIf

		;MsgBox($MB_SYSTEMMODAL, "", $HomeBPAgainst)

		$HomeBuildString = $HomeBuildString & "S,1stServeIn%," & $HomeTeamAllFirstServeIn & ",S,1stServePointsWon%," & $HomeTeamAllFirstServePoints & ",S,2ndServeIn%," & $HomeTeamAllSecondServeIn & ",S,2ndServePointsWon%," & $HomeTeamAllSecondServePoints & ",S,OppReceivePointsWon," & $HomeTeamOppReceivePoints & ",S,BPAgainst," & $HomeBPAgainst & ",S,BPSaved%," & $HomeBPSaved

		$AwayBuildString = $AwayBuildString & "S,1stServeIn%," & $AwayTeamAllFirstServeIn & ",S,1stServePointsWon%," & $AwayTeamAllFirstServePoints & ",S,2ndServeIn%," & $AwayTeamAllSecondServeIn & ",S,2ndServePointsWon%," & $AwayTeamAllSecondServePoints & ",S,OppReceivePointsWon," & $AwayTeamOppReceivePoints & ",S,BPAgainst," & $AwayBPAgainst & ",S,BPSaved%," & $AwayBPSaved

	Else
		$MarketList[$i][1] = ""

	EndIf

EndFunc   ;==>_GetStats


Func _GetPressure()

	$URL = "https://api.sofascore.com/api/v1/event/" & $MarketList[$i][1] & "/graph"
	$data2 = _INetGetSource($URL)
	$object2 = json_decode($data2)

	If $data2 <> "" Then


	EndIf
EndFunc   ;==>_GetPressure




Func _MatchCheck($Word)
	If StringInStr($Word, "Esports") Then
		Return "No"
		;ElseIf StringInStr($Word, "Friend") Then
		;	Return "No"
		;ElseIf StringInStr($Word, "Women") Then
		;Return "No"
		;ElseIf StringInStr($Word, "(W)") Then
		;Return "No"
		;ElseIf StringInStr($Word, "Reserves") Then
		;	Return "No"
		;ElseIf StringInStr($Word, "U16") Then
		;	Return "No"
		;ElseIf StringInStr($Word, "U17") Then
		;	Return "No"
		;ElseIf StringInStr($Word, "Youth") Then
		;	Return "No"
		;ElseIf StringInStr($Word, "U19") Then
		;	Return "No"
		;ElseIf StringInStr($Word, "U21") Then
		;	Return "No"
		;ElseIf StringInStr($Word, "U20") Then
		;	Return "No"
		;ElseIf StringInStr($Word, "Cup") Then
		;	Return "No"
		;ElseIf StringInStr($Word, "Copa") Then
		;	Return "No"
	Else
		Return "Yes"
	EndIf
EndFunc   ;==>_MatchCheck





Func _ReturnTeams()

	Local $TeamArray = StringSplit($HomeTeam, " ", 2)
	Local $AwayTeam = StringSplit($AwayTeam1, " ", 2)
	Local $TeamFinalFunc[0]
	_ArrayConcatenate($TeamArray, $AwayTeam)

	For $f = 0 To UBound($TeamArray) - 1
		$TeamArray[$f] = StringStripWS($TeamArray[$f], 8)
		$TeamArray[$f] = _StringReplaceAccent($TeamArray[$f])
	Next

	;_ArrayDisplay($TeamArray)


	For $x = 0 To UBound($TeamArray) - 1

		If StringInStr($TeamArray[$x], "-") And StringLen($TeamArray[$x]) > 4 Then
			$HyphenSplit = StringSplit($TeamArray[$x], "-", 2)
			;_ArrayDisplay($HyphenSplit)
			For $d = 0 To UBound($HyphenSplit) - 1
				If StringLen($HyphenSplit[$d]) > 3 And _NameCheck($TeamArray[$x]) = "Yes" Then
					_ArrayAdd($TeamFinalFunc, StringLeft($HyphenSplit[$d], 6))
				EndIf
			Next
		ElseIf StringLen($TeamArray[$x]) > 3 And _NameCheck($TeamArray[$x]) = "Yes" Then
			_ArrayAdd($TeamFinalFunc, StringLeft($TeamArray[$x], 6))
		EndIf
	Next

	;_ArrayDisplay($TeamFinalFunc)


	Return ($TeamFinalFunc)


EndFunc   ;==>_ReturnTeams

Func _StringReplaceAccent($sString)
	Local $exp, $rep
	Local $Pattern[29][2] = [ _
			["[ÀÁÂÃÅÆ]", "A"], ["[àáâãåąə]", "a"], ["Ä", "Ae"], ["[æä]", "ae"], _
			["Þ", "B"], ["þ", "b"], _
			["ÇĆ", "C"], ["[çćč]", "c"], _
			["[ÈÉÊË]", "E"], ["[èéêë]", "e"], _
			["[ÌÍÎÏ]", "I"], ["[ìíîïı]", "i"], _
			["Ñ", "N"], ["ñ", "n"], _
			["[ÒÓÔÕÖØ]", "O"], ["[ðòóôõöø]", "o"], _
			["ř", "r"], _
			["[ŠŚ]", "S"], ["[šş]", "s"], _
			["ß", "Ss"], _
			["Ț", "T"], _
			["[ÙÚÛ]", "U"], ["[ùúû]", "u"], ["Ü", "U"], ["ü", "ue"], _
			["Ý", "Y"], ["[ýýÿ]", "y"], _
			["Ž", "Z"], ["ž", "z"]]

	For $i = 0 To (UBound($Pattern) - 1)
		$exp = $Pattern[$i][0]
		If $exp = "" Then ContinueLoop
		$rep = $Pattern[$i][1]

		$sString = StringRegExpReplace($sString, $exp, $rep)
		If @error == 0 And @extended > 0 Then
			;ConsoleWrite($sString & @LF & "--> " & $exp & @LF)
		EndIf
	Next

	Return $sString
EndFunc   ;==>_StringReplaceAccent


Func _NameCheck($Word)
	Return "Yes"
EndFunc   ;==>_NameCheck


Func _StringFuzzyCompareDamLevDist($sString1, $sString2)
	Local $iString1Len = StringLen($sString1)
	Local $iString2Len = StringLen($sString2)
	Select
		Case $iString1Len = 0
			SetError(1)
			Return -1
		Case $iString2Len = 0
			SetError(2)
			Return -1
	EndSelect

	Local $aiMatrix[$iString1Len + 1][$iString2Len + 1]
	Local $iString1Loop, $iString2Loop, $iCost

	For $iString1Loop = 0 To $iString1Len
		$aiMatrix[$iString1Loop][0] = $iString1Loop
	Next
	For $iString2Loop = 1 To $iString2Len
		$aiMatrix[0][$iString2Loop] = $iString2Loop
	Next

	For $iString1Loop = 1 To $iString1Len
		For $iString2Loop = 1 To $iString2Len
			If StringMid($sString1, $iString1Loop, 1) = StringMid($sString2, $iString2Loop, 1) Then
				$iCost = 0
			Else
				$iCost = 1
			EndIf
			$aiMatrix[$iString1Loop][$iString2Loop] = _Min(_Min($aiMatrix[$iString1Loop - 1][$iString2Loop] + 1, $aiMatrix[$iString1Loop][$iString2Loop - 1] + 1), $aiMatrix[$iString1Loop - 1][$iString2Loop - 1] + $iCost)
			If ($iString1Loop > 1) And ($iString2Loop > 1) And (StringMid($sString1, $iString1Loop, 1) = StringMid($sString2, $iString2Loop - 1, 1)) And (StringMid($sString1, $iString1Loop - 1, 1) = StringMid($sString2, $iString2Loop, 1)) Then
				$aiMatrix[$iString1Loop][$iString2Loop] = _Min($aiMatrix[$iString1Loop][$iString2Loop], $aiMatrix[$iString1Loop - 2][$iString2Loop - 2] + $iCost)
			EndIf
		Next
	Next
	Return $aiMatrix[$iString1Len][$iString2Len]
EndFunc   ;==>_StringFuzzyCompareDamLevDist


Func Between($sString)
	Local $aArray = _StringBetween($sString, "(", ")")
	;_ArrayDisplay($aArray)
	If UBound($aArray) > 0 Then
		Return StringReplace($aArray[0], "%", "")
	EndIf
EndFunc   ;==>Between