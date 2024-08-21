USE [GDB_01_001]
GO
/****** Object:  StoredProcedure [dbo].[Hadco_Spotio_CustomerFile]    Script Date: 8/21/2024 3:31:57 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].[Hadco_Spotio_CustomerFile]
AS

DECLARE @saveas varchar(1000)
DECLARE @query varchar(8000)
DECLARE @cmdquery varchar(8000)
DECLARE @filename varchar(100)
DECLARE @date varchar(10)
DECLARE @directory varchar(100)
DECLARE @directorycontents varchar(100)

BEGIN	
	--Unless specified, manifest date is assumed to be today if extract pulled before noon
	--, tomorrow if pulled after noon, Monday if pulled on Friday
	SET @date = REPLACE(CONVERT(VARCHAR(10), GETDATE(), 111), '/', '_')

	SET @filename = 'Customer_file' 
		-- +'_' + @date
		+ '.csv'

	SET @directory = 'D:\CallReportFiles\spotio\upload'
	SET @directorycontents = 'D:\CallReportFiles\spotio\upload\*'

	--Delete any existing files in directory
	EXEC master.sys.xp_delete_files @directorycontents;

	--Create perm file with column headers
	SET @query='select ''ID'',''Address (house # & street name)'',''House #'',''Street'',''City'',''ZIP'',''State'',''Country'',''Longitude'',''Latitude'',''Creation date'',''CUSTOMER OWNER EMAIL'',''Last Visit Result'',''Last Visit Date'',''Visit Count''
	,''Field Company'',''Field Customer ID'',''Field Unit'',''Field Account Type'',''Field Contact #1 First Name'',''Field Contact #1 Last Name'',''Field Contact #1 Title'',''Field Contact #1 Email'',''Field Contact #1 Phone'',''Field Customer Priority'',''Field OS Focus''
	,''Field Estimated Annual Purchases USD'',''Field Estimated Number of Employees'',''Field Estimated Number of Machines'',''Field Estimated Building Size SF'',''Field Material Used'',''Field Website'',''Field Inside Person'',''Field Inside Person Email'',''Field Office Manager'',''Field Office Manager Email'',''Field Added Date''
	,''Field Contact #2 First Name'',''Field Contact #2 Last Name'',''Field Contact #2 Title'',''Field Contact #2 Email'',''Field Contact #2 Phone'',''Field Contact #3 First Name'',''Field Contact #3 Last Name'',''Field Contact #3 Title'',''Field Contact #3 Email'',''Field Contact #3 Phone''
	,''Field Current Year Sales'',''Field Last Year Sales'',''Field Prior Year Sales'',''Field Current Year Orders'',''Field Last Year Orders'',''Field Prior Year Orders'',''Field Current Year Quotes'',''Field Last Year Quotes'',''Field Prior Year Quotes'',''Field Terms'',''Field Not Due A/R''
	,''Field Late A/R 0-30'',''Field Late A/R 31-60'',''Field Late A/R 61+'',''Field Late A/R 61-90'',''Field Late A/R 91-120'',''Field Late A/R 120+'',''Field Quotes in Last 7 Days''
	,''Field Quotes in Last 30 Days'',''Field Quotes in Last 3 Months'',''Field Drop in Sales Last 30 Days'',''Field Drop in Sales Last 90 Days'',''Field Drop in Sales Last 180 Days'',''Field New Products Sales Last 90 Days'',''Field Plastic Quotes in Last 180 Days'',''Field Activity  in Last 3 months'',''Stage'''
	SET @saveas = @directory + '\' + @filename
	SET @cmdquery = 'bcp "' + replace(@query, char(10), '') + '" QUERYOUT "' + @saveas 
	+ '" -c -t, -T -S ' + @@servername 
	EXEC master..xp_cmdshell @cmdquery		

	--Job runs at 6 am, then again at 11 am after corrections are made
	--On first run, move previous values to saved data
	DECLARE @lastrun datetime
	SELECT @lastrun = MAX([Data Date]) FROM Hadco_Spotio_CustomerData_LastValues

	IF DATEDIFF(DAY, @lastrun, GETDATE()) > 0
	BEGIN
		DELETE FROM Hadco_Spotio_CustomerData_PreviousValues
		INSERT INTO Hadco_Spotio_CustomerData_PreviousValues
		SELECT * FROM Hadco_Spotio_CustomerData_LastValues
	END

	--Fill in current values
	DELETE FROM Hadco_Spotio_CustomerData_LastValues
	--Region populated
	INSERT INTO Hadco_Spotio_CustomerData_LastValues
	(
		[Address (house # & street name)] 
		, [House #]
		, [Street] 
		, [City] 
		, [ZIP]
		, [State] 
		, [Country]
		, [Longitude] 
		, [Latitude]
		, [Creation date] 
		, [CUSTOMER OWNER EMAIL] 
		, [Last Visit Result]
		, [Last Visit Date]
		, [Visit Count]
		, [External Customer ID]
		, [External Pin ID]
		, [Field Company] 
		, [Field Customer ID] 
		, [Field Unit] 
		, [Field Account Type] 
		, [Field Contact #1 First Name] 
		, [Field Contact #1 Last Name]
		, [Field Contact #1 Title] 
		, [Field Contact #1 Email] 
		, [Field Contact #1 Phone] 
		, [Field Customer Priority] 
		, [Field High Priority Customer] 
		, [Field Estimated Annual Purchases USD] 
		, [Field Estimated Number of Employees] 
		, [Field Estimated Number of Machines] 
		, [Field Estimated Building Size SF] 
		, [Field Material Used] 
		, [Field Website] 
		, [Field Inside Person] 
		, [Field Inside Person Email] 
		, [Field Office Manager] 
		, [Field Office Manager Email] 
		, [Field Added Date]
		, [Added Date] 
		, [Field Contact #2 First Name]
		, [Field Contact #2 Last Name]
		, [Field Contact #2 Title] 
		, [Field Contact #2 Email] 
		, [Field Contact #2 Phone] 
		, [Field Contact #3 First Name]
		, [Field Contact #3 Last Name] 
		, [Field Contact #3 Title] 
		, [Field Contact #3 Email] 
		, [Field Contact #3 Phone]
		, [Field Current Year Sales]
		, [Field Last Year Sales]
		, [Field Prior Year Sales]
		, [Field Current Year Orders]
		, [Field Last Year Orders]
		, [Field Prior Year Orders]
		, [Field Current Year Quotes]
		, [Field Last Year Quotes] 
		, [Field Prior Year Quotes]
		, [Field Terms] 
		, [Field Not Due A/R] 
		, [Field Late A/R 0-30] 
		, [Field Late A/R 31-60]
		, [Field Late A/R 61+]
		, [Field Late A/R 61-90] 
		, [Field Late A/R 91-120]
		, [Field Late A/R 120+] 
		, [Field Quotes in Last 7 Days]
		, [Field Quotes in Last 30 Days]
		, [Field Quotes in Last 3 Months]
		, [Field Drop in Sales Last 30 Days] 
		, [Field Drop in Sales Last 90 Days] 
		, [Field Drop in Sales Last 180 Days] 
		, [Field New Products Sales Last 90 Days] 
		, [Field Plastic Quotes in Last 180 Days] 
		, [Field Activity  in Last 3 months] 
		--, [Field Days Since Last Active]
		, [Stage]
		, [Data Date]
		, [spotio_id]
	)
	SELECT 
		Dataset.[Address (house # & street name)], Dataset.[House #], Dataset.[Street], Dataset.[City], Dataset.[ZIP], Dataset.[State], Dataset.[Country], Dataset.[Longitude], Dataset.[Latitude], Dataset.[Creation date], Dataset.[CUSTOMER OWNER EMAIL], Dataset.[Last Visit Result], Dataset.[Last Visit Date], Dataset.[Visit Count], Dataset.[External Customer ID], Dataset.[External Pin ID], 
		Dataset.[Field Company], Dataset.[Field Customer ID], Dataset.[Field Unit], Dataset.[Field Account Type], Dataset.[Field Contact #1 First Name], Dataset.[Field Contact #1 Last Name], Dataset.[Field Contact #1 Title], Dataset.[Field Contact #1 Email], Dataset.[Field Contact #1 Phone], Dataset.[Field Customer Priority], Dataset.[Field High Priority Customer], 
		Dataset.[Field Estimated Annual Purchases USD], Dataset.[Field Estimated Number of Employees], Dataset.[Field Estimated Number of Machines], Dataset.[Field Estimated Building Size SF], Dataset.[Field Material Used], Dataset.[Field Website], Dataset.[Field Inside Person], Dataset.[Field Inside Person Email], Dataset.[Field Office Manager], Dataset.[Field Office Manager Email], Dataset.[Field Added Date], 
		Dataset.[Added Date], Dataset.[Field Contact #2 First Name], Dataset.[Field Contact #2 Last Name], Dataset.[Field Contact #2 Title], Dataset.[Field Contact #2 Email], Dataset.[Field Contact #2 Phone], Dataset.[Field Contact #3 First Name], Dataset.[Field Contact #3 Last Name], Dataset.[Field Contact #3 Title], Dataset.[Field Contact #3 Email], Dataset.[Field Contact #3 Phone], 
		Dataset.[Field Current Year Sales], Dataset.[Field Last Year Sales], Dataset.[Field Prior Year Sales], Dataset.[Field Current Year Orders], Dataset.[Field Last Year Orders], Dataset.[Field Prior Year Orders], Dataset.[Field Current Year Quotes], Dataset.[Field Last Year Quotes], Dataset.[Field Prior Year Quotes], Dataset.[Field Terms], Dataset.[Field Not Due A/R], 
		Dataset.[Field Late A/R 0-30], Dataset.[Field Late A/R 31-60], Dataset.[Field Late A/R 61+], Dataset.[Field Late A/R 61-90], Dataset.[Field Late A/R 91-120], Dataset.[Field Late A/R 120+], Dataset.[Field Quotes in Last 7 Days], 
		Dataset.[Field Quotes in Last 30 Days], Dataset.[Field Quotes in Last 3 Months], Dataset.[Field Drop in Sales Last 30 Days], Dataset.[Field Drop in Sales Last 90 Days], Dataset.[Field Drop in Sales Last 180 Days], Dataset.[Field New Products Sales Last 90 Days], Dataset.[Field Plastic Quotes in Last 180 Days], 
		CASE WHEN DATEDIFF(MONTH, (CASE WHEN [LAST SO DATE] >= [LAST QUOTE DATE] AND [LAST SO DATE] >= [Last SALES CALL Date] AND [LAST SO DATE] >= [Added Date] THEN [LAST SO DATE]
					WHEN [LAST QUOTE DATE] >= [LAST SO DATE] AND [LAST QUOTE DATE] >= [Last SALES CALL Date] AND [LAST QUOTE DATE] >= [Added Date] THEN [LAST QUOTE DATE]
					WHEN [Last SALES CALL Date] >= [LAST SO DATE] AND [Last SALES CALL Date] >= [LAST QUOTE DATE] AND [Last SALES CALL Date] >= [Added Date] THEN [Last SALES CALL Date]
					ELSE [Added Date] END), GETDATE()) <= 3 THEN 'Y' ELSE 'N' END AS 'Field Activity  in Last 3 months',
		/*DATEDIFF(DAY, (CASE WHEN [LAST SO DATE] >= [LAST QUOTE DATE] AND [LAST SO DATE] >= [Last SALES CALL Date] AND [LAST SO DATE] >= [Added Date] THEN [LAST SO DATE]
					WHEN [LAST QUOTE DATE] >= [LAST SO DATE] AND [LAST QUOTE DATE] >= [Last SALES CALL Date] AND [LAST QUOTE DATE] >= [Added Date] THEN [LAST QUOTE DATE]
					WHEN [Last SALES CALL Date] >= [LAST SO DATE] AND [Last SALES CALL Date] >= [LAST QUOTE DATE] AND [Last SALES CALL Date] >= [Added Date] THEN [Last SALES CALL Date]
					ELSE [Added Date] END), GETDATE()) AS 'Field Days Since Last Active',*/
		CASE WHEN [Field Customer Priority] IN ('X') THEN 'Not a Good Fit' 
			WHEN [Field Customer Priority] IN ('Z') THEN 'Business Closed' 
			WHEN DATEDIFF(DAY, [LAST SO DATE], GETDATE()) <= 365 THEN 'Customer' 
			WHEN DATEDIFF(DAY, [LAST SO DATE], GETDATE()) > 365 AND DATEDIFF(DAY, [LAST QUOTE DATE], GETDATE()) <= 365 THEN 'Active Prospect'
			ELSE 'Prospect' 
		END AS 'Stage'
		, GETDATE(), Dataset.[ID]
	FROM 
	(
		SELECT 
			ISNULL(ID.spotio_id,'') AS ID,
			ISNULL(C.REGION,'HOME') AS REGION,
			'"' + ISNULL(C.ADR1,'') + '"' AS 'Address (house # & street name)', 
			'' AS 'House #', 
			'' AS 'Street', 
			'"' + ISNULL(C.CITY,'') + '"' AS 'City', 
			'"' + ISNULL(LEFT(C.ZIP,5),'') + '"' AS 'ZIP', 
			'"' + ISNULL(C.STATE,'') + '"' AS 'State', 
			'"' + ISNULL(C.COUNTRY,'') + '"' AS 'Country',
			'' AS 'Longitude', '' AS 'Latitude', 
			CAST(C.ADDED_DTE AS DATE) AS 'Creation date', 
			CASE WHEN ISNULL(C.REGION, '') IN ('HOME','SR01','') THEN 'RDvir@hadco-metal.com'
					WHEN OUTSIDE.HTTP_PROXY_USER IS NULL THEN '' 
					ELSE CONCAT(RTRIM(OUTSIDE.HTTP_PROXY_USER),'@hadco-metal.com') 
				END AS 'CUSTOMER OWNER EMAIL',
			'' AS 'Last Visit Result',
			CAST((SELECT MAX(ENTERED_DATE) FROM CALL_TRACKING WHERE CALL_TRACKING.ACCTNO=C.ACCTNO AND CALL_TRACKING.ENTERED_BY IN (select ccode from userlist where depart in ('osls'))) AS DATE) AS 'Last Visit Date',
			'' AS 'Visit Count',
			C.ACCTNO AS 'External Customer ID',
			'' AS 'External Pin ID',
			'"' + REPLACE(C.NAME, '"', '') + '"' AS 'Field Company',
			C.ACCTNO AS 'Field Customer ID',
			'"' + C.ADR2 + '"' AS 'Field Unit',
			ISNULL(ACCT_TYPE.NAME,'') AS 'Field Account Type',
			'"' + ISNULL((select F_NAME from contacts where ccode = 'S01' and acctno = c.ACCTNO),'') + '"' AS 'Field Contact #1 First Name', 
			'"' + ISNULL((select L_NAME from contacts where ccode = 'S01' and acctno = c.ACCTNO),'') + '"' AS 'Field Contact #1 Last Name', 
			'"' + ISNULL((select TITLE from contacts where ccode = 'S01' and acctno = c.ACCTNO),'') + '"' AS 'Field Contact #1 Title',
			'"' + ISNULL((select EMAIL from contacts where ccode = 'S01' and acctno = c.ACCTNO),'') + '"' AS 'Field Contact #1 Email',
			'"' + ISNULL((select TEL1 from contacts where ccode = 'S01' and acctno = c.ACCTNO),'') + '"' AS 'Field Contact #1 Phone', 
			ISNULL(CV.PRIORITY,'') AS 'Field Customer Priority', 
			ISNULL(C.ALT3_CODE_C,'') AS 'Field High Priority Customer',
			'' AS 'Field Estimated Annual Purchases USD',
			'' AS 'Field Estimated Number of Employees',
			'' AS 'Field Estimated Number of Machines',
			'' AS 'Field Estimated Building Size SF',
			'' AS 'Field Material Used',
			'"' + ISNULL(C.WEB_SITE,'') + '"' AS 'Field Website',
			CONCAT(CONCAT(RTRIM(ISNULL(INSIDE.F_NAME,'')), ' '),RTRIM(ISNULL(INSIDE.L_NAME,''))) AS 'Field Inside Person',
			CASE WHEN INSIDE.HTTP_PROXY_USER IS NULL THEN '' ELSE CONCAT(RTRIM(INSIDE.HTTP_PROXY_USER),'@HADCO-METAL.COM') END AS 'Field Inside Person Email',
			CASE WHEN INSIDE.DIVISION IN ('04','05','03','02') THEN 'Colleen Pierson'
					 WHEN INSIDE.DIVISION IN ('01') THEN 'Chris Harmse'
					 WHEN INSIDE.DIVISION IN ('07','70','71') THEN 'Robert Worth'
				ELSE '' END AS 'Field Office Manager',
			CASE WHEN INSIDE.DIVISION IN ('04','05','03','02') THEN 'ColleenP@hadco-metal.com'
					 WHEN INSIDE.DIVISION IN ('01') THEN 'ChrisH@hadco-metal.com'
					 WHEN INSIDE.DIVISION IN ('07','70','71') THEN 'RobertW@hadco-metal.com'
				ELSE '' END AS 'Field Office Manager Email',
			CAST(C.ADDED_DTE AS DATE) AS 'Field Added Date',
			CAST(ISNULL(C.ADDED_DTE, '01-01-2000') AS DATE) AS 'Added Date',
			'"' + ISNULL((select F_NAME from contacts where ccode = 'S02' and acctno = c.ACCTNO),'') + '"' AS 'Field Contact #2 First Name', 
			'"' + ISNULL((select L_NAME from contacts where ccode = 'S02' and acctno = c.ACCTNO),'') + '"' AS 'Field Contact #2 Last Name', 
			'"' + ISNULL((select TITLE from contacts where ccode = 'S02' and acctno = c.ACCTNO),'') + '"' AS 'Field Contact #2 Title',
			'"' + ISNULL((select EMAIL from contacts where ccode = 'S02' and acctno = c.ACCTNO),'') + '"' AS 'Field Contact #2 Email',
			'"' + ISNULL((select TEL1 from contacts where ccode = 'S02' and acctno = c.ACCTNO),'') + '"' AS 'Field Contact #2 Phone',
			'"' + ISNULL((select F_NAME from contacts where ccode = 's03' and acctno = c.ACCTNO),'') + '"' AS 'Field Contact #3 First Name', 
			'"' + ISNULL((select L_NAME from contacts where ccode = 's03' and acctno = c.ACCTNO),'') + '"' AS 'Field Contact #3 Last Name', 
			'"' + ISNULL((select TITLE from contacts where ccode = 's03' and acctno = c.ACCTNO),'') + '"' AS 'Field Contact #3 Title',
			'"' + ISNULL((select EMAIL from contacts where ccode = 's03' and acctno = c.ACCTNO),'') + '"' AS 'Field Contact #3 Email',
			'"' + ISNULL((select TEL1 from contacts where ccode = 's03' and acctno = c.ACCTNO),'') + '"' AS 'Field Contact #3 Phone',
			ISNULL((SELECT SUM(INV_LINE.DOC_TOTAL) FROM INV_LINE WITH (NOLOCK) LEFT JOIN INV_HDR ON INV_LINE.DOC_NO = INV_HDR.DOC_NO WHERE YEAR(POST_GL_DATE) = YEAR(GETDATE()) AND C.ACCTNO = INV_HDR.ACCTNO AND INV_HDR.DOC_TYPE='I' and INV_LINE.LINE_TYPE<>'02'),0) - 
			ISNULL((SELECT SUM(RMA_LINE.DOC_TOTAL) FROM RMA_LINE WITH (NOLOCK) LEFT JOIN RMA_HDR ON RMA_LINE.DOC_NO = RMA_HDR.DOC_NO WHERE YEAR(POST_GL_DATE) = YEAR(GETDATE()) AND C.ACCTNO = RMA_HDR.ACCTNO AND RMA_HDR.DOC_TYPE='M' and RMA_LINE.LINE_TYPE<>'02'),0) AS 'Field Current Year Sales',
			ISNULL((SELECT SUM(INV_LINE.DOC_TOTAL) FROM INV_LINE WITH (NOLOCK) LEFT JOIN INV_HDR ON INV_LINE.DOC_NO = INV_HDR.DOC_NO WHERE YEAR(POST_GL_DATE) = YEAR(GETDATE())-1 AND C.ACCTNO = INV_HDR.ACCTNO AND INV_HDR.DOC_TYPE='I' and INV_LINE.LINE_TYPE<>'02'),0) - 
			ISNULL((SELECT SUM(RMA_LINE.DOC_TOTAL) FROM RMA_LINE WITH (NOLOCK) LEFT JOIN RMA_HDR ON RMA_LINE.DOC_NO = RMA_HDR.DOC_NO WHERE YEAR(POST_GL_DATE) = YEAR(GETDATE())-1 AND C.ACCTNO = RMA_HDR.ACCTNO AND RMA_HDR.DOC_TYPE='M' and RMA_LINE.LINE_TYPE<>'02'),0) AS 'Field Last Year Sales',
			ISNULL((SELECT SUM(INV_LINE.DOC_TOTAL) FROM INV_LINE WITH (NOLOCK) LEFT JOIN INV_HDR ON INV_LINE.DOC_NO = INV_HDR.DOC_NO WHERE YEAR(POST_GL_DATE) = YEAR(GETDATE())-2 AND C.ACCTNO = INV_HDR.ACCTNO AND INV_HDR.DOC_TYPE='I' and INV_LINE.LINE_TYPE<>'02'),0) - 
			ISNULL((SELECT SUM(RMA_LINE.DOC_TOTAL) FROM RMA_LINE WITH (NOLOCK) LEFT JOIN RMA_HDR ON RMA_LINE.DOC_NO = RMA_HDR.DOC_NO WHERE YEAR(POST_GL_DATE) = YEAR(GETDATE())-2 AND C.ACCTNO = RMA_HDR.ACCTNO AND RMA_HDR.DOC_TYPE='M' and RMA_LINE.LINE_TYPE<>'02'),0) AS 'Field Prior Year Sales',
			ISNULL((SELECT SUM(SO_LINE.DOC_TOTAL) FROM SO_LINE WITH (NOLOCK) LEFT JOIN SO_HDR ON SO_LINE.DOC_NO = SO_HDR.DOC_NO WHERE YEAR(SO_HDR.ADDED_DTE) = YEAR(GETDATE()) AND C.ACCTNO = SO_HDR.ACCTNO),0) AS 'Field Current Year Orders',
			ISNULL((SELECT SUM(SO_LINE.DOC_TOTAL) FROM SO_LINE WITH (NOLOCK) LEFT JOIN SO_HDR ON SO_LINE.DOC_NO = SO_HDR.DOC_NO WHERE YEAR(SO_HDR.ADDED_DTE) = YEAR(GETDATE())-1 AND C.ACCTNO = SO_HDR.ACCTNO),0) AS 'Field Last Year Orders',
			ISNULL((SELECT SUM(SO_LINE.DOC_TOTAL) FROM SO_LINE WITH (NOLOCK) LEFT JOIN SO_HDR ON SO_LINE.DOC_NO = SO_HDR.DOC_NO WHERE YEAR(SO_HDR.ADDED_DTE) = YEAR(GETDATE())-2 AND C.ACCTNO = SO_HDR.ACCTNO),0) AS 'Field Prior Year Orders',
			ISNULL((SELECT SUM(QUOTE_LINE.DOC_TOTAL) FROM QUOTE_LINE WITH (NOLOCK) LEFT JOIN QUOTE_HDR ON QUOTE_LINE.DOC_NO = QUOTE_HDR.DOC_NO WHERE YEAR(QUOTE_HDR.ADDED_DTE) = YEAR(GETDATE()) AND C.ACCTNO = QUOTE_HDR.ACCTNO),0) AS 'Field Current Year Quotes',
			ISNULL((SELECT SUM(QUOTE_LINE.DOC_TOTAL) FROM QUOTE_LINE WITH (NOLOCK) LEFT JOIN QUOTE_HDR ON QUOTE_LINE.DOC_NO = QUOTE_HDR.DOC_NO WHERE YEAR(QUOTE_HDR.ADDED_DTE) = YEAR(GETDATE())-1 AND C.ACCTNO = QUOTE_HDR.ACCTNO),0) AS 'Field Last Year Quotes',
			ISNULL((SELECT SUM(QUOTE_LINE.DOC_TOTAL) FROM QUOTE_LINE WITH (NOLOCK) LEFT JOIN QUOTE_HDR ON QUOTE_LINE.DOC_NO = QUOTE_HDR.DOC_NO WHERE YEAR(QUOTE_HDR.ADDED_DTE) = YEAR(GETDATE())-2 AND C.ACCTNO = QUOTE_HDR.ACCTNO),0) AS 'Field Prior Year Quotes',
			ISNULL(TERMS.DESCRIP,'') AS 'Field Terms',
			ISNULL(AR_DATA.[NOT DUE],0) AS 'Field Not Due A/R',
			ISNULL(AR_DATA.[A/R 0-30],0) AS 'Field Late A/R 0-30',
			ISNULL(AR_DATA.[A/R 31-60],0) AS 'Field Late A/R 31-60',
			ISNULL(AR_DATA.[A/R 61+],0) AS 'Field Late A/R 61+',
			ISNULL(AR_DATA.[A/R 61-90],0) AS 'Field Late A/R 61-90',
			ISNULL(AR_DATA.[A/R 91-120],0) AS 'Field Late A/R 91-120',
			ISNULL(AR_DATA.[A/R 120+],0) AS 'Field Late A/R 120+',
			CASE WHEN ISNULL((SELECT SUM(QUOTE_LINE.DOC_TOTAL) FROM QUOTE_LINE WITH (NOLOCK) LEFT JOIN QUOTE_HDR WITH (NOLOCK) ON QUOTE_LINE.DOC_NO = QUOTE_HDR.DOC_NO WHERE QUOTE_HDR.ADDED_DTE >= DATEADD(DAY, -7, GETDATE()) AND C.ACCTNO = QUOTE_HDR.ACCTNO),0) > 0 THEN 'Y' ELSE 'N' END AS 'Field Quotes in Last 7 Days',
			CASE WHEN ISNULL((SELECT SUM(QUOTE_LINE.DOC_TOTAL) FROM QUOTE_LINE WITH (NOLOCK) LEFT JOIN QUOTE_HDR WITH (NOLOCK) ON QUOTE_LINE.DOC_NO = QUOTE_HDR.DOC_NO WHERE QUOTE_HDR.ADDED_DTE >= DATEADD(DAY, -30, GETDATE()) AND C.ACCTNO = QUOTE_HDR.ACCTNO),0) > 0 THEN 'Y' ELSE 'N' END AS 'Field Quotes in Last 30 Days',
			CASE WHEN ISNULL((SELECT SUM(QUOTE_LINE.DOC_TOTAL) FROM QUOTE_LINE WITH (NOLOCK) LEFT JOIN QUOTE_HDR WITH (NOLOCK) ON QUOTE_LINE.DOC_NO = QUOTE_HDR.DOC_NO WHERE QUOTE_HDR.ADDED_DTE >= DATEADD(MONTH, -3, GETDATE()) AND C.ACCTNO = QUOTE_HDR.ACCTNO),0) > 0 THEN 'Y' ELSE 'N' END AS 'Field Quotes in Last 3 Months',
			CASE WHEN 
			(ISNULL((SELECT SUM(SO_LINE.DOC_TOTAL) FROM SO_LINE WITH (NOLOCK) LEFT JOIN SO_HDR WITH (NOLOCK) ON SO_LINE.DOC_NO = SO_HDR.DOC_NO WHERE SO_HDR.ADDED_DTE >= DATEADD(DAY,-30,GETDATE()) AND C.ACCTNO = SO_HDR.ACCTNO),0)
			-
			ISNULL((SELECT SUM(SO_LINE.DOC_TOTAL) FROM SO_LINE WITH (NOLOCK) LEFT JOIN SO_HDR WITH (NOLOCK) ON SO_LINE.DOC_NO = SO_HDR.DOC_NO WHERE SO_HDR.ADDED_DTE >= DATEADD(DAY,-60,GETDATE()) AND SO_HDR.ADDED_DTE < DATEADD(DAY,-30,GETDATE()) AND C.ACCTNO = SO_HDR.ACCTNO),0)) < 0 THEN 'Y' ELSE 'N' END AS 'Field Drop in Sales Last 30 Days',
			CASE WHEN 
			(ISNULL((SELECT SUM(SO_LINE.DOC_TOTAL) FROM SO_LINE WITH (NOLOCK) LEFT JOIN SO_HDR WITH (NOLOCK) ON SO_LINE.DOC_NO = SO_HDR.DOC_NO WHERE SO_HDR.ADDED_DTE >= DATEADD(DAY,-90,GETDATE()) AND C.ACCTNO = SO_HDR.ACCTNO),0)
			-
			ISNULL((SELECT SUM(SO_LINE.DOC_TOTAL) FROM SO_LINE WITH (NOLOCK) LEFT JOIN SO_HDR WITH (NOLOCK) ON SO_LINE.DOC_NO = SO_HDR.DOC_NO WHERE SO_HDR.ADDED_DTE >= DATEADD(DAY,-180,GETDATE()) AND SO_HDR.ADDED_DTE < DATEADD(DAY,-90,GETDATE()) AND C.ACCTNO = SO_HDR.ACCTNO),0)) < 0 THEN 'Y' ELSE 'N' END AS 'Field Drop in Sales Last 90 Days',
			CASE WHEN 
			(ISNULL((SELECT SUM(SO_LINE.DOC_TOTAL) FROM SO_LINE WITH (NOLOCK) LEFT JOIN SO_HDR WITH (NOLOCK) ON SO_LINE.DOC_NO = SO_HDR.DOC_NO WHERE SO_HDR.ADDED_DTE >= DATEADD(DAY,-180,GETDATE()) AND C.ACCTNO = SO_HDR.ACCTNO),0)
			-
			ISNULL((SELECT SUM(SO_LINE.DOC_TOTAL) FROM SO_LINE WITH (NOLOCK) LEFT JOIN SO_HDR WITH (NOLOCK) ON SO_LINE.DOC_NO = SO_HDR.DOC_NO WHERE SO_HDR.ADDED_DTE >= DATEADD(DAY,-360,GETDATE()) AND SO_HDR.ADDED_DTE < DATEADD(DAY,-180,GETDATE()) AND C.ACCTNO = SO_HDR.ACCTNO),0)) < 0 THEN 'Y' ELSE 'N' END AS 'Field Drop in Sales Last 180 Days',
			CASE WHEN ISNULL((SELECT SUM(SL.DOC_TOTAL) FROM SO_LINE SL WITH (NOLOCK) LEFT JOIN SO_HDR SH WITH (NOLOCK) ON SL.DOC_NO = SH.DOC_NO WHERE SH.ADDED_DTE >= DATEADD(DAY, -90, GETDATE())
			and (LEFT (SL.PARTNUMBER, 2) IN ('ST', 'BR', 'BZ', 'CO', 'PL')  OR LEFT (SL.PARTNUMBER, 3) IN ('P19') 
			  OR LEFT (SL.PARTNUMBER, 4) IN ('SSDP', 'SSSP', 'SACP','STGQ', 'STGF', 'ALGQ', 'STPG', 'STFA', 'STFO', 'MSTP') 
			  OR LEFT (SL.PARTNUMBER, 6) IN ('SSS155','SSS177','SSS174','SSS301','SSP321')) AND SL.ACCTNO = C.ACCTNO),0) > 0 THEN 'Y' ELSE 'N' END AS 'Field New Products Sales Last 90 Days',
			CASE WHEN ISNULL((SELECT SUM(QL.DOC_TOTAL) FROM QUOTE_LINE QL WITH (NOLOCK) LEFT JOIN QUOTE_HDR QH WITH (NOLOCK) ON QL.DOC_NO = QH.DOC_NO WHERE QH.ADDED_DTE >= DATEADD(DAY, -180, GETDATE())
			and LEFT (QL.PARTNUMBER, 2) IN ('PL') AND QL.ACCTNO = C.ACCTNO),0) > 0 THEN 'Y' ELSE 'N' END AS 'Field Plastic Quotes in Last 180 Days',
			CAST(ISNULL((SELECT MAX(ADDED_DTE) FROM SO_HDR WITH (NOLOCK) WHERE SO_HDR.ACCTNO = C.ACCTNO),'01-01-2000') AS DATE) AS 'LAST SO DATE',
			CAST(ISNULL((SELECT MAX(ADDED_DTE) FROM QUOTE_HDR WITH (NOLOCK) WHERE QUOTE_HDR.ACCTNO = C.ACCTNO),'01-01-2000') AS DATE) AS 'LAST QUOTE DATE',
			CAST(ISNULL((SELECT MAX(ENTERED_DATE) FROM CALL_TRACKING WITH (NOLOCK) WHERE CALL_TRACKING.ACCTNO=C.ACCTNO AND CALL_TRACKING.ENTERED_BY IN (select ccode from userlist where depart in ('OSLS','ISLS'))),'01-01-2000') AS DATE) AS 'Last SALES CALL Date'
		FROM CUSTVEND C WITH (NOLOCK) 
		LEFT JOIN Hadco_SpotIO_ID ID WITH (NOLOCK) ON C.ACCTNO = ID.acctno
		LEFT JOIN CUSTVENDSETUP CV WITH (NOLOCK) ON C.ACCTNO=CV.ACCTNO AND CV.CUST_VEND = 'C'
		LEFT JOIN CONTACTS CS WITH (NOLOCK) ON CV.ACCTNO=CS.ACCTNO AND CV.CCODE=CS.CCODE AND CV.SUBC=CS.SUBC LEFT JOIN USERLIST INSIDE WITH (NOLOCK) ON CV.SMAN1_CODE = INSIDE.CCODE
		LEFT JOIN USERLIST OUTSIDE WITH (NOLOCK) ON C.REGION = OUTSIDE.CCODE LEFT JOIN TBLCODE ACCT_TYPE WITH (NOLOCK) ON CV.ACCOUNT_TYPE = ACCT_TYPE.TBLCODE AND TBLTYPE = '002' 
		LEFT JOIN TERMS WITH (NOLOCK) ON CV.TERM_CODE = TERMS.TERM_CODE
		LEFT JOIN 
		(
			SELECT 
			   ACCTNO
			   , SUM(IIF(DATEDIFF(DAY, ISNULL(INV_DUE, INV_DATE), GETDATE()) < 0, IIF(DOC_CATEGORY = 'RM' AND ARAP_BALANCE_TOTAL > 0, 0, ARAP_BALANCE_TOTAL), 0)) AS 'NOT DUE'
			   , SUM(IIF(DATEDIFF(DAY, ISNULL(INV_DUE, INV_DATE), GETDATE()) >= 0 AND DATEDIFF(DAY, ISNULL(INV_DUE, INV_DATE), GETDATE()) < 31, IIF(DOC_CATEGORY = 'RM' AND ARAP_BALANCE_TOTAL > 0, 0, ARAP_BALANCE_TOTAL), 0)) AS 'A/R 0-30'
			   , SUM(IIF(DATEDIFF(DAY, ISNULL(INV_DUE, INV_DATE), GETDATE()) >= 31 AND DATEDIFF(DAY, ISNULL(INV_DUE, INV_DATE), GETDATE()) < 61, IIF(DOC_CATEGORY = 'RM' AND ARAP_BALANCE_TOTAL > 0, 0, ARAP_BALANCE_TOTAL), 0)) AS 'A/R 31-60'
			   , SUM(IIF(DATEDIFF(DAY, ISNULL(INV_DUE, INV_DATE), GETDATE()) >= 61, IIF(DOC_CATEGORY = 'RM' AND ARAP_BALANCE_TOTAL > 0, 0, ARAP_BALANCE_TOTAL), 0)) AS 'A/R 61+'
			   , SUM(IIF(DATEDIFF(DAY, ISNULL(INV_DUE, INV_DATE), GETDATE()) >= 61 AND DATEDIFF(DAY, ISNULL(INV_DUE, INV_DATE), GETDATE()) < 91, IIF(DOC_CATEGORY = 'RM' AND ARAP_BALANCE_TOTAL > 0, 0, ARAP_BALANCE_TOTAL), 0)) AS 'A/R 61-90'
			   , SUM(IIF(DATEDIFF(DAY, ISNULL(INV_DUE, INV_DATE), GETDATE()) >= 91 AND DATEDIFF(DAY, ISNULL(INV_DUE, INV_DATE), GETDATE()) < 120, IIF(DOC_CATEGORY = 'RM' AND ARAP_BALANCE_TOTAL > 0, 0, ARAP_BALANCE_TOTAL), 0)) AS 'A/R 91-120'
			   , CASE WHEN SUM(IIF(DATEDIFF(DAY, ISNULL(INV_DUE, INV_DATE), GETDATE()) >= 120, IIF(DOC_CATEGORY = 'RM' AND ARAP_BALANCE_TOTAL > 0, 0, ARAP_BALANCE_TOTAL), 0)) <=0 THEN 0 
						ELSE SUM(IIF(DATEDIFF(DAY, ISNULL(INV_DUE, INV_DATE), GETDATE()) >= 120, IIF(DOC_CATEGORY = 'RM' AND ARAP_BALANCE_TOTAL > 0, 0, ARAP_BALANCE_TOTAL), 0))
					END AS 'A/R 120+'
		   FROM VIEW_CUSTOMER_INV WITH (NOLOCK)
		   GROUP BY ACCTNO
		) AR_DATA
		ON C.ACCTNO = AR_DATA.ACCTNO
		WHERE CV.CUST_VEND = 'C' 
		AND ISNULL(C.REGION,'NA') NOT IN ('INTER','TNANT', 'COL', 'W/O') 
		AND (ID.spotio_id IS NOT NULL OR DATEDIFF(DAY,CV.ADDED_DTE,GETDATE()) = 1 OR DATEDIFF(DAY,CV.UPDATED_DTE,GETDATE()) = 1)
		AND C.REGION IS NOT NULL
	) DATASET

	--Region null
	INSERT INTO Hadco_Spotio_CustomerData_LastValues
	(
		[Address (house # & street name)] 
		, [House #]
		, [Street] 
		, [City] 
		, [ZIP]
		, [State] 
		, [Country]
		, [Longitude] 
		, [Latitude]
		, [Creation date] 
		, [CUSTOMER OWNER EMAIL] 
		, [Last Visit Result]
		, [Last Visit Date]
		, [Visit Count]
		, [External Customer ID]
		, [External Pin ID]
		, [Field Company] 
		, [Field Customer ID] 
		, [Field Unit] 
		, [Field Account Type] 
		, [Field Contact #1 First Name] 
		, [Field Contact #1 Last Name]
		, [Field Contact #1 Title] 
		, [Field Contact #1 Email] 
		, [Field Contact #1 Phone] 
		, [Field Customer Priority] 
		, [Field High Priority Customer] 
		, [Field Estimated Annual Purchases USD] 
		, [Field Estimated Number of Employees] 
		, [Field Estimated Number of Machines] 
		, [Field Estimated Building Size SF] 
		, [Field Material Used] 
		, [Field Website] 
		, [Field Inside Person] 
		, [Field Inside Person Email] 
		, [Field Office Manager] 
		, [Field Office Manager Email] 
		, [Field Added Date]
		, [Added Date] 
		, [Field Contact #2 First Name]
		, [Field Contact #2 Last Name]
		, [Field Contact #2 Title] 
		, [Field Contact #2 Email] 
		, [Field Contact #2 Phone] 
		, [Field Contact #3 First Name]
		, [Field Contact #3 Last Name] 
		, [Field Contact #3 Title] 
		, [Field Contact #3 Email] 
		, [Field Contact #3 Phone]
		, [Field Current Year Sales]
		, [Field Last Year Sales]
		, [Field Prior Year Sales]
		, [Field Current Year Orders]
		, [Field Last Year Orders]
		, [Field Prior Year Orders]
		, [Field Current Year Quotes]
		, [Field Last Year Quotes] 
		, [Field Prior Year Quotes]
		, [Field Terms] 
		, [Field Not Due A/R] 
		, [Field Late A/R 0-30] 
		, [Field Late A/R 31-60]
		, [Field Late A/R 61+]
		, [Field Late A/R 61-90] 
		, [Field Late A/R 91-120]
		, [Field Late A/R 120+] 
		, [Field Quotes in Last 7 Days]
		, [Field Quotes in Last 30 Days]
		, [Field Quotes in Last 3 Months]
		, [Field Drop in Sales Last 30 Days] 
		, [Field Drop in Sales Last 90 Days] 
		, [Field Drop in Sales Last 180 Days] 
		, [Field New Products Sales Last 90 Days] 
		, [Field Plastic Quotes in Last 180 Days] 
		, [Field Activity  in Last 3 months] 
		--, [Field Days Since Last Active]
		, [Stage]
		, [Data Date]
		, [spotio_id]
	)
	SELECT 
		Dataset.[Address (house # & street name)], Dataset.[House #], Dataset.[Street], Dataset.[City], Dataset.[ZIP], Dataset.[State], Dataset.[Country], Dataset.[Longitude], Dataset.[Latitude], Dataset.[Creation date], Dataset.[CUSTOMER OWNER EMAIL], Dataset.[Last Visit Result], Dataset.[Last Visit Date], Dataset.[Visit Count], Dataset.[External Customer ID], Dataset.[External Pin ID], 
		Dataset.[Field Company], Dataset.[Field Customer ID], Dataset.[Field Unit], Dataset.[Field Account Type], Dataset.[Field Contact #1 First Name], Dataset.[Field Contact #1 Last Name], Dataset.[Field Contact #1 Title], Dataset.[Field Contact #1 Email], Dataset.[Field Contact #1 Phone], Dataset.[Field Customer Priority], Dataset.[Field High Priority Customer], 
		Dataset.[Field Estimated Annual Purchases USD], Dataset.[Field Estimated Number of Employees], Dataset.[Field Estimated Number of Machines], Dataset.[Field Estimated Building Size SF], Dataset.[Field Material Used], Dataset.[Field Website], Dataset.[Field Inside Person], Dataset.[Field Inside Person Email], Dataset.[Field Office Manager], Dataset.[Field Office Manager Email], Dataset.[Field Added Date], 
		Dataset.[Added Date], Dataset.[Field Contact #2 First Name], Dataset.[Field Contact #2 Last Name], Dataset.[Field Contact #2 Title], Dataset.[Field Contact #2 Email], Dataset.[Field Contact #2 Phone], Dataset.[Field Contact #3 First Name], Dataset.[Field Contact #3 Last Name], Dataset.[Field Contact #3 Title], Dataset.[Field Contact #3 Email], Dataset.[Field Contact #3 Phone], 
		Dataset.[Field Current Year Sales], Dataset.[Field Last Year Sales], Dataset.[Field Prior Year Sales], Dataset.[Field Current Year Orders], Dataset.[Field Last Year Orders], Dataset.[Field Prior Year Orders], Dataset.[Field Current Year Quotes], Dataset.[Field Last Year Quotes], Dataset.[Field Prior Year Quotes], Dataset.[Field Terms], Dataset.[Field Not Due A/R], 
		Dataset.[Field Late A/R 0-30], Dataset.[Field Late A/R 31-60], Dataset.[Field Late A/R 61+], Dataset.[Field Late A/R 61-90], Dataset.[Field Late A/R 91-120], Dataset.[Field Late A/R 120+], Dataset.[Field Quotes in Last 7 Days], 
		Dataset.[Field Quotes in Last 30 Days], Dataset.[Field Quotes in Last 3 Months], Dataset.[Field Drop in Sales Last 30 Days], Dataset.[Field Drop in Sales Last 90 Days], Dataset.[Field Drop in Sales Last 180 Days], Dataset.[Field New Products Sales Last 90 Days], Dataset.[Field Plastic Quotes in Last 180 Days], 
		CASE WHEN DATEDIFF(MONTH, (CASE WHEN [LAST SO DATE] >= [LAST QUOTE DATE] AND [LAST SO DATE] >= [Last SALES CALL Date] AND [LAST SO DATE] >= [Added Date] THEN [LAST SO DATE]
					WHEN [LAST QUOTE DATE] >= [LAST SO DATE] AND [LAST QUOTE DATE] >= [Last SALES CALL Date] AND [LAST QUOTE DATE] >= [Added Date] THEN [LAST QUOTE DATE]
					WHEN [Last SALES CALL Date] >= [LAST SO DATE] AND [Last SALES CALL Date] >= [LAST QUOTE DATE] AND [Last SALES CALL Date] >= [Added Date] THEN [Last SALES CALL Date]
					ELSE [Added Date] END), GETDATE()) <= 3 THEN 'Y' ELSE 'N' END AS 'Field Activity  in Last 3 months',
		/*DATEDIFF(DAY, (CASE WHEN [LAST SO DATE] >= [LAST QUOTE DATE] AND [LAST SO DATE] >= [Last SALES CALL Date] AND [LAST SO DATE] >= [Added Date] THEN [LAST SO DATE]
					WHEN [LAST QUOTE DATE] >= [LAST SO DATE] AND [LAST QUOTE DATE] >= [Last SALES CALL Date] AND [LAST QUOTE DATE] >= [Added Date] THEN [LAST QUOTE DATE]
					WHEN [Last SALES CALL Date] >= [LAST SO DATE] AND [Last SALES CALL Date] >= [LAST QUOTE DATE] AND [Last SALES CALL Date] >= [Added Date] THEN [Last SALES CALL Date]
					ELSE [Added Date] END), GETDATE()) AS 'Field Days Since Last Active',*/
		CASE WHEN [Field Customer Priority] IN ('X') THEN 'Not a Good Fit' 
			WHEN [Field Customer Priority] IN ('Z') THEN 'Business Closed' 
			WHEN DATEDIFF(DAY, [LAST SO DATE], GETDATE()) <= 365 THEN 'Customer' 
			WHEN DATEDIFF(DAY, [LAST SO DATE], GETDATE()) > 365 AND DATEDIFF(DAY, [LAST QUOTE DATE], GETDATE()) <= 365 THEN 'Active Prospect'
			ELSE 'Prospect' 
		END AS 'Stage'
		, GETDATE(), Dataset.[ID]
	FROM 
	(
		SELECT 
			ISNULL(ID.spotio_id,'') AS ID,
			ISNULL(C.REGION,'HOME') AS REGION,
			'"' + ISNULL(C.ADR1,'') + '"' AS 'Address (house # & street name)', 
			'' AS 'House #', 
			'' AS 'Street', 
			'"' + ISNULL(C.CITY,'') + '"' AS 'City', 
			'"' + ISNULL(LEFT(C.ZIP,5),'') + '"' AS 'ZIP', 
			'"' + ISNULL(C.STATE,'') + '"' AS 'State', 
			'"' + ISNULL(C.COUNTRY,'') + '"' AS 'Country',
			'' AS 'Longitude', '' AS 'Latitude', 
			CAST(C.ADDED_DTE AS DATE) AS 'Creation date', 
			CASE WHEN ISNULL(C.REGION, '') IN ('HOME','SR01','') THEN 'RDvir@hadco-metal.com'
					WHEN OUTSIDE.HTTP_PROXY_USER IS NULL THEN '' 
					ELSE CONCAT(RTRIM(OUTSIDE.HTTP_PROXY_USER),'@hadco-metal.com') 
				END AS 'CUSTOMER OWNER EMAIL',
			'' AS 'Last Visit Result',
			CAST((SELECT MAX(ENTERED_DATE) FROM CALL_TRACKING WHERE CALL_TRACKING.ACCTNO=C.ACCTNO AND CALL_TRACKING.ENTERED_BY IN (select ccode from userlist where depart in ('osls'))) AS DATE) AS 'Last Visit Date',
			'' AS 'Visit Count',
			C.ACCTNO AS 'External Customer ID',
			'' AS 'External Pin ID',
			'"' + REPLACE(C.NAME, '"', '') + '"' AS 'Field Company',
			C.ACCTNO AS 'Field Customer ID',
			'"' + C.ADR2 + '"' AS 'Field Unit',
			ISNULL(ACCT_TYPE.NAME,'') AS 'Field Account Type',
			'"' + ISNULL((select F_NAME from contacts where ccode = 'S01' and acctno = c.ACCTNO),'') + '"' AS 'Field Contact #1 First Name', 
			'"' + ISNULL((select L_NAME from contacts where ccode = 'S01' and acctno = c.ACCTNO),'') + '"' AS 'Field Contact #1 Last Name', 
			'"' + ISNULL((select TITLE from contacts where ccode = 'S01' and acctno = c.ACCTNO),'') + '"' AS 'Field Contact #1 Title',
			'"' + ISNULL((select EMAIL from contacts where ccode = 'S01' and acctno = c.ACCTNO),'') + '"' AS 'Field Contact #1 Email',
			'"' + ISNULL((select TEL1 from contacts where ccode = 'S01' and acctno = c.ACCTNO),'') + '"' AS 'Field Contact #1 Phone',  
			ISNULL(CV.PRIORITY,'') AS 'Field Customer Priority', 
			ISNULL(C.ALT3_CODE_C,'') AS 'Field High Priority Customer',
			'' AS 'Field Estimated Annual Purchases USD',
			'' AS 'Field Estimated Number of Employees',
			'' AS 'Field Estimated Number of Machines',
			'' AS 'Field Estimated Building Size SF',
			'' AS 'Field Material Used',
			'"' + ISNULL(C.WEB_SITE,'') + '"' AS 'Field Website',
			CONCAT(CONCAT(RTRIM(ISNULL(INSIDE.F_NAME,'')), ' '),RTRIM(ISNULL(INSIDE.L_NAME,''))) AS 'Field Inside Person',
			CASE WHEN INSIDE.HTTP_PROXY_USER IS NULL THEN '' ELSE CONCAT(RTRIM(INSIDE.HTTP_PROXY_USER),'@HADCO-METAL.COM') END AS 'Field Inside Person Email',
			CASE WHEN INSIDE.DIVISION IN ('04','05','03','02') THEN 'Colleen Pierson'
					 WHEN INSIDE.DIVISION IN ('01') THEN 'Chris Harmse'
					 WHEN INSIDE.DIVISION IN ('07','70','71') THEN 'Robert Worth'
				ELSE '' END AS 'Field Office Manager',
			CASE WHEN INSIDE.DIVISION IN ('04','05','03','02') THEN 'ColleenP@hadco-metal.com'
					 WHEN INSIDE.DIVISION IN ('01') THEN 'ChrisH@hadco-metal.com'
					 WHEN INSIDE.DIVISION IN ('07','70','71') THEN 'RobertW@hadco-metal.com'
				ELSE '' END AS 'Field Office Manager Email',
			CAST(C.ADDED_DTE AS DATE) AS 'Field Added Date',
			CAST(ISNULL(C.ADDED_DTE, '01-01-2000') AS DATE) AS 'Added Date',
			'"' + ISNULL((select F_NAME from contacts where ccode = 'S02' and acctno = c.ACCTNO),'') + '"' AS 'Field Contact #2 First Name', 
			'"' + ISNULL((select L_NAME from contacts where ccode = 'S02' and acctno = c.ACCTNO),'') + '"' AS 'Field Contact #2 Last Name', 
			'"' + ISNULL((select TITLE from contacts where ccode = 'S02' and acctno = c.ACCTNO),'') + '"' AS 'Field Contact #2 Title',
			'"' + ISNULL((select EMAIL from contacts where ccode = 'S02' and acctno = c.ACCTNO),'') + '"' AS 'Field Contact #2 Email',
			'"' + ISNULL((select TEL1 from contacts where ccode = 'S02' and acctno = c.ACCTNO),'') + '"' AS 'Field Contact #2 Phone', 
			'"' + ISNULL((select F_NAME from contacts where ccode = 'S03' and acctno = c.ACCTNO),'') + '"' AS 'Field Contact #3 First Name', 
			'"' + ISNULL((select L_NAME from contacts where ccode = 'S03' and acctno = c.ACCTNO),'') + '"' AS 'Field Contact #3 Last Name', 
			'"' + ISNULL((select TITLE from contacts where ccode = 'S03' and acctno = c.ACCTNO),'') + '"' AS 'Field Contact #3 Title',
			'"' + ISNULL((select EMAIL from contacts where ccode = 'S03' and acctno = c.ACCTNO),'') + '"' AS 'Field Contact #3 Email',
			'"' + ISNULL((select TEL1 from contacts where ccode = 'S03' and acctno = c.ACCTNO),'') + '"' AS 'Field Contact #3 Phone', 
			ISNULL((SELECT SUM(INV_LINE.DOC_TOTAL) FROM INV_LINE WITH (NOLOCK) LEFT JOIN INV_HDR ON INV_LINE.DOC_NO = INV_HDR.DOC_NO WHERE YEAR(POST_GL_DATE) = YEAR(GETDATE()) AND C.ACCTNO = INV_HDR.ACCTNO AND INV_HDR.DOC_TYPE='I' and INV_LINE.LINE_TYPE<>'02'),0) - 
			ISNULL((SELECT SUM(RMA_LINE.DOC_TOTAL) FROM RMA_LINE WITH (NOLOCK) LEFT JOIN RMA_HDR ON RMA_LINE.DOC_NO = RMA_HDR.DOC_NO WHERE YEAR(POST_GL_DATE) = YEAR(GETDATE()) AND C.ACCTNO = RMA_HDR.ACCTNO AND RMA_HDR.DOC_TYPE='M' and RMA_LINE.LINE_TYPE<>'02'),0) AS 'Field Current Year Sales',
			ISNULL((SELECT SUM(INV_LINE.DOC_TOTAL) FROM INV_LINE WITH (NOLOCK) LEFT JOIN INV_HDR ON INV_LINE.DOC_NO = INV_HDR.DOC_NO WHERE YEAR(POST_GL_DATE) = YEAR(GETDATE())-1 AND C.ACCTNO = INV_HDR.ACCTNO AND INV_HDR.DOC_TYPE='I' and INV_LINE.LINE_TYPE<>'02'),0) - 
			ISNULL((SELECT SUM(RMA_LINE.DOC_TOTAL) FROM RMA_LINE WITH (NOLOCK) LEFT JOIN RMA_HDR ON RMA_LINE.DOC_NO = RMA_HDR.DOC_NO WHERE YEAR(POST_GL_DATE) = YEAR(GETDATE())-1 AND C.ACCTNO = RMA_HDR.ACCTNO AND RMA_HDR.DOC_TYPE='M' and RMA_LINE.LINE_TYPE<>'02'),0) AS 'Field Last Year Sales',
			ISNULL((SELECT SUM(INV_LINE.DOC_TOTAL) FROM INV_LINE WITH (NOLOCK) LEFT JOIN INV_HDR ON INV_LINE.DOC_NO = INV_HDR.DOC_NO WHERE YEAR(POST_GL_DATE) = YEAR(GETDATE())-2 AND C.ACCTNO = INV_HDR.ACCTNO AND INV_HDR.DOC_TYPE='I' and INV_LINE.LINE_TYPE<>'02'),0) - 
			ISNULL((SELECT SUM(RMA_LINE.DOC_TOTAL) FROM RMA_LINE WITH (NOLOCK) LEFT JOIN RMA_HDR ON RMA_LINE.DOC_NO = RMA_HDR.DOC_NO WHERE YEAR(POST_GL_DATE) = YEAR(GETDATE())-2 AND C.ACCTNO = RMA_HDR.ACCTNO AND RMA_HDR.DOC_TYPE='M' and RMA_LINE.LINE_TYPE<>'02'),0) AS 'Field Prior Year Sales',
			ISNULL((SELECT SUM(SO_LINE.DOC_TOTAL) FROM SO_LINE WITH (NOLOCK) LEFT JOIN SO_HDR ON SO_LINE.DOC_NO = SO_HDR.DOC_NO WHERE YEAR(SO_HDR.ADDED_DTE) = YEAR(GETDATE()) AND C.ACCTNO = SO_HDR.ACCTNO),0) AS 'Field Current Year Orders',
			ISNULL((SELECT SUM(SO_LINE.DOC_TOTAL) FROM SO_LINE WITH (NOLOCK) LEFT JOIN SO_HDR ON SO_LINE.DOC_NO = SO_HDR.DOC_NO WHERE YEAR(SO_HDR.ADDED_DTE) = YEAR(GETDATE())-1 AND C.ACCTNO = SO_HDR.ACCTNO),0) AS 'Field Last Year Orders',
			ISNULL((SELECT SUM(SO_LINE.DOC_TOTAL) FROM SO_LINE WITH (NOLOCK) LEFT JOIN SO_HDR ON SO_LINE.DOC_NO = SO_HDR.DOC_NO WHERE YEAR(SO_HDR.ADDED_DTE) = YEAR(GETDATE())-2 AND C.ACCTNO = SO_HDR.ACCTNO),0) AS 'Field Prior Year Orders',
			ISNULL((SELECT SUM(QUOTE_LINE.DOC_TOTAL) FROM QUOTE_LINE WITH (NOLOCK) LEFT JOIN QUOTE_HDR ON QUOTE_LINE.DOC_NO = QUOTE_HDR.DOC_NO WHERE YEAR(QUOTE_HDR.ADDED_DTE) = YEAR(GETDATE()) AND C.ACCTNO = QUOTE_HDR.ACCTNO),0) AS 'Field Current Year Quotes',
			ISNULL((SELECT SUM(QUOTE_LINE.DOC_TOTAL) FROM QUOTE_LINE WITH (NOLOCK) LEFT JOIN QUOTE_HDR ON QUOTE_LINE.DOC_NO = QUOTE_HDR.DOC_NO WHERE YEAR(QUOTE_HDR.ADDED_DTE) = YEAR(GETDATE())-1 AND C.ACCTNO = QUOTE_HDR.ACCTNO),0) AS 'Field Last Year Quotes',
			ISNULL((SELECT SUM(QUOTE_LINE.DOC_TOTAL) FROM QUOTE_LINE WITH (NOLOCK) LEFT JOIN QUOTE_HDR ON QUOTE_LINE.DOC_NO = QUOTE_HDR.DOC_NO WHERE YEAR(QUOTE_HDR.ADDED_DTE) = YEAR(GETDATE())-2 AND C.ACCTNO = QUOTE_HDR.ACCTNO),0) AS 'Field Prior Year Quotes',
			ISNULL(TERMS.DESCRIP,'') AS 'Field Terms',
			ISNULL(AR_DATA.[NOT DUE],0) AS 'Field Not Due A/R',
			ISNULL(AR_DATA.[A/R 0-30],0) AS 'Field Late A/R 0-30',
			ISNULL(AR_DATA.[A/R 31-60],0) AS 'Field Late A/R 31-60',
			ISNULL(AR_DATA.[A/R 61+],0) AS 'Field Late A/R 61+',
			ISNULL(AR_DATA.[A/R 61-90],0) AS 'Field Late A/R 61-90',
			ISNULL(AR_DATA.[A/R 91-120],0) AS 'Field Late A/R 91-120',
			ISNULL(AR_DATA.[A/R 120+],0) AS 'Field Late A/R 120+',
			CASE WHEN ISNULL((SELECT SUM(QUOTE_LINE.DOC_TOTAL) FROM QUOTE_LINE WITH (NOLOCK) LEFT JOIN QUOTE_HDR WITH (NOLOCK) ON QUOTE_LINE.DOC_NO = QUOTE_HDR.DOC_NO WHERE QUOTE_HDR.ADDED_DTE >= DATEADD(DAY, -7, GETDATE()) AND C.ACCTNO = QUOTE_HDR.ACCTNO),0) > 0 THEN 'Y' ELSE 'N' END AS 'Field Quotes in Last 7 Days',
			CASE WHEN ISNULL((SELECT SUM(QUOTE_LINE.DOC_TOTAL) FROM QUOTE_LINE WITH (NOLOCK) LEFT JOIN QUOTE_HDR WITH (NOLOCK) ON QUOTE_LINE.DOC_NO = QUOTE_HDR.DOC_NO WHERE QUOTE_HDR.ADDED_DTE >= DATEADD(DAY, -30, GETDATE()) AND C.ACCTNO = QUOTE_HDR.ACCTNO),0) > 0 THEN 'Y' ELSE 'N' END AS 'Field Quotes in Last 30 Days',
			CASE WHEN ISNULL((SELECT SUM(QUOTE_LINE.DOC_TOTAL) FROM QUOTE_LINE WITH (NOLOCK) LEFT JOIN QUOTE_HDR WITH (NOLOCK) ON QUOTE_LINE.DOC_NO = QUOTE_HDR.DOC_NO WHERE QUOTE_HDR.ADDED_DTE >= DATEADD(MONTH, -3, GETDATE()) AND C.ACCTNO = QUOTE_HDR.ACCTNO),0) > 0 THEN 'Y' ELSE 'N' END AS 'Field Quotes in Last 3 Months',
			CASE WHEN 
			(ISNULL((SELECT SUM(SO_LINE.DOC_TOTAL) FROM SO_LINE WITH (NOLOCK) LEFT JOIN SO_HDR WITH (NOLOCK) ON SO_LINE.DOC_NO = SO_HDR.DOC_NO WHERE SO_HDR.ADDED_DTE >= DATEADD(DAY,-30,GETDATE()) AND C.ACCTNO = SO_HDR.ACCTNO),0)
			-
			ISNULL((SELECT SUM(SO_LINE.DOC_TOTAL) FROM SO_LINE WITH (NOLOCK) LEFT JOIN SO_HDR WITH (NOLOCK) ON SO_LINE.DOC_NO = SO_HDR.DOC_NO WHERE SO_HDR.ADDED_DTE >= DATEADD(DAY,-60,GETDATE()) AND SO_HDR.ADDED_DTE < DATEADD(DAY,-30,GETDATE()) AND C.ACCTNO = SO_HDR.ACCTNO),0)) < 0 THEN 'Y' ELSE 'N' END AS 'Field Drop in Sales Last 30 Days',
			CASE WHEN 
			(ISNULL((SELECT SUM(SO_LINE.DOC_TOTAL) FROM SO_LINE WITH (NOLOCK) LEFT JOIN SO_HDR WITH (NOLOCK) ON SO_LINE.DOC_NO = SO_HDR.DOC_NO WHERE SO_HDR.ADDED_DTE >= DATEADD(DAY,-90,GETDATE()) AND C.ACCTNO = SO_HDR.ACCTNO),0)
			-
			ISNULL((SELECT SUM(SO_LINE.DOC_TOTAL) FROM SO_LINE WITH (NOLOCK) LEFT JOIN SO_HDR WITH (NOLOCK) ON SO_LINE.DOC_NO = SO_HDR.DOC_NO WHERE SO_HDR.ADDED_DTE >= DATEADD(DAY,-180,GETDATE()) AND SO_HDR.ADDED_DTE < DATEADD(DAY,-90,GETDATE()) AND C.ACCTNO = SO_HDR.ACCTNO),0)) < 0 THEN 'Y' ELSE 'N' END AS 'Field Drop in Sales Last 90 Days',
			CASE WHEN 
			(ISNULL((SELECT SUM(SO_LINE.DOC_TOTAL) FROM SO_LINE WITH (NOLOCK) LEFT JOIN SO_HDR WITH (NOLOCK) ON SO_LINE.DOC_NO = SO_HDR.DOC_NO WHERE SO_HDR.ADDED_DTE >= DATEADD(DAY,-180,GETDATE()) AND C.ACCTNO = SO_HDR.ACCTNO),0)
			-
			ISNULL((SELECT SUM(SO_LINE.DOC_TOTAL) FROM SO_LINE WITH (NOLOCK) LEFT JOIN SO_HDR WITH (NOLOCK) ON SO_LINE.DOC_NO = SO_HDR.DOC_NO WHERE SO_HDR.ADDED_DTE >= DATEADD(DAY,-360,GETDATE()) AND SO_HDR.ADDED_DTE < DATEADD(DAY,-180,GETDATE()) AND C.ACCTNO = SO_HDR.ACCTNO),0)) < 0 THEN 'Y' ELSE 'N' END AS 'Field Drop in Sales Last 180 Days',
			CASE WHEN ISNULL((SELECT SUM(SL.DOC_TOTAL) FROM SO_LINE SL WITH (NOLOCK) LEFT JOIN SO_HDR SH WITH (NOLOCK) ON SL.DOC_NO = SH.DOC_NO WHERE SH.ADDED_DTE >= DATEADD(DAY, -90, GETDATE())
			and (LEFT (SL.PARTNUMBER, 2) IN ('ST', 'BR', 'BZ', 'CO', 'PL')  OR LEFT (SL.PARTNUMBER, 3) IN ('P19') 
			  OR LEFT (SL.PARTNUMBER, 4) IN ('SSDP', 'SSSP', 'SACP','STGQ', 'STGF', 'ALGQ', 'STPG', 'STFA', 'STFO', 'MSTP') 
			  OR LEFT (SL.PARTNUMBER, 6) IN ('SSS155','SSS177','SSS174','SSS301','SSP321')) AND SL.ACCTNO = C.ACCTNO),0) > 0 THEN 'Y' ELSE 'N' END AS 'Field New Products Sales Last 90 Days',
			CASE WHEN ISNULL((SELECT SUM(QL.DOC_TOTAL) FROM QUOTE_LINE QL WITH (NOLOCK) LEFT JOIN QUOTE_HDR QH WITH (NOLOCK) ON QL.DOC_NO = QH.DOC_NO WHERE QH.ADDED_DTE >= DATEADD(DAY, -180, GETDATE())
			and LEFT (QL.PARTNUMBER, 2) IN ('PL') AND QL.ACCTNO = C.ACCTNO),0) > 0 THEN 'Y' ELSE 'N' END AS 'Field Plastic Quotes in Last 180 Days',
			CAST(ISNULL((SELECT MAX(ADDED_DTE) FROM SO_HDR WITH (NOLOCK) WHERE SO_HDR.ACCTNO = C.ACCTNO),'01-01-2000') AS DATE) AS 'LAST SO DATE',
			CAST(ISNULL((SELECT MAX(ADDED_DTE) FROM QUOTE_HDR WITH (NOLOCK) WHERE QUOTE_HDR.ACCTNO = C.ACCTNO),'01-01-2000') AS DATE) AS 'LAST QUOTE DATE',
			CAST(ISNULL((SELECT MAX(ENTERED_DATE) FROM CALL_TRACKING WITH (NOLOCK) WHERE CALL_TRACKING.ACCTNO=C.ACCTNO AND CALL_TRACKING.ENTERED_BY IN (select ccode from userlist where depart in ('OSLS','ISLS'))),'01-01-2000') AS DATE) AS 'Last SALES CALL Date'
		FROM CUSTVEND C WITH (NOLOCK) 
		LEFT JOIN Hadco_SpotIO_ID ID WITH (NOLOCK) ON C.ACCTNO = ID.acctno
		LEFT JOIN CUSTVENDSETUP CV WITH (NOLOCK) ON C.ACCTNO=CV.ACCTNO AND CV.CUST_VEND = 'C'
		LEFT JOIN CONTACTS CS WITH (NOLOCK) ON CV.ACCTNO=CS.ACCTNO AND CV.CCODE=CS.CCODE AND CV.SUBC=CS.SUBC LEFT JOIN USERLIST INSIDE WITH (NOLOCK) ON CV.SMAN1_CODE = INSIDE.CCODE
		LEFT JOIN USERLIST OUTSIDE WITH (NOLOCK) ON C.REGION = OUTSIDE.CCODE LEFT JOIN TBLCODE ACCT_TYPE WITH (NOLOCK) ON CV.ACCOUNT_TYPE = ACCT_TYPE.TBLCODE AND TBLTYPE = '002' 
		LEFT JOIN TERMS WITH (NOLOCK) ON CV.TERM_CODE = TERMS.TERM_CODE
		LEFT JOIN 
		(
			SELECT 
			   ACCTNO
			   , SUM(IIF(DATEDIFF(DAY, ISNULL(INV_DUE, INV_DATE), GETDATE()) < 0, IIF(DOC_CATEGORY = 'RM' AND ARAP_BALANCE_TOTAL > 0, 0, ARAP_BALANCE_TOTAL), 0)) AS 'NOT DUE'
			   , SUM(IIF(DATEDIFF(DAY, ISNULL(INV_DUE, INV_DATE), GETDATE()) >= 0 AND DATEDIFF(DAY, ISNULL(INV_DUE, INV_DATE), GETDATE()) < 31, IIF(DOC_CATEGORY = 'RM' AND ARAP_BALANCE_TOTAL > 0, 0, ARAP_BALANCE_TOTAL), 0)) AS 'A/R 0-30'
			   , SUM(IIF(DATEDIFF(DAY, ISNULL(INV_DUE, INV_DATE), GETDATE()) >= 31 AND DATEDIFF(DAY, ISNULL(INV_DUE, INV_DATE), GETDATE()) < 61, IIF(DOC_CATEGORY = 'RM' AND ARAP_BALANCE_TOTAL > 0, 0, ARAP_BALANCE_TOTAL), 0)) AS 'A/R 31-60'
			   , SUM(IIF(DATEDIFF(DAY, ISNULL(INV_DUE, INV_DATE), GETDATE()) >= 61, IIF(DOC_CATEGORY = 'RM' AND ARAP_BALANCE_TOTAL > 0, 0, ARAP_BALANCE_TOTAL), 0)) AS 'A/R 61+'
			   , SUM(IIF(DATEDIFF(DAY, ISNULL(INV_DUE, INV_DATE), GETDATE()) >= 61 AND DATEDIFF(DAY, ISNULL(INV_DUE, INV_DATE), GETDATE()) < 91, IIF(DOC_CATEGORY = 'RM' AND ARAP_BALANCE_TOTAL > 0, 0, ARAP_BALANCE_TOTAL), 0)) AS 'A/R 61-90'
			   , SUM(IIF(DATEDIFF(DAY, ISNULL(INV_DUE, INV_DATE), GETDATE()) >= 91 AND DATEDIFF(DAY, ISNULL(INV_DUE, INV_DATE), GETDATE()) < 120, IIF(DOC_CATEGORY = 'RM' AND ARAP_BALANCE_TOTAL > 0, 0, ARAP_BALANCE_TOTAL), 0)) AS 'A/R 91-120'
			   , CASE WHEN SUM(IIF(DATEDIFF(DAY, ISNULL(INV_DUE, INV_DATE), GETDATE()) >= 120, IIF(DOC_CATEGORY = 'RM' AND ARAP_BALANCE_TOTAL > 0, 0, ARAP_BALANCE_TOTAL), 0)) <=0 THEN 0 
						ELSE SUM(IIF(DATEDIFF(DAY, ISNULL(INV_DUE, INV_DATE), GETDATE()) >= 120, IIF(DOC_CATEGORY = 'RM' AND ARAP_BALANCE_TOTAL > 0, 0, ARAP_BALANCE_TOTAL), 0))
					END AS 'A/R 120+'
		   FROM VIEW_CUSTOMER_INV WITH (NOLOCK)
		   GROUP BY ACCTNO
		) AR_DATA
		ON C.ACCTNO = AR_DATA.ACCTNO
		WHERE CV.CUST_VEND = 'C' 
		AND ISNULL(C.REGION,'NA') NOT IN ('INTER','TNANT', 'COL', 'W/O') 
		AND (ID.spotio_id IS NOT NULL OR DATEDIFF(DAY,CV.ADDED_DTE,GETDATE()) = 1 OR DATEDIFF(DAY,CV.UPDATED_DTE,GETDATE()) = 1)
		AND C.REGION IS NULL
	) DATASET

	--Export temp data file
	SET @query= 'SELECT * FROM GDB_01_001.dbo.Hadco_Spotio_CustomerData' --this is a view
	SET @saveas = @directory + '\temp_' + @filename
	SET @cmdquery = 'bcp "' + replace(@query, char(10), '') + '" QUERYOUT "' + @saveas 
	+ '" -c -t, -T -S ' + @@servername 
	EXEC master..xp_cmdshell @cmdquery 
	
	--Append data to the perm file
	SET @cmdquery = 'type "' + @directory + '\temp_' + @filename + '" >> "' + @directory + '\' + @filename + '"'
	EXEC master..xp_cmdshell @cmdquery
	
	--Delete temp file
	SET @cmdquery = 'del "' + @directory + '\temp_' + @filename + '"'
	EXEC master..xp_cmdshell @cmdquery


END
