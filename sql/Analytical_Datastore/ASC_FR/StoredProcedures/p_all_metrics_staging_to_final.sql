CREATE PROCEDURE [ASC_FR].[p_all_metrics_staging_to_final]
AS
BEGIN
    -- Drop the all_metrics table if it exists
    DROP TABLE IF EXISTS [ASC_FR].[all_metrics];

    -- Insert data into the new all_metrics table from the pivoted all_metrics_staging table
    SELECT
    CAST([GEOGRAPHY_LEVEL] AS NVARCHAR) AS [geography_level],
    CAST([DATA_Level] AS NVARCHAR) AS [data_level],
    CAST([UUID] AS INT) AS [uuid],
    CAST([FY_ENDING] AS INT) AS [fiscal_year],
    CAST([CASSR] AS NVARCHAR) AS [cassr],
    CAST([GEOGRAPHY_CODE] AS NVARCHAR) AS [geography_code],
    CAST([DH_GEOGRAPHY_NAME] AS NVARCHAR) AS [dh_geography_name],
    CAST([REGION_GO_CODE] AS NVARCHAR) AS [region_go_code],
    CAST([GEOGRAPHY_NAME] AS NVARCHAR) AS [geography_name],
    CAST([DimensionGroup] AS NVARCHAR) AS [dimension_group],
    CAST([CareType] AS NVARCHAR) AS [care_type],
    CAST([FinanceType] AS NVARCHAR) AS [finance_type],
    CAST([FinanceDescription] AS NVARCHAR) AS [finance_description],
    CAST([AgeBand] AS NVARCHAR) AS [age_band],
    CAST([PrimarySupportReason] AS NVARCHAR) AS [primary_support_reason],
    CAST([SupportSetting] AS NVARCHAR) AS [support_setting],
    CAST([Purpose] AS NVARCHAR) AS [purpose],
    CAST([CarerSupportType] AS NVARCHAR) AS [carer_support_type],
    CAST([DataType] AS NVARCHAR) AS [data_type],
    CAST([ActivityProvision] AS NVARCHAR) AS [activity_provision],
    TRY_CAST([ComparativeTotals] AS FLOAT) AS [comparative_totals],
    CAST([DataSource] AS NVARCHAR) AS [data_source],
    TRY_CAST([Numerator] AS FLOAT) AS [numerator],
    TRY_CAST([Denominator] AS FLOAT) AS [denominator],
    TRY_CAST([UNIT COST] AS FLOAT) AS [unit_cost],
    TRY_CAST([Value] AS FLOAT) AS [value]
    INTO [ASC_FR].[all_metrics]
    FROM (
        SELECT 
        [GEOGRAPHY_LEVEL]
      ,[DATA_Level]
      ,[UUID]
      ,[FY_ENDING]
      ,[CASSR]
      ,[GEOGRAPHY_CODE]
      ,[DH_GEOGRAPHY_NAME]
      ,[REGION_GO_CODE]
      ,[GEOGRAPHY_NAME]
      ,[DimensionGroup]
      ,[CareType]
      ,[FinanceType]
      ,[FinanceDescription]
      ,[AgeBand]
      ,[PrimarySupportReason]
      ,[SupportSetting]
      ,[Purpose]
      ,[CarerSupportType]
      ,[DataType]
      ,[ActivityProvision]
      ,[ComparativeTotals]
      ,[DataSource]
      ,CASE WHEN [ValueType] IS NULL THEN 'Value' ELSE [ValueType] END AS [ModifiedValueType]
      ,[ITEMVALUE]
    FROM [ASC_FR].[all_metrics_staging]
) AS SourceTable
PIVOT (
    MAX([ITEMVALUE])
    FOR [ModifiedValueType] IN ([Denominator], [Numerator], [UNIT COST], [Value])
) AS PivotTable;

DROP TABLE IF EXISTS [ASC_FR].[all_metrics_staging];

END;
GO

