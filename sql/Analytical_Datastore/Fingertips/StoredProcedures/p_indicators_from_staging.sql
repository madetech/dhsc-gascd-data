
CREATE PROCEDURE Fingertips.p_indicators_from_staging AS
BEGIN
    -- DROP PROC [Fingertips].[p_indicators_from_staging]
    -- exec Fingertips.p_indicators_from_staging
    -- Delete all indicators from the indicators table that are in the staging table
    DELETE FROM Fingertips.indicators
    WHERE indicator_id IN (SELECT DISTINCT [Indicator ID] FROM Fingertips.indicators_staging)

    -- Insert all indicators from the staging table into the indicators table
    INSERT INTO Fingertips.indicators
    SELECT
    	CAST([Indicator ID] AS nvarchar) AS [indicator_id],
    	CAST([Indicator Name] AS nvarchar) AS [indicator_name],
    	CAST([Parent Code] AS nvarchar) AS [parent_code],
    	CAST([Parent Name] AS nvarchar) AS [parent_name],
    	CAST([Area Code] AS nvarchar) AS [area_code],
    	CAST([Area Name] AS nvarchar) AS [area_name],
    	CAST([Area Type] AS nvarchar) AS [area_type],
    	CAST([Sex] AS nvarchar) AS [sex],
    	CAST([Age] AS nvarchar) AS [age],
    	CAST([Category Type] AS nvarchar) AS [category_type],
    	CAST([Category] AS nvarchar) AS [category],
    	CAST([Time period] AS nvarchar) AS [time_period], 
    	CAST([Value] AS float) AS [value],
    	CAST([Lower CI 95.0 limit] AS float) AS [lower_ci_95_limit],
    	CAST([Upper CI 95.0 limit] AS float) AS [upper_ci_95_limit],
    	CAST([Lower CI 99.8 limit] AS float) AS [lower_ci_99_limit],
    	CAST([Upper CI 99.8 limit] AS float) AS [upper_ci_99_limit],
    	CAST([Count] AS float) AS [count],
    	CAST([Denominator] AS float) AS [denominator],
    	CAST([Value note] AS nvarchar) AS [value_note], 
    	CAST([Recent Trend] AS nvarchar) AS [recent_trend], 
    	CAST([Compared to England value or percentiles] AS nvarchar) AS [compared_to_england_value_or_percentiles],
    	CAST([Column not used] AS nvarchar) AS [column_not_used],
    	CAST([Time period Sortable] AS nvarchar) AS [time_period_sortable],
    	CAST([New data] AS nvarchar) AS [new_data], 
    	CAST([Compared to goal] AS nvarchar) AS [compared_to_goal],
    	CAST([Time period range] AS nvarchar) AS [time_period_range]
    FROM Fingertips.indicators_staging

    -- Truncate the staging table
    DROP TABLE Fingertips.indicators_staging
END
GO
