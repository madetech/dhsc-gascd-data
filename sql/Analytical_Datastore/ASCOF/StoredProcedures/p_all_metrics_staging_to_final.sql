CREATE PROCEDURE [ASCOF].[p_all_metrics_staging_to_final]
AS
BEGIN
    -- Drop the all_metrics table if it exists
    DROP TABLE IF EXISTS [ASCOF].[all_metrics];

    -- Insert data into the new all_metrics table from the pivoted all_metrics_staging table
    SELECT
        CAST(REPLACE([FileName], '.csv', '') AS INT) AS [fiscal_year],
        CAST([Geographical Code] AS NVARCHAR(50)) AS [geographical_code],
        CAST([Geographical Description] AS NVARCHAR(255)) AS [geographical_description],
        CAST([Geographical Level] AS NVARCHAR(50)) AS [geographical_level],
        CAST([ONS Code] AS NVARCHAR(50)) AS [ons_code],
        CAST([ASCOF Measure Code] AS NVARCHAR(50)) AS [ascof_measure_code],
        CAST([Disaggregation Level] AS NVARCHAR(50)) AS [disaggregation_level],
        CAST([Measure Group] AS NVARCHAR(50)) AS [measure_group],
        CAST([Measure Group Description] AS NVARCHAR(1000)) AS [measure_group_description],
        TRY_CAST([Denominator] AS INT) AS [denominator],
        TRY_CAST([Outcome] AS FLOAT) AS [outcome],
        TRY_CAST([Base] AS INT) AS [base],
        TRY_CAST([Margin of Error] AS FLOAT) AS [margin_of_error],
        TRY_CAST([Numerator] AS INT) AS [numerator]
    INTO [ASCOF].[all_metrics]
    FROM (
        SELECT
            *
        FROM [ASCOF].[all_metrics_staging]
    ) AS SourceTable
    PIVOT (
        MAX([Measure_Value])
        FOR [Measure Type] IN ([Denominator], [Outcome], [Base], [Margin of Error], [Numerator])
    ) AS PivotTable;

DROP TABLE IF EXISTS [ASCOF].[all_metrics_staging];

END;
GO

