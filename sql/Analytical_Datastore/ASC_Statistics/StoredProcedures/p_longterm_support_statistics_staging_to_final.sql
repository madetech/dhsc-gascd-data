CREATE PROCEDURE [ASC_Statistics].[p_longterm_support_statistics_staging_to_final]
AS
BEGIN
drop table if exists [ASC_Statistics].[longterm_support_statistics];

CREATE TABLE [ASC_Statistics].[longterm_support_statistics] (
    area NVARCHAR(50),
    support_setting NVARCHAR(50),
    ethnicity NVARCHAR(255),
    gender NVARCHAR(50),
    age_band NVARCHAR(50),
    area_code NVARCHAR(50),
    area_unit NVARCHAR(50),
    month DATE,
    value INT
);

INSERT INTO [ASC_Statistics].[longterm_support_statistics] (
    area,
    support_setting,
    ethnicity,
    gender,
    age_band,
    area_code,
    area_unit,
    month,
    value
)
SELECT 
    [Area] AS area,
    [Support Setting] AS support_setting,
    [Ethnicity] AS ethnicity,
    [Gender] AS gender,
    [Age Band] AS age_band,
    [Area Code] AS area_code,
    [Area unit] AS area_unit,
    CONVERT(DATE, [Month], 103) AS month,
    TRY_CAST([Value] AS INT) AS value
FROM 
    [ASC_Statistics].[longterm_support_statistics_staging]
UNPIVOT
(
    [Value] 
    FOR [Month] IN 
        (
            [30/04/2023],
            [31/05/2023],
            [30/06/2023],
            [31/07/2023],
            [31/08/2023],
            [30/09/2023],
            [31/10/2023],
            [30/11/2023],
            [31/12/2023]
        )
) AS unpvt;
END;
GO

