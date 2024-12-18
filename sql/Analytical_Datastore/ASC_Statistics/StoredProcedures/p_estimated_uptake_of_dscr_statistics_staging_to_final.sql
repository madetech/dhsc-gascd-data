CREATE PROCEDURE [ASC_Statistics].[p_estimated_uptake_of_dscr_statistics_staging_to_final]
AS
BEGIN

DROP TABLE IF EXISTS [ASC_Statistics].[estimated_uptake_of_dscr_statistics];

CREATE TABLE [ASC_Statistics].[estimated_uptake_of_dscr_statistics] (
    area NVARCHAR(255),
    area_unit NVARCHAR(255),
    description NVARCHAR(255),
    month DATE,
    value FLOAT
);

INSERT INTO [ASC_Statistics].[estimated_uptake_of_dscr_statistics] (
    area,
    area_unit,
    description,
    month,
    value
)
SELECT 
    [Area] AS area,
    [Area Unit] AS area_unit,
    [Description] AS description,
    CAST([Month] AS DATE) AS month,
    [Value] AS value
FROM 
    [ASC_Statistics].[estimated_uptake_of_dscr_statistics_staging]
UNPIVOT
(
    [Value] 
    FOR [Month] IN 
        (
            [December 2021],
            [January 2022],
            [February 2022],
            [March 2022],
            [April 2022],
            [May 2022],
            [June 2022],
            [July 2022],
            [August 2022],
            [September 2022],
            [October 2022],
            [November 2022],
            [December 2022],
            [January 2023],
            [February 2023],
            [March 2023],
            [April 2023],
            [May 2023],
            [June 2023],
            [July 2023],
            [August 2023],
            [September 2023],
            [October 2023],
            [November 2023],
            [December 2023],
            [January 2024],
            [February 2024],
            [March 2024],
            [April 2024],
            [May 2024]
        )
) AS unpvt;
END;
GO

