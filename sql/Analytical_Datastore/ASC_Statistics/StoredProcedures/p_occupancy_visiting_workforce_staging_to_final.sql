CREATE PROCEDURE [ASC_Statistics].[p_occupancy_visiting_workforce_staging_to_final]
AS
BEGIN
-- Drop the all_metrics table if it exists
DROP TABLE IF EXISTS [ASC_Statistics].[occupancy_visiting_workforce];

-- Create the new all_metrics table with snake case column names
CREATE TABLE [ASC_Statistics].[occupancy_visiting_workforce] (
    metric_group_description NVARCHAR(255),
    area NVARCHAR(50),
    area_code NVARCHAR(50),
    area_unit NVARCHAR(50),
    metric NVARCHAR(255),
    week_ending DATE,
    value FLOAT
);

-- Declare variables to hold dynamic SQL and column names
DECLARE @columns NVARCHAR(MAX);
DECLARE @sql NVARCHAR(MAX);
DECLARE @table_name NVARCHAR(128);

SET @table_name = 'occupancy_visiting_workforce_table_1_staging';

-- Get the list of columns to UNPIVOT i.e. columns that start with 'Week ending'
SELECT @columns = STRING_AGG(QUOTENAME(COLUMN_NAME), ', ')
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'ASC_Statistics'
  AND TABLE_NAME = @table_name
  AND COLUMN_NAME LIKE 'Week ending%';

-- Construct the UNPIVOT dynamic SQL
SET @sql = N'
INSERT INTO [ASC_Statistics].[occupancy_visiting_workforce] (
    metric_group_description,
    area,
    area_code,
    area_unit,
    metric,
    week_ending,
    value
)
SELECT
    ''Care homes permitting residents to receive visitors, England'' AS metric_group_description,
    [Area] AS area,
    [Area Code] AS area_code,
    [Area unit] AS area_unit,
    ISNULL([Measure], '''') + '' where '' + ISNULL(LOWER([Description]), '''') AS metric,
    CONVERT(DATE, SUBSTRING([Week ending], 13, 10), 103) AS week_ending,
    TRY_CAST([Value] AS FLOAT) AS value
FROM 
     [ASC_Statistics].[' + @table_name + ']
UNPIVOT
(
    [Value] 
    FOR [Week ending] IN (' + @columns + ')
) AS Unpvt
';

-- Execute the dynamic SQL
EXEC sp_executesql @sql;

-------------------------------------------------------------------------------------------------------------------
SET @table_name = 'occupancy_visiting_workforce_table_2_staging';

-- Get the list of columns to UNPIVOT i.e. columns that start with 'Week ending'
SELECT @columns = STRING_AGG(QUOTENAME(COLUMN_NAME), ', ')
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'ASC_Statistics'
  AND TABLE_NAME = @table_name
  AND COLUMN_NAME LIKE 'Week ending%';

-- Construct the UNPIVOT dynamic SQL
SET @sql = N'
INSERT INTO [ASC_Statistics].[occupancy_visiting_workforce] (
    metric_group_description,
    area,
    area_code,
    area_unit,
    metric,
    week_ending,
    value
)
SELECT
    ''Staff absence rates in care homes in the last 7 days, England'' AS metric_group_description,
    [Area] AS area,
    [Area Code] AS area_code,
    [Area unit] AS area_unit,
    [Description] AS metric,
    CONVERT(DATE, SUBSTRING([Week ending], 13, 10), 103) AS week_ending,
    TRY_CAST([Value] AS FLOAT) AS value
FROM 
     [ASC_Statistics].[' + @table_name + ']
UNPIVOT
(
    [Value] 
    FOR [Week ending] IN (' + @columns + ')
) AS Unpvt
';

-- Execute the dynamic SQL
EXEC sp_executesql @sql;

-------------------------------------------------------------------------------------------------------------------
SET @table_name = 'occupancy_visiting_workforce_table_3_staging';

-- Get the list of columns to UNPIVOT i.e. columns that start with 'Week ending'
SELECT @columns = STRING_AGG(QUOTENAME(COLUMN_NAME), ', ')
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'ASC_Statistics'
  AND TABLE_NAME = @table_name
  AND COLUMN_NAME LIKE 'Week ending%';

-- Construct the UNPIVOT dynamic SQL
SET @sql = N'
INSERT INTO [ASC_Statistics].[occupancy_visiting_workforce] (
    metric_group_description,
    area,
    area_code,
    area_unit,
    metric,
    week_ending,
    value
)
SELECT
    ''Staff absence rates in domiciliary care providers in the last 7 days, England'' AS metric_group_description,
    [Area] AS area,
    [Area Code] AS area_code,
    [Area unit] AS area_unit,
    [Description] AS metric,
    CONVERT(DATE, SUBSTRING([Week ending], 13, 10), 103) AS week_ending,
    TRY_CAST([Value] AS FLOAT) AS value
FROM 
     [ASC_Statistics].[' + @table_name + ']
UNPIVOT
(
    [Value] 
    FOR [Week ending] IN (' + @columns + ')
) AS Unpvt
';

-- Execute the dynamic SQL
EXEC sp_executesql @sql;

-------------------------------------------------------------------------------------------------------------------
SET @table_name = 'occupancy_visiting_workforce_table_4_staging';

-- Get the list of columns to UNPIVOT i.e. columns that start with 'Week ending'
SELECT @columns = STRING_AGG(QUOTENAME(COLUMN_NAME), ', ')
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'ASC_Statistics'
  AND TABLE_NAME = @table_name
  AND COLUMN_NAME LIKE 'Week ending%';

-- Construct the UNPIVOT dynamic SQL
SET @sql = N'
INSERT INTO [ASC_Statistics].[occupancy_visiting_workforce] (
    metric_group_description,
    area,
    area_code,
    area_unit,
    metric,
    week_ending,
    value
)
SELECT
    ''Occupancy rates in care homes in the last 7 days, England'' AS metric_group_description,
    [Area] AS area,
    [Area Code] AS area_code,
    [Area unit] AS area_unit,
    [Measure] AS metric,
    CONVERT(DATE, SUBSTRING([Week ending], 13, 10), 103) AS week_ending,
    TRY_CAST([Value] AS FLOAT) AS value
FROM 
     [ASC_Statistics].[' + @table_name + ']
UNPIVOT
(
    [Value] 
    FOR [Week ending] IN (' + @columns + ')
) AS Unpvt
';

-- Execute the dynamic SQL
EXEC sp_executesql @sql;

-------------------------------------------------------------------------------------------------------------------
SET @table_name = 'occupancy_visiting_workforce_table_5_staging';

-- Get the list of columns to UNPIVOT i.e. columns that start with 'Week ending'
SELECT @columns = STRING_AGG(QUOTENAME(COLUMN_NAME), ', ')
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'ASC_Statistics'
  AND TABLE_NAME = @table_name
  AND COLUMN_NAME LIKE 'Week ending%';

-- Construct the UNPIVOT dynamic SQL
SET @sql = N'
INSERT INTO [ASC_Statistics].[occupancy_visiting_workforce] (
    metric_group_description,
    area,
    area_code,
    area_unit,
    metric,
    week_ending,
    value
)
SELECT
    ''Response rates for care home visitation, England'' AS metric_group_description,
    [Area] AS area,
    [Area Code] AS area_code,
    [Area unit] AS area_unit,
    [Description] AS metric,
    CONVERT(DATE, SUBSTRING([Week ending], 13, 10), 103) AS week_ending,
    TRY_CAST([Value] AS FLOAT) AS value
FROM 
     [ASC_Statistics].[' + @table_name + ']
UNPIVOT
(
    [Value] 
    FOR [Week ending] IN (' + @columns + ')
) AS Unpvt
';

-- Execute the dynamic SQL
EXEC sp_executesql @sql;

-------------------------------------------------------------------------------------------------------------------
SET @table_name = 'occupancy_visiting_workforce_table_6_staging';

-- Get the list of columns to UNPIVOT i.e. columns that start with 'Week ending'
SELECT @columns = STRING_AGG(QUOTENAME(COLUMN_NAME), ', ')
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'ASC_Statistics'
  AND TABLE_NAME = @table_name
  AND COLUMN_NAME LIKE 'Week ending%';

-- Construct the UNPIVOT dynamic SQL
SET @sql = N'
INSERT INTO [ASC_Statistics].[occupancy_visiting_workforce] (
    metric_group_description,
    area,
    area_code,
    area_unit,
    metric,
    week_ending,
    value
)
SELECT
    ''Response rates for staff absence in care homes, England'' AS metric_group_description,
    [Area] AS area,
    [Area Code] AS area_code,
    [Area unit] AS area_unit,
    [Description] AS metric,
    CONVERT(DATE, SUBSTRING([Week ending], 13, 10), 103) AS week_ending,
    TRY_CAST([Value] AS FLOAT) AS value
FROM 
     [ASC_Statistics].[' + @table_name + ']
UNPIVOT
(
    [Value] 
    FOR [Week ending] IN (' + @columns + ')
) AS Unpvt
';

-- Execute the dynamic SQL
EXEC sp_executesql @sql;

-------------------------------------------------------------------------------------------------------------------
SET @table_name = 'occupancy_visiting_workforce_table_7_staging';

-- Get the list of columns to UNPIVOT i.e. columns that start with 'Week ending'
SELECT @columns = STRING_AGG(QUOTENAME(COLUMN_NAME), ', ')
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'ASC_Statistics'
  AND TABLE_NAME = @table_name
  AND COLUMN_NAME LIKE 'Week ending%';

-- Construct the UNPIVOT dynamic SQL
SET @sql = N'
INSERT INTO [ASC_Statistics].[occupancy_visiting_workforce] (
    metric_group_description,
    area,
    area_code,
    area_unit,
    metric,
    week_ending,
    value
)
SELECT
    ''Response rates for staff absences in domiciliary care, England'' AS metric_group_description,
    [Area] AS area,
    [Area Code] AS area_code,
    [Area unit] AS area_unit,
    [Description] AS metric,
    CONVERT(DATE, SUBSTRING([Week ending], 13, 10), 103) AS week_ending,
    TRY_CAST([Value] AS FLOAT) AS value
FROM 
     [ASC_Statistics].[' + @table_name + ']
UNPIVOT
(
    [Value] 
    FOR [Week ending] IN (' + @columns + ')
) AS Unpvt
';

-- Execute the dynamic SQL
EXEC sp_executesql @sql;

-------------------------------------------------------------------------------------------------------------------
SET @table_name = 'occupancy_visiting_workforce_table_8_staging';

-- Get the list of columns to UNPIVOT i.e. columns that start with 'Week ending'
SELECT @columns = STRING_AGG(QUOTENAME(COLUMN_NAME), ', ')
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'ASC_Statistics'
  AND TABLE_NAME = @table_name
  AND COLUMN_NAME LIKE 'Week ending%';

-- Construct the UNPIVOT dynamic SQL
SET @sql = N'
INSERT INTO [ASC_Statistics].[occupancy_visiting_workforce] (
    metric_group_description,
    area,
    area_code,
    area_unit,
    metric,
    week_ending,
    value
)
SELECT
    ''Response rates for occupancy in care homes, England'' AS metric_group_description,
    [Area] AS area,
    [Area Code] AS area_code,
    [Area unit] AS area_unit,
    [Description] AS metric,
    CONVERT(DATE, SUBSTRING([Week ending], 13, 10), 103) AS week_ending,
    TRY_CAST([Value] AS FLOAT) AS value
FROM 
     [ASC_Statistics].[' + @table_name + ']
UNPIVOT
(
    [Value] 
    FOR [Week ending] IN (' + @columns + ')
) AS Unpvt
';

-- Execute the dynamic SQL
EXEC sp_executesql @sql;
END;
GO

