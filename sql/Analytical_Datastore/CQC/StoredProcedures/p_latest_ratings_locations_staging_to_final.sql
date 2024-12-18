CREATE PROCEDURE [CQC].[p_latest_ratings_locations_staging_to_final]
AS
BEGIN

DROP TABLE IF EXISTS [CQC].[latest_ratings_locations];

SELECT 
    [Location ID] AS [location_id],
    [Location ODS Code] AS [location_ods_code],
    [Location Name] AS [location_name],
    CASE WHEN [Care Home?] = 'Y' THEN 1 ELSE 0 END AS [care_home],
    [Location Type] AS [location_type],
    [Location Primary Inspection Category] AS [location_primary_inspection_category],
    [Location Street Address] AS [location_street_address],
    [Location Address Line 2] AS [location_address_line_2],
    [Location City] AS [location_city],
    [Location Post Code] AS [location_post_code],
    [Location Local Authority] AS [location_local_authority],
    [Location Region] AS [location_region],
    [Location NHS Region] AS [location_nhs_region],
    [Location ONSPD CCG Code] AS [location_onspd_ccg_code],
    [Location ONSPD CCG] AS [location_onspd_ccg],
    [Location Commissioning CCG Code] AS [location_commissioning_ccg_code],
    [Location Commissioning CCG Name] AS [location_commissioning_ccg_name],
    [Service / Population Group] AS [service_population_group],
    [Domain] AS [domain],
    [Latest Rating] AS [latest_rating],
    CAST([Publication Date] AS DATE) AS [publication_date],
    [Report Type] AS [report_type],
    CASE WHEN [Inherited Rating (Y/N)] = 'Y' THEN 1 ELSE 0 END AS [inherited_rating],
    [URL] AS [url],
    [Provider ID] AS [provider_id],
    [Provider Name] AS [provider_name],
    CASE WHEN [Brand ID] = '-' THEN NULL ELSE [Brand ID] END AS [brand_id],
    CASE WHEN [Brand Name] = '-' THEN NULL ELSE [Brand Name] END AS [brand_name]
INTO [CQC].[latest_ratings_locations]
FROM [CQC].[latest_ratings_locations_staging]

DROP TABLE IF EXISTS [CQC].[latest_ratings_locations_staging];

END;
GO

