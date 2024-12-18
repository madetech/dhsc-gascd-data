CREATE PROCEDURE [CQC].[p_latest_ratings_providers_staging_to_final]
AS
BEGIN

DROP TABLE IF EXISTS [CQC].[latest_ratings_providers];

SELECT
    [Provider ID] AS [provider_id],
    [Provider ODS Code] AS [provider_ods_code],
    [Provider Name] AS [provider_name],
    [Provider Type] AS [provider_type],
    [Provider Primary Inspection Category] AS [provider_primary_inspection_category],
    [Provider Street Address] AS [provider_street_address],
    [Provider Address Line 2] AS [provider_address_line_2],
    [Provider City] AS [provider_city],
    [Provider Post Code] AS [provider_post_code],
    [Provider Local Authority] AS [provider_local_authority],
    [Provider Region] AS [provider_region],
    [Provider NHS Region] AS [provider_nhs_region],
    [Service / Population Group] AS [service_population_group],
    [Domain] AS [domain],
    [Latest Rating] AS [latest_rating],
    CAST([Publication Date] AS DATE) AS [publication_date],
    [Report Type] AS [report_type],
    CASE WHEN [Inherited Rating (Y/N)] = 'Y' THEN 1 ELSE 0 END AS [inherited_rating],
    [URL] AS [url],
    CASE WHEN [Brand ID] = '-' THEN NULL ELSE [Brand ID] END AS [brand_id],
    CASE WHEN [Brand Name] = '-' THEN NULL ELSE [Brand Name] END AS [brand_name]
INTO [CQC].[latest_ratings_providers]
FROM [CQC].[latest_ratings_providers_staging]

DROP TABLE IF EXISTS [CQC].[latest_ratings_providers_staging];

END;
GO

