CREATE PROCEDURE [CQC].[p_directory_staging_to_final]
AS
BEGIN

DROP TABLE IF EXISTS [CQC].[directory];

SELECT
    [Name] AS [name],
    [Also known as] AS [also_known_as],
    [Address] AS [address],
    [Postcode] AS [postcode],
    TRY_CAST([Phone number] AS BIGINT) AS [phone_number],
    [Service's website (if available)] AS [services_website],
    [Service types] AS [service_types],
    CONVERT(DATE, SUBSTRING([Date of latest check], 1, 2) + '-' + SUBSTRING([Date of latest check], 4, 3) + '-' + SUBSTRING([Date of latest check], 8, 4), 106) AS [date_of_latest_check],
    [Specialisms/services] AS [specialisms_services],
    [Provider name] AS [provider_name],
    [Local authority] AS [local_authority],
    [Region] AS [region],
    [Location URL] AS [location_url],
    [CQC Location ID (for office use only)] AS [cqc_location_id],
    [CQC Provider ID (for office use only)] AS [cqc_provider_id]
INTO [CQC].[directory]
FROM [CQC].[directory_staging];

DROP TABLE IF EXISTS [CQC].[directory_staging];

END;
GO

