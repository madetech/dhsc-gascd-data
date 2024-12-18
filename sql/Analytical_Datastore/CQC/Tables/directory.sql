CREATE TABLE [CQC].[directory] (
    [name]                 VARCHAR (255)  NULL,
    [also_known_as]        VARCHAR (255)  NULL,
    [address]              VARCHAR (255)  NULL,
    [postcode]             VARCHAR (255)  NULL,
    [phone_number]         BIGINT         NULL,
    [services_website]     VARCHAR (255)  NULL,
    [service_types]        VARCHAR (1000) NULL,
    [date_of_latest_check] DATE           NULL,
    [specialisms_services] VARCHAR (1000) NULL,
    [provider_name]        VARCHAR (255)  NULL,
    [local_authority]      VARCHAR (255)  NULL,
    [region]               VARCHAR (255)  NULL,
    [location_url]         VARCHAR (255)  NULL,
    [cqc_location_id]      VARCHAR (255)  NULL,
    [cqc_provider_id]      VARCHAR (255)  NULL
);
GO

