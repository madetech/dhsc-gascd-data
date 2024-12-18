CREATE TABLE [CQC].[latest_ratings_providers] (
    [provider_id]                          VARCHAR (255) NULL,
    [provider_ods_code]                    VARCHAR (255) NULL,
    [provider_name]                        VARCHAR (255) NULL,
    [provider_type]                        VARCHAR (255) NULL,
    [provider_primary_inspection_category] VARCHAR (255) NULL,
    [provider_street_address]              VARCHAR (255) NULL,
    [provider_address_line_2]              VARCHAR (255) NULL,
    [provider_city]                        VARCHAR (255) NULL,
    [provider_post_code]                   VARCHAR (255) NULL,
    [provider_local_authority]             VARCHAR (255) NULL,
    [provider_region]                      VARCHAR (255) NULL,
    [provider_nhs_region]                  VARCHAR (255) NULL,
    [service_population_group]             VARCHAR (255) NULL,
    [domain]                               VARCHAR (255) NULL,
    [latest_rating]                        VARCHAR (255) NULL,
    [publication_date]                     DATE          NULL,
    [report_type]                          VARCHAR (255) NULL,
    [inherited_rating]                     INT           NOT NULL,
    [url]                                  VARCHAR (255) NULL,
    [brand_id]                             VARCHAR (255) NULL,
    [brand_name]                           VARCHAR (255) NULL
);
GO

