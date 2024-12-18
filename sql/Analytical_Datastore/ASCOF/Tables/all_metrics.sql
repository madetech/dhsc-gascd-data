CREATE TABLE [ASCOF].[all_metrics] (
    [fiscal_year]               INT             NULL,
    [geographical_code]         NVARCHAR (50)   NULL,
    [geographical_description]  NVARCHAR (255)  NULL,
    [geographical_level]        NVARCHAR (50)   NULL,
    [ons_code]                  NVARCHAR (50)   NULL,
    [ascof_measure_code]        NVARCHAR (50)   NULL,
    [disaggregation_level]      NVARCHAR (50)   NULL,
    [measure_group]             NVARCHAR (50)   NULL,
    [measure_group_description] NVARCHAR (1000) NULL,
    [denominator]               INT             NULL,
    [outcome]                   FLOAT (53)      NULL,
    [base]                      INT             NULL,
    [margin_of_error]           FLOAT (53)      NULL,
    [numerator]                 INT             NULL
);
GO

