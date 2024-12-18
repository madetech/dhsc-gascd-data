CREATE TABLE [Capacity_Tracker].[all_metrics] (
    [location_level]            NVARCHAR (MAX) NULL,
    [location_id]               NVARCHAR (MAX) NULL,
    [location_name]             NVARCHAR (MAX) NULL,
    [parent_location_id]        NVARCHAR (MAX) NULL,
    [parent_location_name]      NVARCHAR (MAX) NULL,
    [grandparent_location_name] NVARCHAR (MAX) NULL,
    [metric]                    NVARCHAR (MAX) NULL,
    [value]                     FLOAT (53)     NULL
);
GO

