CREATE TABLE [ctl].[SilverWatermark] (
    [EntityName]              VARCHAR (100) NOT NULL,
    [LastSuccessfulWatermark] VARCHAR (200) NULL,
    [LastSuccessfulRunId]     VARCHAR (100) NULL,
    [UpdatedAt]               DATETIME2 (6) NULL
);


GO

ALTER TABLE [ctl].[SilverWatermark]
    ADD CONSTRAINT [PK_SilverWatermark] PRIMARY KEY NONCLUSTERED ([EntityName] ASC) NOT ENFORCED;


GO