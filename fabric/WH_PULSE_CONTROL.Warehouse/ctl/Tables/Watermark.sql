CREATE TABLE [ctl].[Watermark] (
    [EntityName]              VARCHAR (100) NOT NULL,
    [LastSuccessfulWatermark] VARCHAR (200) NULL,
    [LastSuccessfulRunId]     VARCHAR (100) NULL,
    [UpdatedAt]               DATETIME2 (0) NULL
);


GO

ALTER TABLE [ctl].[Watermark]
    ADD CONSTRAINT [PK_Watermark] PRIMARY KEY NONCLUSTERED ([EntityName] ASC) NOT ENFORCED;


GO