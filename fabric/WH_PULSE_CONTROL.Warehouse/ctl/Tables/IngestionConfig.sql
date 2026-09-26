CREATE TABLE [ctl].[IngestionConfig] (
    [EntityName]         VARCHAR (100) NOT NULL,
    [SourceSchema]       VARCHAR (100) NOT NULL,
    [SourceTable]        VARCHAR (100) NOT NULL,
    [TargetTable]        VARCHAR (100) NOT NULL,
    [LoadType]           VARCHAR (30)  NOT NULL,
    [WatermarkColumn]    VARCHAR (100) NULL,
    [WatermarkDataType]  VARCHAR (30)  NULL,
    [BusinessDateColumn] VARCHAR (100) NULL,
    [LookbackDays]       INT           NULL,
    [LoadOrder]          INT           NOT NULL,
    [IsActive]           BIT           NOT NULL,
    [UpsertKeyColumn]    VARCHAR (100) NULL
);


GO

ALTER TABLE [ctl].[IngestionConfig]
    ADD CONSTRAINT [PK_IngestionConfig] PRIMARY KEY NONCLUSTERED ([EntityName] ASC) NOT ENFORCED;


GO