CREATE TABLE [ctl].[SilverConfig] (
    [EntityName]       VARCHAR (100)  NOT NULL,
    [SourceSchema]     VARCHAR (100)  NOT NULL,
    [SourceTable]      VARCHAR (100)  NOT NULL,
    [TargetSchema]     VARCHAR (100)  NOT NULL,
    [TargetTable]      VARCHAR (100)  NOT NULL,
    [LoadType]         VARCHAR (30)   NOT NULL,
    [WatermarkColumn]  VARCHAR (100)  NULL,
    [MergeKeyColumns]  VARCHAR (500)  NOT NULL,
    [DedupKeyColumns]  VARCHAR (500)  NULL,
    [TransformProfile] VARCHAR (100)  NOT NULL,
    [QuarantineSchema] VARCHAR (100)  NOT NULL,
    [QuarantineTable]  VARCHAR (100)  NOT NULL,
    [MaxRejectPct]     DECIMAL (9, 4) NULL,
    [LoadOrder]        INT            NOT NULL,
    [IsActive]         BIT            NOT NULL,
    [WriteStrategy]    VARCHAR (30)   NULL
);


GO

ALTER TABLE [ctl].[SilverConfig]
    ADD CONSTRAINT [PK_SilverConfig] PRIMARY KEY NONCLUSTERED ([EntityName] ASC) NOT ENFORCED;


GO