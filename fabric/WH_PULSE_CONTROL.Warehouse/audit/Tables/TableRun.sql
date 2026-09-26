CREATE TABLE [audit].[TableRun] (
    [PipelineRunId]  VARCHAR (100)  NOT NULL,
    [EntityName]     VARCHAR (100)  NOT NULL,
    [StartTime]      DATETIME2 (0)  NOT NULL,
    [EndTime]        DATETIME2 (0)  NULL,
    [StartWatermark] VARCHAR (200)  NULL,
    [EndWatermark]   VARCHAR (200)  NULL,
    [SourceRowCount] BIGINT         NULL,
    [RowsRead]       BIGINT         NULL,
    [RowsWritten]    BIGINT         NULL,
    [TargetRowCount] BIGINT         NULL,
    [RowsRejected]   BIGINT         NULL,
    [Status]         VARCHAR (20)   NOT NULL,
    [ErrorMessage]   VARCHAR (4000) NULL
);


GO