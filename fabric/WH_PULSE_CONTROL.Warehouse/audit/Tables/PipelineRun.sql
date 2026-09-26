CREATE TABLE [audit].[PipelineRun] (
    [PipelineRunId]   VARCHAR (100)  NOT NULL,
    [PipelineName]    VARCHAR (200)  NOT NULL,
    [EnvironmentName] VARCHAR (20)   NOT NULL,
    [StartTime]       DATETIME2 (0)  NOT NULL,
    [EndTime]         DATETIME2 (0)  NULL,
    [Status]          VARCHAR (20)   NOT NULL,
    [ErrorActivity]   VARCHAR (200)  NULL,
    [ErrorMessage]    VARCHAR (4000) NULL
);


GO

ALTER TABLE [audit].[PipelineRun]
    ADD CONSTRAINT [PK_PipelineRun] PRIMARY KEY NONCLUSTERED ([PipelineRunId] ASC) NOT ENFORCED;


GO