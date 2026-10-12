CREATE TABLE [ctl].[SilverDQRule] (
    [EntityName]     VARCHAR (100)  NOT NULL,
    [RuleCode]       VARCHAR (100)  NOT NULL,
    [RuleName]       VARCHAR (300)  NOT NULL,
    [RuleType]       VARCHAR (50)   NOT NULL,
    [ColumnName]     VARCHAR (100)  NULL,
    [RuleExpression] VARCHAR (2000) NOT NULL,
    [Severity]       VARCHAR (20)   NOT NULL,
    [Action]         VARCHAR (30)   NOT NULL,
    [RuleOrder]      INT            NOT NULL,
    [IsActive]       BIT            NOT NULL,
    [ExecutionTier]  VARCHAR (30)   NULL
);


GO

ALTER TABLE [ctl].[SilverDQRule]
    ADD CONSTRAINT [PK_SilverDQRule] PRIMARY KEY NONCLUSTERED ([EntityName] ASC, [RuleCode] ASC) NOT ENFORCED;


GO