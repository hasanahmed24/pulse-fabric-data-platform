CREATE TABLE [gold].[DimMerchant] (
    [MerchantRecordID]      INT             NULL,
    [MerchantID]            VARCHAR (8000)  NULL,
    [BankAccountNumber]     VARCHAR (8000)  NULL,
    [BankID]                INT             NULL,
    [Description]           VARCHAR (8000)  NULL,
    [MerchantType]          VARCHAR (8000)  NULL,
    [RegisteredCapital]     DECIMAL (19, 2) NULL,
    [Industry]              VARCHAR (8000)  NULL,
    [OperatingStatus]       VARCHAR (8000)  NULL,
    [EstablishmentDate]     DATE            NULL,
    [LegalRepresentativeID] VARCHAR (8000)  NULL,
    [GoldLoadedAt]          DATETIME2 (6)   NULL
);


GO