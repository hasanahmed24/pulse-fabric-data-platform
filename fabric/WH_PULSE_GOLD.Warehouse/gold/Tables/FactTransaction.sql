CREATE TABLE [gold].[FactTransaction] (
    [TransactionID]        BIGINT          NULL,
    [TransactionTimestamp] DATETIME2 (6)   NULL,
    [TransactionDate]      DATE            NULL,
    [FromBankID]           INT             NULL,
    [FromAccount]          VARCHAR (8000)  NULL,
    [ToBankID]             INT             NULL,
    [ToAccount]            VARCHAR (8000)  NULL,
    [AmountReceived]       DECIMAL (19, 4) NULL,
    [ReceivingCurrency]    VARCHAR (8000)  NULL,
    [AmountPaid]           DECIMAL (19, 4) NULL,
    [PaymentCurrency]      VARCHAR (8000)  NULL,
    [PaymentFormat]        VARCHAR (8000)  NULL,
    [IsLaundering]         BIT             NULL,
    [GoldLoadedAt]         DATETIME2 (6)   NULL
);


GO