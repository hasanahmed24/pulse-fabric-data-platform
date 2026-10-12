CREATE TABLE [gold].[DimPerson] (
    [PersonID]            VARCHAR (8000) NULL,
    [BankAccountNumber]   VARCHAR (8000) NULL,
    [BankID]              INT            NULL,
    [PersonAge]           SMALLINT       NULL,
    [PersonEducation]     VARCHAR (8000) NULL,
    [PersonGender]        VARCHAR (8000) NULL,
    [PersonMaritalStatus] VARCHAR (8000) NULL,
    [PersonOccupation]    VARCHAR (8000) NULL,
    [GoldLoadedAt]        DATETIME2 (6)  NULL
);


GO