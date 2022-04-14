
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [METADATA].[BatchLog](
	[BatchID] [int] IDENTITY(1000,1) NOT NULL,
	[OrchestrationTool] [varchar](50) NOT NULL,
	[InitPipelineName] [varchar](50) NOT NULL,
	[Project] [varchar](50) NOT NULL,
	[SourceSystem] [varchar](50) NULL,
	[TargetSystem] [varchar](50) NULL,
	[Status] [varchar](25) NOT NULL,
	[StartDateTime] [datetime] NOT NULL,
	[EndDateTime] [datetime] NULL,
PRIMARY KEY CLUSTERED 
(
	[BatchID] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO


SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [METADATA].[BatchStepLog](
	[BatchStepID] [int] IDENTITY(1,1) NOT NULL,
	[BatchID] [int] NOT NULL,
	[StepNumber] [varchar](50) NOT NULL,
	[StepName] [varchar](100) NOT NULL,
	[SourceSchema] [varchar](100) NULL,
	[SourceObject] [varchar](50) NULL,
	[TargetUpdateStrategy] [varchar](50) NULL,
	[TargetSchema] [varchar](100) NULL,
	[TargetObject] [varchar](50) NULL,
	[TargetFiles] [int] NULL,
	[TargetRows] [int] NULL,
	[Status] [varchar](25) NOT NULL,
	[StartDateTime] [datetime] NOT NULL,
	[EndDateTime] [datetime] NULL,
PRIMARY KEY CLUSTERED 
(
	[BatchStepID] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY],
 CONSTRAINT [NK_Unique] UNIQUE NONCLUSTERED 
(
	[BatchID] ASC,
	[StepName] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
