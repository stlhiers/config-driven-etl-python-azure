SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE procedure [METADATA].[s_BatchLogging]
@Status varchar(25),
@OrchestrationTool varchar(50) = null,
@Project varchar(50) = null,
@InitPipelineName varchar(50) = null,
@SourceSystem varchar(50) = null,
@TargetSystem varchar(50) = null,
@BatchID int = null

as
begin

/***********************
Upsert Records
***********************/
declare @TransactionName varchar(100) = concat('BatchLog', '_', @Status)

begin transaction @TransactionName

	begin try

		declare @LockResult int;
	    exec @LockResult = sp_getapplock
			@Resource = 'METADATA.s_BatchLogging',
			@LockMode = 'Exclusive',
			@LockOwner = 'Transaction',
			@LockTimeout = 600000 -- 10 minutes

		if @LockResult >= 0 --Acquired the lock

		begin

			merge into metadata.BatchLog with (holdlock) as target
			using (select @BatchID as BatchID) as source
				on source.BatchID = target.BatchID
			when matched then 
				update set 
					target.Status = @Status,
					target.EndDateTime = dbo.getdate()
			when not matched by target then
				insert
				(
					OrchestrationTool,
					InitPipelineName,
					Project,
					SourceSystem,
					TargetSystem,
					Status,
					StartDateTime,
					EndDateTime
				)
				values
				(
					@OrchestrationTool,
					@InitPipelineName,
					@Project,
					@SourceSystem,
					@TargetSystem,
					@Status,
					dbo.GetDate(),
					null
				)
			;

			/***********************
			Get newly inserted Pk and
			use it to execute the
			first batch step
			***********************/
			set @BatchID = isnull(@BatchID, SCOPE_IDENTITY())
			exec METADATA.s_BatchStepLogging @BatchID, 'Main', @Status

			/***********************
			Return new BatchID
			***********************/
			select @BatchID as BatchID

			commit transaction @TransactionName

		end

	end try

	begin catch
				
		rollback transaction @TransactionName;

		throw;

	end catch

end

GO


