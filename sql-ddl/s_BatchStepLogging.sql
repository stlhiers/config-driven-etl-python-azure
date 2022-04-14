SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE procedure [METADATA].[s_BatchStepLogging] 
@BatchID int,
@StepName varchar(100),
@Status varchar(25),
@SourceSchema varchar(100) = null,
@SourceObject varchar(50) = null,
@TargetUpdateStrategy varchar(50) = null,
@TargetSchema varchar(100) = null,
@TargetObject varchar(50) = null,
@TargetFiles int = null

as
begin

/***********************
Insert / Update Records
***********************/
declare @TransactionName varchar(100) = concat(@StepName, '_', @BatchID, '_', @Status)

begin transaction @TransactionName

	begin try

		declare @LockResult int;
	    exec @LockResult = sp_getapplock
			@Resource = 'METADATA.s_BatchStepLogging',
			@LockMode = 'Exclusive',
			@LockOwner = 'Transaction',
			@LockTimeout = 3600000 -- 60 minutes

		if @LockResult >= 0 --Acquired the lock

		begin

			merge into metadata.BatchStepLog with (holdlock) as target
			using (select @BatchID as BatchID, @StepName as StepName) as source
				on source.BatchID = target.BatchID
				and source.StepName = target.StepName
			when matched then
				update set
					target.Status = @Status,
					target.TargetFiles = @TargetFiles,
					target.EndDateTime = dbo.getdate()
			when not matched by target then
				insert
				(
					BatchID,
					StepNumber,
					StepName,
					SourceSchema,
					SourceObject,
					TargetUpdateStrategy,
					TargetSchema,
					TargetObject,
					TargetFiles,
					TargetRows,
					Status,
					StartDateTime,
					EndDateTime
				)
				values
				(
					@BatchID,
					isnull(cast(((select max(cast(StepNumber as numeric)) from metadata.BatchStepLog where BatchID = @BatchID) + 1) as varchar), '1'),
					@StepName,
					@SourceSchema,
					@SourceObject,
					@TargetUpdateStrategy,
					@TargetSchema,
					@TargetObject,
					@TargetFiles,
					0,
					@Status,
					dbo.getdate(),
					null
				);
	
			if 
				(select TargetSystem from metadata.BatchLog where BatchID = @BatchID) = 'Data Warehouse'
				and exists (select 1 from sys.schemas where name = @TargetSchema) 
				and exists (select 1 from sys.objects where name = @TargetObject)
				and @Status != 'Begin'
			begin
				declare @TargetRows table (RowNum int primary key identity, TargetRows int)
				declare @SQLStatement nvarchar(500)
				set @SQLStatement = 'select count(*) as RecordCount from ' + @TargetSchema + '.' + @TargetObject + ' where DW_BatchID  = ' + cast(@BatchID as varchar)

				insert into @TargetRows
				exec sp_executesql @SQLStatement

				update trgt
				set trgt.TargetRows = isnull((select TargetRows from @TargetRows), 0)
				from
				metadata.BatchStepLog trgt
				where
					trgt.BatchID = @BatchID
					and trgt.StepName = @StepName
			end

			commit transaction @TransactionName

		end
	end try

	begin catch
				
		rollback transaction @TransactionName;

		throw;

	end catch

end
GO


