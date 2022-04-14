SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create function [METADATA].[f_GetBatchStepStartDateTimeFromLastSuccessfulBatch](@TargetSystem varchar(50), @TargetSchema varchar(50), @TargetObject varchar(50), @DoNotIncludeTodayFlag bit)
returns datetime
as begin

	declare @StartDate date
	set @StartDate = iif(@DoNotIncludeTodayFlag = 1, convert(date, getdate()), dateadd(dd, 1, convert(date, getdate())))
	declare @LatestDateTime datetime =
	(
	select
		a.StartDateTime
	from
	(
		select
			bs.StartDateTime,
			row_number()over(order by b.StartDateTime desc) as rnk
		from
		metadata.batchlog b
		inner join metadata.batchsteplog bs
			on bs.BatchID = b.BatchID
		where
			b.Status = 'Success'
			and b.TargetSystem = @TargetSystem
			and bs.TargetObject = @TargetObject
			and bs.TargetSchema like '%' + @TargetSchema + '%'
			and b.StartDateTime < @StartDate
	) a 
	where
		a.rnk = 1
	)

	--Return the date
	return isnull(@LatestDateTime, '01/01/1900')

end
GO


