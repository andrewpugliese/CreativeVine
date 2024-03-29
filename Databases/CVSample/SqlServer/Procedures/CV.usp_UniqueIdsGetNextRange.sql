--						
create proc CV.usp_UniqueIdsGetNextRange
(
@UniqueIdKey varchar(64),
@RangeAmount int = 1,
@UniqueIdValue bigint out,
@ReturnTable bit = 0
)
as
begin
--
--	Procedure Get Next UniqueId Range
--	This procedure will obtain the next UniqueId range of numbers for
--	a given uniqueIdKey and range amount.  
--	The key is the identifier for which the uniqueIds 
--		will be generated; e.g. tableName).
--	The RangeAmount determines what the new value will be
--		the default is sequential, but you could obtain
--		50 at a time for example.  
--	It will return the end of the range and you use all the numbers
--		from the returned value - the range amount.
--
-- If MaxValue is not null then that will be the limit for that key
-- If RollOverValue is not null, then that will be the new value,
-- if MaxValue is not null and has been reached.  If RollOverValue
-- is null when MaxValue has been reached, then it will result in 
-- an exception.
--
	set nocount on
	declare @MaxIdValue bigint, @RolloverIdValue bigint
	
	begin tran
	set xact_abort on

		select @UniqueIdValue = UniqueIdValue
		, @MaxIdValue = MaxIdValue
		, @RolloverIdValue = RolloverIdValue
		from CV.UniqueIds with (xlock, rowlock)
		where UniqueIdKey = @UniqueIdKey

		if @UniqueIdValue is null	-- key not found
		begin
			begin try
				insert into CV.UniqueIds (UniqueIdKey, UniqueIdValue)
				values (@UniqueIdKey, @RangeAmount)
				set @UniqueIdValue = @RangeAmount
				commit tran
				return
			end try
			begin catch
				rollback tran
				exec cv.usp_UniqueIdsGetNextRange @UniqueIdKey, @RangeAmount, @UniqueIdValue out
				return
			end catch
		end
		
		set @UniqueIdValue = @UniqueIdValue + @RangeAmount
		
		-- check to see if we had an overflow
		if @UniqueIdValue > @MaxIdValue
		begin
			if @RolloverIdValue is null -- we have an overflow
			begin
				-- raise error
				declare @RolloverIdValueStr varchar(20)
				set @RolloverIdValueStr = cast(@MaxIdValue as varchar(20))
				raiserror('usp_UniqueIdsGetNextRange:: Overflow for key: %s; MaxIdValue: %s.  No overflow value assigned.', 16, 1, @UniqueIdKey, @RolloverIdValueStr)
				-- rollback and return
				rollback tran
				return
			end -- we have a rollover value
			else set @UniqueIdValue = @RolloverIdValue + ( @RangeAmount - (@UniqueIdValue - @MaxIdValue))
		end
			
		update CV.UniqueIds
		set UniqueIdValue = @UniqueIdValue
		where UniqueIdKey = @UniqueIdKey

	commit tran
	
	if @ReturnTable = 1
		select @UniqueIdKey as UniqueIdKey
		, @UniqueIdValue as UniqueIdValue
		, @RolloverIdValue as RolloverIdValue
		, @MaxIdValue as MaxIdValue
end
GO
grant execute on CV.usp_UniqueIdsGetNextRange to public
go