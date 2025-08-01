/*

Quality checking of summary data of coal mining

*/
drop table if exists layer_gold.fail_summary_coal_mining;
create table layer_gold.fail_summary_coal_mining engine = MergeTree() order by _id as
with check_total_production_daily as (
	select 
		*,
		'check_total_production_daily < 0' as reason
	from layer_gold.summary_coal_mining scm 
	where total_production_daily < 0
),
check_equipment_utilization as (
	select 
		*,
		'equipment_utilization < 0 or > 100' as reason
	from layer_gold.summary_coal_mining scm 
	where equipment_utilization < 0 or equipment_utilization > 100
),
check_weather_impact as (
	select 
		*,
		'weather_impact is null' as reason
	from layer_gold.summary_coal_mining scm 
	where weather_impact is null
),
merge_data as (
	select * from check_total_production_daily
	union all
	select * from check_equipment_utilization
	union all
	select * from check_weather_impact
),
check_result as (
	select
		_id,
		any_value(date) as date,
		any_value(total_production_daily) as total_production_daily,
		any_value(average_quality_grade) as average_quality_grade,
		any_value(equipment_utilization) as equipment_utilization,
		any_value(fuel_efficiency) as fuel_efficiency,
		any_value(weather_impact) as weather_impact,
		groupArray(reason) as reasons,
		now() as _created_at
	from merge_data
	group by _id
)
select * from check_result;

alter table layer_gold.summary_coal_mining 
delete where _id in (select _id from layer_gold.test_summary_coal_mining);