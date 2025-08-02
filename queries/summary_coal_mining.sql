/*

Generate summary data of coal mining

*/
drop table if exists layer_gold.daily_production_metrics;
create table layer_gold.daily_production_metrics engine = MergeTree() order by _id as
with src_summary_production_logs as (
	select
		date,
		sum(case when mpl.tons_extracted < 0 then 0 else mpl.tons_extracted end) as total_production_daily,
		floor(avg(mpl.quality_grade), 2) as average_quality_grade
	from layer_bronze.mysql_coal_mining_production_logs mpl 
	group by date
),
get_equipment_id as (
	select distinct 
		equipment_id 
	from layer_bronze.file_csv_equipement_sensor 
	order by equipment_id
),
get_date as (
	select distinct
		toDate(timestamp) as date
	from layer_bronze.file_csv_equipement_sensor 
),
expected_equipment_id as (
	select
	*
	from get_date
	cross join get_equipment_id
),
stg_equipment_sensor as (
	select 
		*
	from layer_bronze.file_csv_equipement_sensor fces
	union all
    select distinct
    	generateUUIDv4() as _id,
    	concat(eei.date, ' 59:59:59') as timestamp,
		eei.equipment_id,
		'unknown' as status,
		null as fuel_consumption,
		null as maintenance_alert,
		now() as _created_at,
		now() as _updated_at
    from expected_equipment_id as eei
    left join layer_bronze.file_csv_equipement_sensor as fces
    on toDate(fces.timestamp) = eei.date and eei.equipment_id = fces.equipment_id
    where fces.equipment_id is null
),
src_summary_equipment_sensor as (
	select
		toDate(fes.timestamp) as date,
		floor(sum(case when fes.status = 'active' then 1 else 0 end) / count(*), 3) * 100 as equipment_utilization,
		floor(sum(fes.fuel_consumption)) as total_fuel_consumption
	from stg_equipment_sensor fes
	group by toDate(timestamp)
),
src_daily_weather as (
	select
		arrayJoin(arrayZip(
	        JSONExtract(adw.daily, 'time', 'Array(Nullable(Date))'),
	        JSONExtract(adw.daily, 'temperature_2m_mean', 'Array(Nullable(Float64))'),
	        JSONExtract(adw.daily, 'precipitation_sum', 'Array(Nullable(Float64))')
	    )) as zip,
	    zip.1 as date,
	    zip.2 as temperature_2m_mean,
	    zip.3 as precipitation_sum
	from layer_bronze.api_daily_weather adw
),
transformation as (
	select
		sspl.date as date,
		sspl.total_production_daily,
		sspl.average_quality_grade,
		sses.equipment_utilization,
		floor((sses.total_fuel_consumption / sspl.total_production_daily), 3) as fuel_efficiency,
		case 
            when sdw.precipitation_sum = 0 then 'non-rainy'
            when  sdw.precipitation_sum > 0 then 'rainy'
            else null 
        end as weather_impact
	from src_summary_production_logs sspl
	left join src_summary_equipment_sensor sses
	using(date)
	left join src_daily_weather sdw
	using(date)
),
main as (
	select 
		generateUUIDv4() as _id,
		date,
		total_production_daily,
		average_quality_grade,
		equipment_utilization,
		fuel_efficiency,
		weather_impact,
		now() as _created_at
	from transformation t
	where 1=1
	order by date
)
select * from main;