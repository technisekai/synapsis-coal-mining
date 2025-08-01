with src_summary_production_logs as (
	select
		date,
		sum(case when mpl.tons_extracted < 0 then 0 else mpl.tons_extracted end) as total_production_daily,
		floor(avg(mpl.quality_grade), 2) as average_quality_grade
	from layer_bronze.mysql_production_logs mpl 
	group by date
),
src_summary_equipment_sensor as (
	select
		toDate(fes.timestamp) as date,
		floor(sum(case when fes.status = 'active' then 1 else 0 end) / count(*), 3) * 100 as equipment_utilization,
		floor(sum(fes.fuel_consumption)) as total_fuel_consumption
	from layer_bronze.files_equipement_sensor fes
	group by toDate(timestamp)
),
src_daily_weather as (
	select
		arrayJoin(JSONExtract(adw.daily, 'time', 'Array(Date)')) as date,
		arrayJoin(JSONExtract(adw.daily, 'temperature_2m_mean', 'Array(Float64)')) as temperature_2m_mean,
		arrayJoin(JSONExtract(adw.daily, 'precipitation_sum', 'Array(Float64)')) as precipitation_sum
	from layer_bronze.api_daily_weather adw
),
transformation as (
	select
		sspl.date as date,
		sspl.total_production_daily,
		sspl.average_quality_grade,
		sses.equipment_utilization,
		floor((sses.total_fuel_consumption / sspl.total_production_daily), 3) as fuel_efficiency,
		sdw.temperature_2m_mean as weather_impact
	from src_summary_production_logs sspl
	left join src_summary_equipment_sensor sses
	using(date)
	left join src_daily_weather sdw
	using(date)
),
main as (
	select 
		date,
		total_production_daily,
		average_quality_grade,
		equipment_utilization,
		fuel_efficiency,
		weather_impact
	from transformation t
	where 1=1
	order by date
)
select * from main 