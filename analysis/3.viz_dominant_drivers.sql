-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Report on Dominant Formula 1 Drivers </h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

create or replace temp view v_dominant_drivers
as
select driver_name,
       count(1) As total_races,
       sum(calculated_points) as total_points,
       AVG(calculated_points) as avg_points,
       Rank() over(order by AVG(calculated_points) DESC) driver_rank
       from f1_presentation.calculated_race_results
       group by driver_name
       having count(1) >= 50
       order by avg_points desc

-- COMMAND ----------

select race_year,
       driver_name,
       count(1) As total_races,
       sum(calculated_points) as total_points,
       AVG(calculated_points) as avg_points
       from f1_presentation.calculated_race_results
       where driver_name in (Select driver_name from v_dominant_drivers where driver_rank <= 10)
       group by  race_year, driver_name
       order by race_year, avg_points desc

-- COMMAND ----------

select race_year,
       driver_name,
       count(1) As total_races,
       sum(calculated_points) as total_points,
       AVG(calculated_points) as avg_points
       from f1_presentation.calculated_race_results
       where driver_name in (Select driver_name from v_dominant_drivers where driver_rank <= 10)
       group by  race_year, driver_name
       order by race_year, avg_points desc

-- COMMAND ----------

select race_year,
       driver_name,
       count(1) As total_races,
       sum(calculated_points) as total_points,
       AVG(calculated_points) as avg_points
       from f1_presentation.calculated_race_results
       where driver_name in (Select driver_name from v_dominant_drivers where driver_rank <= 10)
       group by  race_year, driver_name
       order by race_year, avg_points desc

-- COMMAND ----------

