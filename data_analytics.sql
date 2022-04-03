-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.text('database_name', 'default')
-- MAGIC database_name = dbutils.widgets.get('database_name')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC #First Task

-- COMMAND ----------

-- MAGIC %md ## Run next command to create dataset

-- COMMAND ----------

create table if not exists $database_name.organization(organization_id integer, organization_name varchar(50), manager_id integer, parent_organization_id integer);

insert into $database_name.organization values(1, "Commercial & Support", 101, 7);
insert into $database_name.organization values(2, "Quality", 49, 11);
insert into $database_name.organization values(3, "IT", 35, 1);
insert into $database_name.organization values(4, "Finance", 3, 1);
insert into $database_name.organization values(5, "HR", 2, 1);
insert into $database_name.organization values(6, "Training", 5, 11);
insert into $database_name.organization values(7, "Board of Directors", 1, null);
insert into $database_name.organization values(8, "Development", 19, 3);
insert into $database_name.organization values(9, "H&S", 67, 11);
insert into $database_name.organization values(10, "Operations", 31, 3);
insert into $database_name.organization values(11, "Management Systems", 94, 7);
insert into $database_name.organization values(12, "Environment", 89, 11);
insert into $database_name.organization values(13, "Noticing & Planning", 74, 1);
insert into $database_name.organization values(14, "Procurement & Tendering", 55, 1);
insert into $database_name.organization values(15, "Operations Division", 62, 7);
insert into $database_name.organization values(16, "Highway Division", 47, 15);
insert into $database_name.organization values(17, "Water", 78, 20);
insert into $database_name.organization values(18, "Street Lighting", 12, 20);
insert into $database_name.organization values(19, "PFI Lighting Division", 65, 15);
insert into $database_name.organization values(20, "Infrastructure Division", 23, 15);
insert into $database_name.organization values(21, "Substation Works", 43, 20);
insert into $database_name.organization values(22, "Capital Area", 77, 23);
insert into $database_name.organization values(23, "Major Projects Division", 81, 15);
insert into $database_name.organization values(24, "Western Area", 87, 23);
insert into $database_name.organization values(25, "Southern Area", 10, 23);

-- COMMAND ----------

-- MAGIC %md #### Present the organization hierarchy in human-readable format.
-- MAGIC     The result set should include each organization level as a column (only names).
-- MAGIC         i.e. level1, level2 ... 
-- MAGIC     

-- COMMAND ----------

use $database_name;

select * 
from organization;

-- COMMAND ----------

-- MAGIC %md # Second Task

-- COMMAND ----------

-- MAGIC %md ## Run next command to create dataset

-- COMMAND ----------

create table if not exists $database_name.emp(firstname varchar(20), lastname varchar(20),  unit integer);

insert into $database_name.emp values('James', 'Barry', 1);
insert into $database_name.emp values('James', 'Barry', 3);
insert into $database_name.emp values('James', 'Barry', 1);
insert into $database_name.emp values('Mohan', 'Kumar', 2);
insert into $database_name.emp values('Raj', 'Gupta', 3);
insert into $database_name.emp values('Raj', 'Gupta', 4);

create table if not exists $database_name.unit(unitname varchar(20), country varchar(20), unitid integer);

insert into $database_name.unit values ('HR', 'UK', 1);
insert into $database_name.unit values ('R&D', 'USA', 2);
insert into $database_name.unit values ('Sales', 'India', 3);
insert into $database_name.unit values ('R&D', 'India', 4);

-- COMMAND ----------

-- MAGIC %md #### List employees who have worked in two different units. 
-- MAGIC     The result set should include:
-- MAGIC     * first name of the employee
-- MAGIC     * last name of the employee
-- MAGIC     * unit name

-- COMMAND ----------

use $database_name;


select firstname,lastname,unitname from emp
inner join unit
on emp.unit = unit.unitid

where firstname in (select firstname
from emp
group by firstname
having count(*) = 2)




-- COMMAND ----------

select *
from unit;

-- COMMAND ----------

-- MAGIC %md # Third Task

-- COMMAND ----------

-- MAGIC %md ## Run next command to create dataset

-- COMMAND ----------

create table $database_name.org_sales(yyyymm integer, organization_name varchar(50), sales_amount integer);

insert into $database_name.org_sales values(202104, "Southern Area", 230);
insert into $database_name.org_sales values(202104, "Western Area", 45);
insert into $database_name.org_sales values(202105, "Southern Area", 250);
insert into $database_name.org_sales values(202106, "Southern Area", 234);
insert into $database_name.org_sales values(202103, "Western Area", 99);
insert into $database_name.org_sales values(202107, "Southern Area", 199);
insert into $database_name.org_sales values(202105, "Western Area", 100);
insert into $database_name.org_sales values(202106, "Western Area", 120);
insert into $database_name.org_sales values(202101, "Southern Area", 233);
insert into $database_name.org_sales values(202102, "Southern Area", 265);
insert into $database_name.org_sales values(202103, "Southern Area", 290);
insert into $database_name.org_sales values(202101, "Western Area", 87);
insert into $database_name.org_sales values(202102, "Western Area", 67);

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### Part 1: Present the monthly sales for each orgnization. 
-- MAGIC     The result set should include:
-- MAGIC         * organization_name
-- MAGIC         * year and month (yyyymm)
-- MAGIC         * sales amount
-- MAGIC         * Yes/No flag if sales amount is greater than sales in the previous month.
-- MAGIC             Yes = greater
-- MAGIC             No = lower
-- MAGIC             null = unknown
-- MAGIC             
-- MAGIC     You can use either SQL or Python for this exercise

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.table(f'{database_name}.org_sales').display()

-- COMMAND ----------

SELECT organization_name, yyyymm, sales_amount,

CASE 
  WHEN difference < 0 THEN 'Yes'
  WHEN difference > 0 THEN 'No'
  ELSE 'Unknown'
  END AS Flag


FROM 
  (
    SELECT
        yyyymm, organization_name, sales_amount,
        sales_amount - lag(sales_amount) over (partition by organization_name order by organization_name, yyyymm) as difference
    FROM
        (
          $database_name.org_sales
        )
        )


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Part 2: Include data for the last 4 months only.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.table(f'{database_name}.org_sales').display()

-- COMMAND ----------

select *
from $database_name.org_sales
where yyyymm <= 202107 AND yyyymm > 202103
ORDER BY organization_name, yyyymm 
