------sql project----

create database project11;
use project11;

SELECT * FROM project11.`layoffs (1) (1)`;

rename table `layoffs (1) (1)` to layoffs;

select * from layoffs;

--- first thing we want to do is create a staging table
---this is the one we will work in and clean the data
---we want a table with the raw data in case something happens

create table layoffs_staging
like layoffs;

insert layoffs_staging
select * from layoffs;      ----run this together 

select * from layoffs_staging;

-- now when we are data cleaning we usually follow a few steps
---1 check for duplicates and remove any
---2 standarlize data and fix errors
---3 look at null values and see that
---4 remove any columns and rows that are not necessary -few days

--remove duplicates
select company, industry, total_laid_off,`date`,
   row_number() over 
             (partition by company, industry, total_laid_off,`date`)
      as row_num
from layoffs_staging;


select * from (
select company, industry, total_laid_off,`date`,
   row_number() over (
      partition by company, industry, total_laid_off,`date`)
      as row_num
from layoffs_staging)
duplicates 
 where row_num > 1;
 
 
 --lets just look at oda to comfirm 
select * from layoffs_staging
where company = 'oda';


-- it looks like these are all legitimate entries and shouldnt be deleted 

--THESE are our real duplicates
select * from (
    select company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions,
    row_number() over (
    partition by company, location, industry, total_laid_off, percentage_laid_off,`date`, stage, country, funds_raised_millions)
as row_num
from layoffs_staging)
duplicates 
 where row_num > 1; 
 
select version()
--these are the ones we want to delete where the row number is > 1 or
2 or greater essentially

--now you may want to write it like this:
with delete_cte as 
(select * from (
select company, location, industry, total_laid_off, percentage_laid_off,`date`, stage, country, funds_raised_millions,
row_number() over (partition by company, location, industry, total_laid_off, percentage_laid_off,`date`, stage, country, funds_raised_millions)
as row_num
from layoffs_staging)
duplicates 
where row_num > 1)
delete from delete_cte; 


--one solution which i think is a good one . is to create a new column
--and add those row numbers in. then delete where row numbers are over 2,
then delete that columns ---


alter table layoffs_staging
add column row_num int;

select * from layoffs_staging;


CREATE TABLE `layoffs_staging2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int,
row_num INT
);
 
INSERT INTO `layoffs_staging2`
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
SELECT `company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,
            percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		layoffs_staging;


--now that we have this we can delete rows were row_num is greater than 2 

delete from layoffs_staging2
where row_num >= 2;

set sql_safe_updates = 0
select * from layoffs_staging2;

---check duplicates 
select * from (
    select company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions,
    row_number() over (
    partition by company, location, industry, total_laid_off, percentage_laid_off,`date`, stage, country, funds_raised_millions)
as row_num
from layoffs_staging2)
duplicates 
 where row_num > 1; 

2. stadardize data
-- if we look at industry it looks like we have some null
and empty rows ,lets take a look at these 

select distinct industry
from layoffs_staging2
order by industry;


select * from layoffs_staging2
where industry is null
or industry = ''
order by industry;

---lets take a look at these 
select * from layoffs_staging2
where company like 'bally%';

--nothing wrong here 
select * from layoffs_staging2
where company like 'airbnb%';

--it looks like airbnb is a travel , but this one just isnt populated
im sure its the same for the others, what we can do is
write a query that if there is another row with the same company name,
it will update it to the non-null industry values 
makes it easy so if there were thousands we wouldnt have to manually check them all---

--we should set the blanks to nulls since those
are typically easier to work with

update layoffs_staging2
set industry = null
where industry = '';

--now if we check those are all null

select * from layoffs_staging2
where industry is null 
or industry = ''
order by industry;

--now we need to populate those nulls if possible

update layoffs_staging2 t1
join layoffs_staging2 t2
on t1.comapny = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;

--and if we check it looks like ballys was
the only one without a populated row to populate this null values 

select * from layoffs_staging2
 where industry is null 
or industry = ''
order by industry;


-- i also noticed the crypto has multiple different variations
we need to standardize that ...lets say all to crypto

select distinct industry
from layoffs_staging2
order by industry;

update layoffs_staging2
set industry = 'crypto'
where industry in ('crypto currency', 'cryptocurrency');


---now  thats taken care of
select distinct industry
from layoffs_staging2
order by industry; 


----we also need to look at 
select * from layoffs_staging2;

--everything looks good except apparently
we have some united sates and some united states
with a period at the end .lets standardize this

select distinct country
from layoffs_staging2
order by country; 

update layoffs_staging2
set country = trim(trailing '.' from country);     
--this query will successfully remove any trailing periods from the country column--

SET SQL_SAFE_UPDATES = 0;

--now if we run this again it is fixed 
select distinct country
from layoffs_staging2
order by country; 

--lets also fix the date column
select * from layoffs_staging2;

-- we can use str to date to update this field
update layoffs_staging2
set `date` = str_to_date(`date`,'%m/%d/%y');

--now we can convert the date typemproperly
update layoffs_staging2
set 'date' = 
  case 
   - HANDLE M/D/YYYY OR MM/DD/YYYY
WHEN 'DATE' REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
THEN STR_TO_DATE(DATE, '%m%d%Y')

-- HANDLE YYY-MM-DD
WHEN 'DATE' REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
THEN 'DATE'

-- NULL OR INVALID STAYS NULL
ELSE null
END;

SET SQL_SAFE_UPDATES = 0;
 
SELECT *
FROM layoffs_staging2;


-- 3. Look at Null Values
-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions 
--- all look normal. I don't think I want to change that
-- I like having them null because it makes it easier for calculations during the EDA phase
-- so there isn't anything I want to change with the null values
 
 -- 4. remove any columns and rows we need to
select * from layoffs_staging2
where total_laid_off is null;

select * from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

---delete useless data we cant really use
delete from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

SELECT * FROM layoffs_staging2;

alter table layoffs_staging2
drop column row_num;


-- EDA
-- Here we are jsut going to explore the data and find trends or patterns or 
--anything interesting like outliers
-- normally when you start the EDA process you have some idea of what you're looking for
-- with this info we are just going to look around and see what we find!
 
 SELECT * FROM layoffs_staging2;

--EASIER QUERIES 
SELECT MAX(total_laid_off)
FROM layoffs_staging2;
 
-- Looking at Percentage to see how big these layoffs were
SELECT MAX(percentage_laid_off),  MIN(percentage_laid_off)
FROM layoffs_staging2
WHERE  percentage_laid_off IS NOT NULL;
 
-- Which companies had 1 which is basically 100 percent of they company laid off
SELECT *
FROM layoffs_staging2
WHERE  percentage_laid_off = 1;

-- these are mostly startups it looks like who all went 
-- out of business during this time
-- if we order by funds_raised_millions we can see
--  how big some of these companies were

SELECT *
FROM layoffs_staging2
WHERE  percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;
 
-- BritishVolt looks like an EV company, Quibi! 
-- I recognize that company - wow raised like 2 billion 
-- dollars and went under - ouch
-- SOMEWHAT TOUGHER AND MOSTLY USING GROUP BY--------------------------------------------------------------------------------------------------
-- Companies with the biggest single DAY Layoff
 
SELECT company, total_laid_off
FROM layoffs_staging
ORDER BY 2 DESC
LIMIT 5;

-- now that's just on a single day
-- Companies with the most Total Layoffs
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC
LIMIT 10;
 
-- by location
SELECT location, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY location
ORDER BY 2 DESC
LIMIT 10;
 
--THIS IS TOTAL IN THE PAST 3 YRS OR IN DATASET
SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;
 
SELECT YEAR(date), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(date)
ORDER BY 1 ASC;
 
SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;
 
SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;


WITH Company_Year AS 
(
  SELECT company, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
  FROM layoffs_staging2
  GROUP BY company, YEAR(date)
)
, Company_Year_Rank AS (
  SELECT company, years, total_laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY 
  total_laid_off DESC) AS ranking
  FROM Company_Year
)
SELECT company, years, total_laid_off, ranking
FROM Company_Year_Rank
WHERE ranking <= 3
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;
 
-- rolling total of layoffs per month
select substring(date,1,7) as dates, SUM(total_laid_off) as total_laid_off
FROM layoffs_staging2
group by dates 
order by dates asc;

--now use it in a cte so we can query off of it 
WITH DATE_CTE AS 
(
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY dates
ORDER BY dates ASC
)
SELECT dates, SUM(total_laid_off) OVER (ORDER BY dates ASC) as rolling_total_layoffs
FROM DATE_CTE
ORDER BY dates ASC;
 

