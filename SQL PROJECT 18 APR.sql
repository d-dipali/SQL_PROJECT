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


-- it looks like these are all 



 






