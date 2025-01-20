

-- 1. Eliminar duplicados.
-- 2. Estandarizar data.
-- 3. Null values y espacios en blanco
-- 4. Eliminar columnas innecesarias.

create table layoffs_staging
Like layoffs;

SELECT*
FROM layoffs_staging;

-- Crear una copia de datos.
INSERT layoffs_staging
SELECT *
FROM layoffs;

-- Encontrar datos duplicados.
WITH duplicate_cte AS (
SELECT *,
ROW_NUMBER() OVER(partition by company,location,industry, total_laid_off, percentage_laid_off, `date`,stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT*
FROM duplicate_cte
WHERE row_num > 1;

SELECT*
FROM layoffs_staging
WHERE company = 'Casper';

-- Borramos datos duplicados.
WITH duplicate_cte AS 
(
SELECT *,
ROW_NUMBER() OVER(
partition by company,location,industry, total_laid_off, percentage_laid_off, 'date',stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging
)
DELETE
FROM duplicate_cte
WHERE row_num > 1;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


SELECT*
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(partition by company,location,industry, total_laid_off, percentage_laid_off, `date`,stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging;


-- Estandarizar datos.

SELECT company, trim(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = trim(company);


SELECT distinct country, trim(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry like 'Crypto%';

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' from country)
WHERE country like 'United States%';

SELECT `date`,
str_to_date(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
modify column `date` DATE;

SELECT*
FROM layoffs_staging2;

-- Eliminar espacios en blanco y null values.


SELECT*
FROM layoffs_staging2
WHERE total_laid_off is null
AND percentage_laid_off is null;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layoffs_staging2
WHERE industry is null OR
industry = '';

SELECT*
FROM layoffs_staging2
WHERE company like 'Bally%';

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry is null or t1.industry = '')
AND t2.industry is not null;


UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE(t1.industry is null or t1.industry = '')
AND t2.industry is not null;


SELECT*
FROM layoffs_staging2;


DELETE
FROM layoffs_staging2
WHERE total_laid_off is null
AND percentage_laid_off is null;

ALTER TABLE layoffs_staging2
DROP column row_num;


-- Exploratory data anlysis


SELECT*
FROM layoffs_staging2;


SELECT MAX(total_laid_off), max(percentage_laid_off)
FROM layoffs_staging2;


SELECT*
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions desc;

-- Total de despidos por compañias.
SELECT company, sum(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 desc;


SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

-- Total de despidos por industria.
SELECT industry, sum(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 desc;


SELECT country, sum(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 desc;

-- Total de despidos por año.
SELECT YEAR(`date`), sum(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 desc;


SELECT stage, sum(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 desc;


SELECT SUBSTRING(`date`,1,7) AS `Month`, sum(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) is not null
GROUP BY `Month`
ORDER BY 1 ASC;


WITH Rolling_total AS
(
SELECT SUBSTRING(`date`,1,7) AS `Month`, sum(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) is not null
GROUP BY `Month`
ORDER BY 1 ASC
)
SELECT `Month`, total_off
,sum(total_off) OVER(ORDER BY `Month`) AS rolling_total
FROM Rolling_total;



SELECT company, YEAR(`date`), sum(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 desc;

-- Top 5 compañias con el mayor numero de despidos por año.
WITH company_year (company, years, total_laid_off) AS
( -- CTE company total laid off by year --
SELECT company, YEAR(`date`), sum(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
), Company_year_rank AS
( -- CTE ranking company total laid off by year --
SELECT*, DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
FROM company_year
WHERE years is not null
) -- TOP 5 company laid off by year --
SELECT*
FROM Company_year_rank
WHERE ranking <= 5;


















