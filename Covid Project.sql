USE Projescts
go

SELECT*
FROM
	CovidDeaths
ORDER BY
	location, date
go

-- Seleccionar data que vamos a usar

SELECT
	location,
	date,
	total_cases,
	new_cases,
	total_deaths,
	population
FROM
	CovidDeaths
ORDER BY
	location,date
go

-- Casos totales vs muertes totales

SELECT
	location,
	date,
	total_cases,
	total_deaths,
	(total_deaths/total_cases) * 100 as DeathPercentage
FROM
	CovidDeaths
WHERE
	location like '%spain%'
ORDER BY
	location,date
go

-- Casos totales vs Poblacion

SELECT
	location,
	date,
	total_cases,
	population,
	(total_cases/population) * 100 
FROM
	CovidDeaths
WHERE
	location like '%spain%'
ORDER BY
	location,date
go


-- Paises con mayor ratio de infeccion por Poblacion

SELECT
	location,
	population,
	max(total_cases) as HighestInfectionCases,
	max(total_cases/population) * 100 as PopulationInfected
FROM
	CovidDeaths
GROUP BY
	location,
	population
ORDER BY
	PopulationInfected desc
go


-- Paises con el mayor ratio de muertes por poblacion

SELECT
	location,
	max(total_deaths) as TotalDeathCount
FROM
	CovidDeaths
WHERE
	continent is not null
GROUP BY
	location,
	population
ORDER BY
	TotalDeathCount DESC
go

-- Lo mismo pero por continente

SELECT
	location,
	max(total_deaths) as TotalDeathCount
FROM
	CovidDeaths
WHERE
	continent is null
GROUP BY
	location
ORDER BY
	TotalDeathCount DESC
go


-- Numeros Globales.

SELECT
	SUM(new_cases) as total_cases,
	SUM(cast(new_deaths as int)) as total_deaths,
	SUM(cast(new_deaths as int))/SUM(new_cases)*100 as 'Death Percentage'
FROM
	CovidDeaths
WHERE
	continent is null
ORDER BY
	date, total_cases
go


-- Poblacion total vs Vacunaciones
-- Muestra el porcentage de la poblacion que se ha puesto al menos una vacuna del covid.

SELECT
	cd.continent,
	cd.location,
	cd.date,
	cd.population,
	cv.new_vaccinations,
	SUM(convert(int, cv.new_vaccinations)) OVER (Partition by cd.location ORDER BY cd.location, cd.date) as 'Total personas vacunadas'
FROM
	CovidDeaths AS cd
	JOIN CovidVaccinations as cv ON cv.location = cd.location and cv.date = cd.date
WHERE
	cd.continent is not null
ORDER BY
	cd.location, cd.date
go


-- CTE para mejorar el calculo de la consulta anterior

WITH PoblvsVac (Continent, Location, Date, Population, New_Vaccinations, [Total de personas vacunadas]) as (
	SELECT
		cd.continent,
		cd.location,
		cd.date,
		cd.population,
		cv.new_vaccinations,
		SUM(convert(int, cv.new_vaccinations)) OVER (Partition by cd.location ORDER BY cd.location, cd.date) as 'Total personas vacunadas'
	FROM
		CovidDeaths AS cd
		JOIN CovidVaccinations as cv ON cv.location = cd.location and cv.date = cd.date
	WHERE
		cd.continent is not null
)
SELECT *,
	([Total personas vacunadas]/Population)*100
FROM
	PoblvsVac
go


-- Usar una tabla temporal para mejorar el calculo de la consulta anterior

DROP TABLE if exists #PercentPopulationVaccinated
create table #PercentPopulationVaccinated
(
	Continent varchar(255),
	Location varchar(255),
	Date datetime,
	Population numeric,
	New_vaccinations numeric,
	[Total personas vacunadas] numeric
)

INSERT INTO #PercentPopulationVaccinated
	SELECT
		cd.continent,
		cd.location,
		cd.date,
		cd.population,
		cv.new_vaccinations,
		SUM(convert(int, cv.new_vaccinations)) OVER (Partition by cd.location ORDER BY cd.location, cd.date) as 'Total personas vacunadas'
	FROM
		CovidDeaths AS cd
		JOIN CovidVaccinations as cv ON cv.location = cd.location and cv.date = cd.date

SELECT *,
	([Total personas vacunadas]/Population)*100
FROM
	#PercentPopulationVaccinated
go


-- Crear view para visualizaciones futuras

CREATE VIEW PercentPopulationVaccinated as 
	SELECT
		cd.continent,
		cd.location,
		cd.date,
		cd.population,
		cv.new_vaccinations,
		SUM(convert(int, cv.new_vaccinations)) OVER (Partition by cd.location ORDER BY cd.location, cd.date) as 'Total personas vacunadas'
	FROM
		CovidDeaths AS cd
		JOIN CovidVaccinations as cv ON cv.location = cd.location and cv.date = cd.date
	WHERE
		cd.continent is not null