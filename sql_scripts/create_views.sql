-- sql_scripts/create_views.sql (CORRIGIDO com aspas duplas)

-- VIEW 1: Resultado de cada corredor (quantidade de vitórias e quantidade de pontos) por ano
CREATE OR REPLACE VIEW view_driver_yearly_results AS
SELECT
    r.year AS ano,
    d.fullname AS driver_fullname,
    c.name AS constructor_name,
    -- Usar aspas duplas nas colunas com letras maiúsculas
    SUM(CASE WHEN res."positionOrder" = 1 THEN 1 ELSE 0 END) AS qtd_vitorias,
    SUM(res.points) AS qtd_pontos
FROM
    results res
-- Usar aspas duplas nas colunas da junção (JOIN)
JOIN
    races r ON res."raceId" = r."raceId"
JOIN
    drivers d ON res."driverId" = d."driverId"
JOIN
    constructors c ON res."constructorId" = c."constructorId"
GROUP BY
    r.year,
    d.fullname,
    c.name
ORDER BY
    r.year,
    d.fullname;

-- VIEW 2: Para cada Grand Prix, qual corredor fez a volta mais rápida, e em que data
CREATE OR REPLACE VIEW view_grand_prix_fastest_laps AS
WITH ranked_laps AS (
    SELECT
        ra.date AS race_date,
        ra.name AS grand_prix_name,
        -- Usar aspas duplas nas colunas com letras maiúsculas
        re."fastestLapTime" AS fastest_lap_time,
        d.fullname AS driver_fullname,
        c.name AS constructor_name,
        ROW_NUMBER() OVER (PARTITION BY ra."raceId" ORDER BY re."fastestLapTime" ASC) as rn
    FROM
        results re
    -- Usar aspas duplas nas colunas da junção (JOIN)
    JOIN
        races ra ON re."raceId" = ra."raceId"
    JOIN
        drivers d ON re."driverId" = d."driverId"
    JOIN
        constructors c ON re."constructorId" = c."constructorId"
    WHERE
        re."fastestLapTime" IS NOT NULL
)
SELECT
    race_date,
    grand_prix_name,
    fastest_lap_time,
    driver_fullname,
    constructor_name
FROM
    ranked_laps
WHERE
    rn = 1
ORDER BY
    race_date DESC,
    grand_prix_name;