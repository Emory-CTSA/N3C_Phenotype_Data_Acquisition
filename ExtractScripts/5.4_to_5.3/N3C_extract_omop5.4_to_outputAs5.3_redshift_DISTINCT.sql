/**
OMOP v5.3.1 extraction code for N3C
Author: Kristin Kostka (OHDSI), Robert Miller (Tufts)

HOW TO RUN:
If you are not using the R or Python exporters, you will need to find and replace @cdmDatabaseSchema and @resultsDatabaseSchema with your local OMOP schema details


USER NOTES:
This extract pulls the following OMOP tables: PERSON, OBSERVATION_PERIOD, VISIT_OCCURRENCE, CONDITION_OCCURRENCE, DRUG_EXPOSURE, PROCEDURE_OCCURRENCE, MEASUREMENT, OBSERVATION, LOCATION, CARE_SITE, PROVIDER, DEATH, DRUG_ERA, CONDITION_ERA
As an OMOP site, you are expected to be populating derived tables (OBSERVATION_PERIOD, DRUG_ERA, CONDITION_ERA)
Please refer to the OMOP site instructions for assistance on how to generate these tables.


SCRIPT ASSUMPTIONS:
1. You have already built the N3C_COHORT table (with that name) prior to running this extract
2. You are extracting data with a lookback period to 1-1-2018
3. You have existing tables for each of these extracted tables. If you do not, at a minimum, you MUST create a shell table so it can extract an empty table. Failure to create shells for missing table will result in ingestion problems.

RELEASE DATE: 2-10-2020
**/

--MANIFEST TABLE: CHANGE PER YOUR SITE'S SPECS
--OUTPUT_FILE: MANIFEST.csv
SELECT
   '@siteAbbrev' as SITE_ABBREV,
   '@siteName'    AS SITE_NAME,
   '@contactName' as CONTACT_NAME,
   '@contactEmail' as CONTACT_EMAIL,
   '@cdmName' as CDM_NAME,
   '@cdmVersion' as CDM_VERSION,
   (SELECT vocabulary_version FROM @resultsDatabaseSchema.N3C_PRE_COHORT LIMIT 1) AS VOCABULARY_VERSION,
   'Y' as N3C_PHENOTYPE_YN,
   (SELECT phenotype_version FROM @resultsDatabaseSchema.N3C_PRE_COHORT LIMIT 1) as N3C_PHENOTYPE_VERSION,
   '@shiftDateYN' as SHIFT_DATE_YN,
   '@maxNumShiftDays' as MAX_NUM_SHIFT_DAYS,
   CAST(CURRENT_DATE as TIMESTAMP) as RUN_DATE,
   CAST( (CURRENT_DATE + -@dataLatencyNumDays*INTERVAL'1 day') as TIMESTAMP) as UPDATE_DATE,	--change integer based on your site's data latency
   CAST( (CURRENT_DATE + @daysBetweenSubmissions*INTERVAL'1 day') as TIMESTAMP) as NEXT_SUBMISSION_DATE;

--VALIDATION_SCRIPT
--OUTPUT_FILE: EXTRACT_VALIDATION.csv
SELECT 
	'PERSON' TABLE_NAME
	,COUNT(*) DUP_COUNT
	,'person_id' DUPED_UNIQUE_COLUMN
    ,x.person_id val
FROM @cdmDatabaseSchema.PERSON x
INNER JOIN @resultsDatabaseSchema.N3C_COHORT n3c
ON x.person_id = n3c.person_id
GROUP BY x.person_id
HAVING COUNT(*) > 1

UNION
SELECT 
	'OBSERVATION_PERIOD' TABLE_NAME
	,COUNT(*) DUP_COUNT
	,'observation_period_id' DUPED_UNIQUE_COLUMN
    ,x.observation_period_id val
FROM @cdmDatabaseSchema.OBSERVATION_PERIOD x
INNER JOIN @resultsDatabaseSchema.N3C_COHORT n3c
ON x.person_id = n3c.person_id
AND x.observation_period_start_date > TO_DATE('2018-01-01', 'YYYY-MM-DD')
GROUP BY x.observation_period_id
HAVING COUNT(*) > 1

UNION
SELECT 
	'VISIT_OCCURRENCE' TABLE_NAME
	,COUNT(*) DUP_COUNT
	,'visit_occurrence_id' DUPED_UNIQUE_COLUMN
    ,x.visit_occurrence_id val
FROM @cdmDatabaseSchema.VISIT_OCCURRENCE x
INNER JOIN @resultsDatabaseSchema.N3C_COHORT n3c
ON x.person_id = n3c.person_id
AND x.visit_start_date > TO_DATE('2018-01-01', 'YYYY-MM-DD')
GROUP BY x.visit_occurrence_id
HAVING COUNT(*) > 1

UNION
SELECT 
	'CONDITION_OCCURRENCE' TABLE_NAME
	,COUNT(*) DUP_COUNT
	,'condition_occurrence_id' DUPED_UNIQUE_COLUMN
    ,x.condition_occurrence_id val
FROM @cdmDatabaseSchema.CONDITION_OCCURRENCE x
INNER JOIN @resultsDatabaseSchema.N3C_COHORT n3c
ON x.person_id = n3c.person_id
AND x.condition_start_date > TO_DATE('2018-01-01', 'YYYY-MM-DD')
GROUP BY x.condition_occurrence_id
HAVING COUNT(*) > 1

UNION
SELECT 
	'DRUG_EXPOSURE' TABLE_NAME
	,COUNT(*) DUP_COUNT
	,'drug_exposure_id' DUPED_UNIQUE_COLUMN
    ,x.drug_exposure_id val
FROM @cdmDatabaseSchema.DRUG_EXPOSURE x
INNER JOIN @resultsDatabaseSchema.N3C_COHORT n3c
ON x.person_id = n3c.person_id
AND x.drug_exposure_start_date > TO_DATE('2018-01-01', 'YYYY-MM-DD')
GROUP BY x.drug_exposure_id
HAVING COUNT(*) > 1

UNION
SELECT 
	'DEVICE_EXPOSURE' TABLE_NAME
	,COUNT(*) DUP_COUNT
	,'device_exposure_id' DUPED_UNIQUE_COLUMN
    ,x.device_exposure_id val
FROM @cdmDatabaseSchema.DEVICE_EXPOSURE x
INNER JOIN @resultsDatabaseSchema.N3C_COHORT n3c
ON x.person_id = n3c.person_id
AND x.device_exposure_start_date > TO_DATE('2018-01-01', 'YYYY-MM-DD')
GROUP BY x.device_exposure_id
HAVING COUNT(*) > 1

UNION
SELECT 
	'PROCEDURE_OCCURRENCE' TABLE_NAME
	,COUNT(*) DUP_COUNT
	,'procedure_occurrence_id' DUPED_UNIQUE_COLUMN
    ,x.procedure_occurrence_id val
FROM @cdmDatabaseSchema.PROCEDURE_OCCURRENCE x
INNER JOIN @resultsDatabaseSchema.N3C_COHORT n3c
ON x.person_id = n3c.person_id
AND x.procedure_date > TO_DATE('2018-01-01', 'YYYY-MM-DD')
GROUP BY x.procedure_occurrence_id
HAVING COUNT(*) > 1

UNION
SELECT 
	'MEASUREMENT' TABLE_NAME
	,COUNT(*) DUP_COUNT
	,'measurement_id' DUPED_UNIQUE_COLUMN
    ,x.measurement_id val
FROM @cdmDatabaseSchema.MEASUREMENT x
INNER JOIN @resultsDatabaseSchema.N3C_COHORT n3c
ON x.person_id = n3c.person_id
AND x.measurement_date > TO_DATE('2018-01-01', 'YYYY-MM-DD')
GROUP BY x.measurement_id
HAVING COUNT(*) > 1

UNION
SELECT 
	'OBSERVATION' TABLE_NAME
	,COUNT(*) DUP_COUNT
	,'observation_id' DUPED_UNIQUE_COLUMN
    ,x.observation_id val
FROM @cdmDatabaseSchema.OBSERVATION x
INNER JOIN @resultsDatabaseSchema.N3C_COHORT n3c
ON x.person_id = n3c.person_id
AND x.observation_date > TO_DATE('2018-01-01', 'YYYY-MM-DD')
GROUP BY x.observation_id
HAVING COUNT(*) > 1

UNION
SELECT 
	'LOCATION' TABLE_NAME
	,COUNT(*) DUP_COUNT
	,'location_id' DUPED_UNIQUE_COLUMN
    ,x.location_id val
FROM @cdmDatabaseSchema.LOCATION x
GROUP BY x.location_id
HAVING COUNT(*) > 1

UNION
SELECT 
	'CARE_SITE' TABLE_NAME
	,COUNT(*) DUP_COUNT
	,'care_site_id' DUPED_UNIQUE_COLUMN
    ,x.care_site_id val
FROM @cdmDatabaseSchema.CARE_SITE x
GROUP BY x.care_site_id
HAVING COUNT(*) > 1

UNION
SELECT 
	'PROVIDER' TABLE_NAME
	,COUNT(*) DUP_COUNT
	,'' DUPED_UNIQUE_COLUMN
    ,x.provider_id val
FROM @cdmDatabaseSchema.PROVIDER x
GROUP BY x.provider_id
HAVING COUNT(*) > 1

UNION
SELECT 
	'DRUG_ERA' TABLE_NAME
	,COUNT(*) DUP_COUNT
	,'drug_era_id' DUPED_UNIQUE_COLUMN
    ,x.drug_era_id val
FROM @cdmDatabaseSchema.DRUG_ERA x
INNER JOIN @resultsDatabaseSchema.N3C_COHORT n3c
ON x.person_id = n3c.person_id
AND x.drug_era_start_date > TO_DATE('2018-01-01', 'YYYY-MM-DD')
GROUP BY x.drug_era_id
HAVING COUNT(*) > 1

UNION
SELECT 
	'CONDITION_ERA' TABLE_NAME
	,COUNT(*) DUP_COUNT
	,'condition_era_id' DUPED_UNIQUE_COLUMN
    ,x.condition_era_id val
FROM @cdmDatabaseSchema.CONDITION_ERA x
INNER JOIN @resultsDatabaseSchema.N3C_COHORT n3c
ON x.person_id = n3c.person_id
AND x.condition_era_start_date > TO_DATE('2018-01-01', 'YYYY-MM-DD')
GROUP BY x.condition_era_id
HAVING COUNT(*) > 1;

--PERSON
--OUTPUT_FILE: PERSON.csv
SELECT DISTINCT
   p.PERSON_ID,
   GENDER_CONCEPT_ID,
   COALESCE(YEAR_OF_BIRTH,DATE_PART('year', birth_datetime )) as YEAR_OF_BIRTH,
   COALESCE(MONTH_OF_BIRTH,DATE_PART('month', birth_datetime)) as MONTH_OF_BIRTH,
   RACE_CONCEPT_ID,
   ETHNICITY_CONCEPT_ID,
   NULL as LOCATION_ID,
   PROVIDER_ID,
   CARE_SITE_ID,
   NULL as PERSON_SOURCE_VALUE,
   GENDER_SOURCE_VALUE,
   RACE_SOURCE_VALUE,
   RACE_SOURCE_CONCEPT_ID,
   ETHNICITY_SOURCE_VALUE,
   ETHNICITY_SOURCE_CONCEPT_ID
  FROM @cdmDatabaseSchema.PERSON p
  JOIN @resultsDatabaseSchema.N3C_COHORT n
    ON p.PERSON_ID = n.PERSON_ID;

--OBSERVATION_PERIOD
--OUTPUT_FILE: OBSERVATION_PERIOD.csv
SELECT DISTINCT
   OBSERVATION_PERIOD_ID,
   p.PERSON_ID,
   CAST(OBSERVATION_PERIOD_START_DATE as TIMESTAMP) as OBSERVATION_PERIOD_START_DATE,
   CAST(OBSERVATION_PERIOD_END_DATE as TIMESTAMP) as OBSERVATION_PERIOD_END_DATE,
   PERIOD_TYPE_CONCEPT_ID
 FROM @cdmDatabaseSchema.OBSERVATION_PERIOD p
 JOIN @resultsDatabaseSchema.N3C_COHORT n
   ON p.PERSON_ID = n.PERSON_ID
   AND (p.OBSERVATION_PERIOD_START_DATE >= TO_DATE('2018-01-01', 'YYYY-MM-DD')
      OR p.OBSERVATION_PERIOD_END_DATE >= TO_DATE('2018-01-01', 'YYYY-MM-DD'));

--VISIT_OCCURRENCE
--OUTPUT_FILE: VISIT_OCCURRENCE.csv
SELECT DISTINCT
   VISIT_OCCURRENCE_ID,
   n.PERSON_ID,
   VISIT_CONCEPT_ID,
   CAST(VISIT_START_DATE as TIMESTAMP) as VISIT_START_DATE,
   CAST(VISIT_START_DATETIME as TIMESTAMP) as VISIT_START_DATETIME,
   CAST(VISIT_END_DATE as TIMESTAMP) as VISIT_END_DATE,
   CAST(VISIT_END_DATETIME as TIMESTAMP) as VISIT_END_DATETIME,
   VISIT_TYPE_CONCEPT_ID,
   PROVIDER_ID,
   CARE_SITE_ID,
   VISIT_SOURCE_VALUE,
   VISIT_SOURCE_CONCEPT_ID,
   Admitted_from_concept_id as ADMITTING_SOURCE_CONCEPT_ID, -- omop 5.4 vocab to omop 5.3
   Admitted_from_source_value as ADMITTING_SOURCE_VALUE, -- see above
   Discharged_to_concept_id as DISCHARGE_TO_CONCEPT_ID, -- see above
   Discharged_to_source_value as DISCHARGE_TO_SOURCE_VALUE, -- see above
   PRECEDING_VISIT_OCCURRENCE_ID
FROM @cdmDatabaseSchema.VISIT_OCCURRENCE v
JOIN @resultsDatabaseSchema.N3C_COHORT n
  ON v.PERSON_ID = n.PERSON_ID
WHERE v.VISIT_START_DATE >= TO_DATE('2018-01-01', 'YYYY-MM-DD')
  AND @dateRangePartition;

--CONDITION_OCCURRENCE
--OUTPUT_FILE: CONDITION_OCCURRENCE.csv
SELECT DISTINCT
   CONDITION_OCCURRENCE_ID,
   n.PERSON_ID,
   CONDITION_CONCEPT_ID,
   CAST(CONDITION_START_DATE as TIMESTAMP) as CONDITION_START_DATE,
   CAST(CONDITION_START_DATETIME as TIMESTAMP) as CONDITION_START_DATETIME,
   CAST(CONDITION_END_DATE as TIMESTAMP) as CONDITION_END_DATE,
   CAST(CONDITION_END_DATETIME as TIMESTAMP) as CONDITION_END_DATETIME,
   CONDITION_TYPE_CONCEPT_ID,
   CONDITION_STATUS_CONCEPT_ID,
   NULL as STOP_REASON,
   VISIT_OCCURRENCE_ID,
   NULL as VISIT_DETAIL_ID,
   CONDITION_SOURCE_VALUE,
   CONDITION_SOURCE_CONCEPT_ID,
   NULL as CONDITION_STATUS_SOURCE_VALUE
FROM @cdmDatabaseSchema.CONDITION_OCCURRENCE co
JOIN @resultsDatabaseSchema.N3C_COHORT n
  ON CO.person_id = n.person_id
WHERE co.CONDITION_START_DATE >= TO_DATE('2018-01-01', 'YYYY-MM-DD')
  AND @dateRangePartition;

--DRUG_EXPOSURE
--OUTPUT_FILE: DRUG_EXPOSURE.csv
SELECT DISTINCT
   DRUG_EXPOSURE_ID,
   n.PERSON_ID,
   DRUG_CONCEPT_ID,
   CAST(DRUG_EXPOSURE_START_DATE as TIMESTAMP) as DRUG_EXPOSURE_START_DATE,
   CAST(DRUG_EXPOSURE_START_DATETIME as TIMESTAMP) as DRUG_EXPOSURE_START_DATETIME,
   CAST(DRUG_EXPOSURE_END_DATE as TIMESTAMP) as DRUG_EXPOSURE_END_DATE,
   CAST(DRUG_EXPOSURE_END_DATETIME as TIMESTAMP) as DRUG_EXPOSURE_END_DATETIME,
   DRUG_TYPE_CONCEPT_ID,
   NULL as STOP_REASON,
   REFILLS,
   QUANTITY,
   DAYS_SUPPLY,
   NULL as SIG,
   ROUTE_CONCEPT_ID,
   LOT_NUMBER,
   PROVIDER_ID,
   VISIT_OCCURRENCE_ID,
   null as VISIT_DETAIL_ID,
   DRUG_SOURCE_VALUE,
   DRUG_SOURCE_CONCEPT_ID,
   ROUTE_SOURCE_VALUE,
   DOSE_UNIT_SOURCE_VALUE
FROM @cdmDatabaseSchema.DRUG_EXPOSURE de
JOIN @resultsDatabaseSchema.N3C_COHORT n
  ON de.PERSON_ID = n.PERSON_ID
WHERE de.DRUG_EXPOSURE_START_DATE >= TO_DATE('2018-01-01', 'YYYY-MM-DD') and @dateRangePartition;

--DEVICE_EXPOSURE
--OUTPUT_FILE: DEVICE_EXPOSURE.csv
SELECT DISTINCT
   DEVICE_EXPOSURE_ID,
   n.PERSON_ID,
   DEVICE_CONCEPT_ID,
   CAST(DEVICE_EXPOSURE_START_DATE as TIMESTAMP) as DEVICE_EXPOSURE_START_DATE,
   CAST(DEVICE_EXPOSURE_START_DATETIME as TIMESTAMP) as DEVICE_EXPOSURE_START_DATETIME,
   CAST(DEVICE_EXPOSURE_END_DATE as TIMESTAMP) as DEVICE_EXPOSURE_END_DATE,
   CAST(DEVICE_EXPOSURE_END_DATETIME as TIMESTAMP) as DEVICE_EXPOSURE_END_DATETIME,
   DEVICE_TYPE_CONCEPT_ID,
   NULL as UNIQUE_DEVICE_ID,
   QUANTITY,
   PROVIDER_ID,
   VISIT_OCCURRENCE_ID,
   NULL as VISIT_DETAIL_ID,
   DEVICE_SOURCE_VALUE,
   DEVICE_SOURCE_CONCEPT_ID
FROM @cdmDatabaseSchema.DEVICE_EXPOSURE de
JOIN @resultsDatabaseSchema.N3C_COHORT n
  ON de.PERSON_ID = n.PERSON_ID
WHERE de.DEVICE_EXPOSURE_START_DATE >= TO_DATE('2018-01-01', 'YYYY-MM-DD');

--PROCEDURE_OCCURRENCE
--OUTPUT_FILE: PROCEDURE_OCCURRENCE.csv
SELECT DISTINCT
   PROCEDURE_OCCURRENCE_ID,
   n.PERSON_ID,
   PROCEDURE_CONCEPT_ID,
   CAST(PROCEDURE_DATE as TIMESTAMP) as PROCEDURE_DATE,
   CAST(PROCEDURE_DATETIME as TIMESTAMP) as PROCEDURE_DATETIME,
   PROCEDURE_TYPE_CONCEPT_ID,
   MODIFIER_CONCEPT_ID,
   QUANTITY,
   PROVIDER_ID,
   VISIT_OCCURRENCE_ID,
   NULL as VISIT_DETAIL_ID,
   PROCEDURE_SOURCE_VALUE,
   PROCEDURE_SOURCE_CONCEPT_ID,
   NULL as MODIFIER_SOURCE_VALUE
FROM @cdmDatabaseSchema.PROCEDURE_OCCURRENCE po
JOIN @resultsDatabaseSchema.N3C_COHORT n
  ON PO.PERSON_ID = N.PERSON_ID
WHERE po.PROCEDURE_DATE >= TO_DATE('2018-01-01', 'YYYY-MM-DD');

--MEASUREMENT
--OUTPUT_FILE: MEASUREMENT.csv
SELECT DISTINCT
   MEASUREMENT_ID,
   n.PERSON_ID,
   MEASUREMENT_CONCEPT_ID,
   CAST(MEASUREMENT_DATE as TIMESTAMP) as MEASUREMENT_DATE,
   CAST(MEASUREMENT_DATETIME as TIMESTAMP) as MEASUREMENT_DATETIME,
   NULL as MEASUREMENT_TIME,
   MEASUREMENT_TYPE_CONCEPT_ID,
   OPERATOR_CONCEPT_ID,
   VALUE_AS_NUMBER,
   VALUE_AS_CONCEPT_ID,
   UNIT_CONCEPT_ID,
   RANGE_LOW,
   RANGE_HIGH,
   PROVIDER_ID,
   VISIT_OCCURRENCE_ID,
   NULL as VISIT_DETAIL_ID,
   MEASUREMENT_SOURCE_VALUE,
   MEASUREMENT_SOURCE_CONCEPT_ID,
   NULL as UNIT_SOURCE_VALUE,
   NULL as VALUE_SOURCE_VALUE
FROM @cdmDatabaseSchema.MEASUREMENT m
JOIN @resultsDatabaseSchema.N3C_COHORT n
  ON M.PERSON_ID = N.PERSON_ID
WHERE @dateRangePartition;

--OBSERVATION
--OUTPUT_FILE: OBSERVATION.csv
SELECT DISTINCT
   OBSERVATION_ID,
   n.PERSON_ID,
   OBSERVATION_CONCEPT_ID,
   CAST(OBSERVATION_DATE as TIMESTAMP) as OBSERVATION_DATE,
   CAST(OBSERVATION_DATETIME as TIMESTAMP) as OBSERVATION_DATETIME,
   OBSERVATION_TYPE_CONCEPT_ID,
   VALUE_AS_NUMBER,
   VALUE_AS_STRING,
   VALUE_AS_CONCEPT_ID,
   QUALIFIER_CONCEPT_ID,
   UNIT_CONCEPT_ID,
   PROVIDER_ID,
   VISIT_OCCURRENCE_ID,
   NULL as VISIT_DETAIL_ID,
   OBSERVATION_SOURCE_VALUE,
   OBSERVATION_SOURCE_CONCEPT_ID,
   NULL as UNIT_SOURCE_VALUE,
   NULL as QUALIFIER_SOURCE_VALUE
FROM @cdmDatabaseSchema.OBSERVATION o
JOIN @resultsDatabaseSchema.N3C_COHORT n
  ON O.PERSON_ID = N.PERSON_ID
WHERE o.OBSERVATION_DATE >= TO_DATE('2018-01-01', 'YYYY-MM-DD')
  AND @dateRangePartition;

--DEATH
--OUTPUT_FILE: DEATH.csv
SELECT DISTINCT
   n.PERSON_ID,
    CAST(DEATH_DATE as TIMESTAMP) as DEATH_DATE,
	CAST(DEATH_DATETIME as TIMESTAMP) as DEATH_DATETIME,
	DEATH_TYPE_CONCEPT_ID,
	CAUSE_CONCEPT_ID,
	NULL as CAUSE_SOURCE_VALUE,
	CAUSE_SOURCE_CONCEPT_ID
FROM @cdmDatabaseSchema.DEATH d
JOIN @resultsDatabaseSchema.N3C_COHORT n
ON D.PERSON_ID = N.PERSON_ID
WHERE d.DEATH_DATE >= TO_DATE('2020-01-01', 'YYYY-MM-DD');

--LOCATION
--OUTPUT_FILE: LOCATION.csv
SELECT DISTINCT
   l.LOCATION_ID,
   null as ADDRESS_1, -- to avoid identifying information
   null as ADDRESS_2, -- to avoid identifying information
   CITY,
   STATE,
   ZIP,
   COUNTY,
   NULL as LOCATION_SOURCE_VALUE
FROM @cdmDatabaseSchema.LOCATION l
JOIN (
        SELECT DISTINCT p.LOCATION_ID
        FROM @cdmDatabaseSchema.PERSON p
        JOIN @resultsDatabaseSchema.N3C_COHORT n
          ON p.person_id = n.person_id
      ) a
  ON l.location_id = a.location_id
;

--CARE_SITE
--OUTPUT_FILE: CARE_SITE.csv
SELECT DISTINCT
   cs.CARE_SITE_ID,
   CARE_SITE_NAME,
   PLACE_OF_SERVICE_CONCEPT_ID,
   NULL as LOCATION_ID,
   NULL as CARE_SITE_SOURCE_VALUE,
   NULL as PLACE_OF_SERVICE_SOURCE_VALUE
FROM @cdmDatabaseSchema.CARE_SITE cs
JOIN (
        SELECT DISTINCT CARE_SITE_ID
        FROM @cdmDatabaseSchema.VISIT_OCCURRENCE vo
        JOIN @resultsDatabaseSchema.N3C_COHORT n
          ON vo.person_id = n.person_id
      ) a
  ON cs.CARE_SITE_ID = a.CARE_SITE_ID
;

--PROVIDER
--OUTPUT_FILE: PROVIDER.csv
SELECT DISTINCT
   pr.PROVIDER_ID,
   null as PROVIDER_NAME, -- to avoid accidentally identifying sites
   null as NPI, -- to avoid accidentally identifying sites
   null as DEA, -- to avoid accidentally identifying sites
   SPECIALTY_CONCEPT_ID,
   CARE_SITE_ID,
   null as YEAR_OF_BIRTH,
   GENDER_CONCEPT_ID,
   null as PROVIDER_SOURCE_VALUE, -- to avoid accidentally identifying sites
   SPECIALTY_SOURCE_VALUE,
   SPECIALTY_SOURCE_CONCEPT_ID,
   GENDER_SOURCE_VALUE,
   GENDER_SOURCE_CONCEPT_ID
FROM @cdmDatabaseSchema.PROVIDER pr
JOIN (
       SELECT DISTINCT PROVIDER_ID
       FROM @cdmDatabaseSchema.VISIT_OCCURRENCE vo
       JOIN @resultsDatabaseSchema.N3C_COHORT n
          ON vo.PERSON_ID = n.PERSON_ID
       UNION
       SELECT DISTINCT PROVIDER_ID
       FROM @cdmDatabaseSchema.DRUG_EXPOSURE de
       JOIN @resultsDatabaseSchema.N3C_COHORT n
          ON de.PERSON_ID = n.PERSON_ID
       UNION
       SELECT DISTINCT PROVIDER_ID
       FROM @cdmDatabaseSchema.MEASUREMENT m
       JOIN @resultsDatabaseSchema.N3C_COHORT n
          ON m.PERSON_ID = n.PERSON_ID
       UNION
       SELECT DISTINCT PROVIDER_ID
       FROM @cdmDatabaseSchema.PROCEDURE_OCCURRENCE po
       JOIN @resultsDatabaseSchema.N3C_COHORT n
          ON po.PERSON_ID = n.PERSON_ID
       UNION
       SELECT DISTINCT PROVIDER_ID
       FROM @cdmDatabaseSchema.OBSERVATION o
       JOIN @resultsDatabaseSchema.N3C_COHORT n
          ON o.PERSON_ID = n.PERSON_ID
     ) a
 ON pr.PROVIDER_ID = a.PROVIDER_ID
;

--DRUG_ERA
--OUTPUT_FILE: DRUG_ERA.csv
SELECT DISTINCT
   DRUG_ERA_ID,
   n.PERSON_ID,
   DRUG_CONCEPT_ID,
   CAST(DRUG_ERA_START_DATE as TIMESTAMP) as DRUG_ERA_START_DATE,
   CAST(DRUG_ERA_END_DATE as TIMESTAMP) as DRUG_ERA_END_DATE,
   DRUG_EXPOSURE_COUNT,
   GAP_DAYS
FROM @cdmDatabaseSchema.DRUG_ERA dre
JOIN @resultsDatabaseSchema.N3C_COHORT n
  ON DRE.PERSON_ID = N.PERSON_ID
WHERE DRUG_ERA_START_DATE >= TO_DATE('2018-01-01', 'YYYY-MM-DD');

--CONDITION_ERA
--OUTPUT_FILE: CONDITION_ERA.csv
SELECT DISTINCT
   CONDITION_ERA_ID,
   n.PERSON_ID,
   CONDITION_CONCEPT_ID,
   CAST(CONDITION_ERA_START_DATE as TIMESTAMP) as CONDITION_ERA_START_DATE,
   CAST(CONDITION_ERA_END_DATE as TIMESTAMP) as CONDITION_ERA_END_DATE,
   CONDITION_OCCURRENCE_COUNT
FROM @cdmDatabaseSchema.CONDITION_ERA ce JOIN @resultsDatabaseSchema.N3C_COHORT n ON CE.PERSON_ID = N.PERSON_ID
WHERE CONDITION_ERA_START_DATE >= TO_DATE('2018-01-01', 'YYYY-MM-DD');

--DATA_COUNTS TABLE
--OUTPUT_FILE: DATA_COUNTS.csv
SELECT * from
(select
   'PERSON' as TABLE_NAME,
   (select count(DISTINCT p.person_id) from @cdmDatabaseSchema.PERSON p JOIN @resultsDatabaseSchema.N3C_COHORT n ON p.PERSON_ID = n.PERSON_ID) as ROW_COUNT

UNION

select
   'OBSERVATION_PERIOD' as TABLE_NAME,
   (select count(DISTINCT op.observation_period_id) from @cdmDatabaseSchema.OBSERVATION_PERIOD op JOIN @resultsDatabaseSchema.N3C_COHORT n ON op.PERSON_ID = n.PERSON_ID AND (OBSERVATION_PERIOD_START_DATE >= TO_DATE(TO_CHAR(2018,'0000')||'-'||TO_CHAR(01,'00')||'-'||TO_CHAR(01,'00'), 'YYYY-MM-DD') OR OBSERVATION_PERIOD_END_DATE >= TO_DATE(TO_CHAR(2018,'0000')||'-'||TO_CHAR(01,'00')||'-'||TO_CHAR(01,'00'), 'YYYY-MM-DD'))) as ROW_COUNT

UNION

select
   'VISIT_OCCURRENCE' as TABLE_NAME,
   (select count(DISTINCT vo.visit_occurrence_id) from @cdmDatabaseSchema.VISIT_OCCURRENCE vo JOIN @resultsDatabaseSchema.N3C_COHORT n ON vo.PERSON_ID = n.PERSON_ID AND VISIT_START_DATE >= TO_DATE(TO_CHAR(2018,'0000')||'-'||TO_CHAR(01,'00')||'-'||TO_CHAR(01,'00'), 'YYYY-MM-DD')) as ROW_COUNT

UNION

select
   'CONDITION_OCCURRENCE' as TABLE_NAME,
   (select count(DISTINCT co.condition_occurrence_id) from @cdmDatabaseSchema.CONDITION_OCCURRENCE co JOIN @resultsDatabaseSchema.N3C_COHORT n ON co.PERSON_ID = n.PERSON_ID AND CONDITION_START_DATE >= TO_DATE(TO_CHAR(2018,'0000')||'-'||TO_CHAR(01,'00')||'-'||TO_CHAR(01,'00'), 'YYYY-MM-DD')) as ROW_COUNT

UNION

select
   'DRUG_EXPOSURE' as TABLE_NAME,
   (select count(DISTINCT de.drug_exposure_id) from @cdmDatabaseSchema.DRUG_EXPOSURE de JOIN @resultsDatabaseSchema.N3C_COHORT n ON de.PERSON_ID = n.PERSON_ID AND DRUG_EXPOSURE_START_DATE >= TO_DATE(TO_CHAR(2018,'0000')||'-'||TO_CHAR(01,'00')||'-'||TO_CHAR(01,'00'), 'YYYY-MM-DD')) as ROW_COUNT

UNION

select
   'DEVICE_EXPOSURE' as TABLE_NAME,
   (select count(DISTINCT de.device_exposure_id) from @cdmDatabaseSchema.DEVICE_EXPOSURE de JOIN @resultsDatabaseSchema.N3C_COHORT n ON de.PERSON_ID = n.PERSON_ID AND DEVICE_EXPOSURE_START_DATE >= TO_DATE(TO_CHAR(2018,'0000')||'-'||TO_CHAR(01,'00')||'-'||TO_CHAR(01,'00'), 'YYYY-MM-DD')) as ROW_COUNT

UNION

select
   'PROCEDURE_OCCURRENCE' as TABLE_NAME,
   (select count(DISTINCT po.procedure_occurrence_id) from @cdmDatabaseSchema.PROCEDURE_OCCURRENCE po JOIN @resultsDatabaseSchema.N3C_COHORT n ON po.PERSON_ID = n.PERSON_ID AND PROCEDURE_DATE >= TO_DATE(TO_CHAR(2018,'0000')||'-'||TO_CHAR(01,'00')||'-'||TO_CHAR(01,'00'), 'YYYY-MM-DD')) as ROW_COUNT

UNION

select
   'MEASUREMENT' as TABLE_NAME,
   (select count(DISTINCT m.measurement_id) from @cdmDatabaseSchema.MEASUREMENT m JOIN @resultsDatabaseSchema.N3C_COHORT n ON m.PERSON_ID = n.PERSON_ID AND MEASUREMENT_DATE >= TO_DATE(TO_CHAR(2018,'0000')||'-'||TO_CHAR(01,'00')||'-'||TO_CHAR(01,'00'), 'YYYY-MM-DD')) as ROW_COUNT

UNION

select
   'OBSERVATION' as TABLE_NAME,
   (select count(DISTINCT o.observation_id) from @cdmDatabaseSchema.OBSERVATION o JOIN @resultsDatabaseSchema.N3C_COHORT n ON o.PERSON_ID = n.PERSON_ID AND OBSERVATION_DATE >= TO_DATE(TO_CHAR(2018,'0000')||'-'||TO_CHAR(01,'00')||'-'||TO_CHAR(01,'00'), 'YYYY-MM-DD')) as ROW_COUNT

UNION

SELECT
   'DEATH' as TABLE_NAME,
  (select count(*) from @cdmDatabaseSchema.DEATH d JOIN @resultsDatabaseSchema.N3C_COHORT n ON d.PERSON_ID = n.PERSON_ID AND DEATH_DATE >= TO_DATE(TO_CHAR(2020,'0000')||'-'||TO_CHAR(01,'00')||'-'||TO_CHAR(01,'00'), 'YYYY-MM-DD')) as ROW_COUNT
  -- no distinct possible in count.  may need to place in subquery
UNION

select
   'LOCATION' as TABLE_NAME,
   (select count(*) from @cdmDatabaseSchema.LOCATION l
   JOIN (
        SELECT DISTINCT p.LOCATION_ID
        FROM @cdmDatabaseSchema.PERSON p
        JOIN @resultsDatabaseSchema.N3C_COHORT n
          ON p.person_id = n.person_id
      ) a
  ON l.location_id = a.location_id) as ROW_COUNT

UNION

select
   'CARE_SITE' as TABLE_NAME,
   (select count(*) from @cdmDatabaseSchema.CARE_SITE cs
	JOIN (
        SELECT DISTINCT CARE_SITE_ID
        FROM @cdmDatabaseSchema.VISIT_OCCURRENCE vo
        JOIN @resultsDatabaseSchema.N3C_COHORT n
          ON vo.person_id = n.person_id
      ) a
  ON cs.CARE_SITE_ID = a.CARE_SITE_ID) as ROW_COUNT

UNION

 select
   'PROVIDER' as TABLE_NAME,
   (select count(DISTINCT pr.provider_id) from @cdmDatabaseSchema.PROVIDER pr
	JOIN (
       SELECT DISTINCT PROVIDER_ID
       FROM @cdmDatabaseSchema.VISIT_OCCURRENCE vo
       JOIN @resultsDatabaseSchema.N3C_COHORT n
          ON vo.PERSON_ID = n.PERSON_ID
       UNION
       SELECT DISTINCT PROVIDER_ID
       FROM @cdmDatabaseSchema.DRUG_EXPOSURE de
       JOIN @resultsDatabaseSchema.N3C_COHORT n
          ON de.PERSON_ID = n.PERSON_ID
       UNION
       SELECT DISTINCT PROVIDER_ID
       FROM @cdmDatabaseSchema.MEASUREMENT m
       JOIN @resultsDatabaseSchema.N3C_COHORT n
          ON m.PERSON_ID = n.PERSON_ID
       UNION
       SELECT DISTINCT PROVIDER_ID
       FROM @cdmDatabaseSchema.PROCEDURE_OCCURRENCE po
       JOIN @resultsDatabaseSchema.N3C_COHORT n
          ON po.PERSON_ID = n.PERSON_ID
       UNION
       SELECT DISTINCT PROVIDER_ID
       FROM @cdmDatabaseSchema.OBSERVATION o
       JOIN @resultsDatabaseSchema.N3C_COHORT n
          ON o.PERSON_ID = n.PERSON_ID
     ) a
 ON pr.PROVIDER_ID = a.PROVIDER_ID) as ROW_COUNT

UNION

select
   'DRUG_ERA' as TABLE_NAME,
   (select count(distinct de.drug_era_id) from @cdmDatabaseSchema.DRUG_ERA de JOIN @resultsDatabaseSchema.N3C_COHORT n ON de.PERSON_ID = n.PERSON_ID AND DRUG_ERA_START_DATE >= TO_DATE(TO_CHAR(2018,'0000')||'-'||TO_CHAR(01,'00')||'-'||TO_CHAR(01,'00'), 'YYYY-MM-DD')) as ROW_COUNT

UNION

select
   'CONDITION_ERA' as TABLE_NAME,
   (select count(distinct ce.condition_era_id) from @cdmDatabaseSchema.CONDITION_ERA ce JOIN @resultsDatabaseSchema.N3C_COHORT ON ce.PERSON_ID = N3C_COHORT.PERSON_ID AND CONDITION_ERA_START_DATE >= TO_DATE(TO_CHAR(2018,'0000')||'-'||TO_CHAR(01,'00')||'-'||TO_CHAR(01,'00'), 'YYYY-MM-DD')) as ROW_COUNT
) s;


--n3c_control_map
--OUTPUT_FILE: N3C_CONTROL_MAP.csv
SELECT DISTINCT *
FROM @resultsDatabaseSchema.N3C_CONTROL_MAP;