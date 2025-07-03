/**
OMOP v5.3.1 extraction code for N3C
Author: Matt Pagel (Emory), Kristin Kostka (OHDSI), Robert Miller (Tufts)

HOW TO RUN:
If you are not using the R or Python exporters, you will need to find and replace @cdmDatabaseSchema and @resultsDatabaseSchema with your local OMOP schema details

USER NOTES:
This extract pulls the following OMOP tables: PERSON, OBSERVATION_PERIOD, VISIT_OCCURRENCE, CONDITION_OCCURRENCE, DRUG_EXPOSURE, PROCEDURE_OCCURRENCE, MEASUREMENT, OBSERVATION, LOCATION, CARE_SITE, PROVIDER, DEATH, DRUG_ERA, CONDITION_ERA and VISIT_DETAIL
As an OMOP site, you are expected to be populating derived tables (OBSERVATION_PERIOD, DRUG_ERA, CONDITION_ERA)
Please refer to the OMOP site instructions for assistance on how to generate these tables.


SCRIPT ASSUMPTIONS:
0. You have an OMOP CDM 5.4 data source, but the project requires 5.3.1
1. You have already built the CLAD_COHORT table (name specified by `@cohortTable`) prior to running this extract
2. You have run the AoU_create_crosswalk script to create mapping tables (as prefixed by `scram_`) for time-sensitive data points that overlap any part of the lookback period between @startDate and @endDate
3. You want to change any end dates that are after the lookback period to NULL
4. You wish to leave start dates unaltered, even if they are from before the lookback period began
5. You have existing tables in the `@cdmDatabaseSchema` for each of these extracted tables. If you do not, at a minimum, you MUST create a shell table so it can extract an empty table. Failure to create shells for missing table will result in ingestion problems.

RELEASE DATE: 2020-02-10
MODIFICATION DATE: 2025-05-09
**/

-- NO MANIFEST included for this project. This will be done on the foundry platform

--VALIDATION_SCRIPT
--OUTPUT_FILE: EXTRACT_VALIDATION.csv
SELECT 
   TABLE_NAME
   ,numRows
   ,numRaw
   ,numHash
   ,diffRaw
   ,diffHash
FROM @resultsDatabaseSchema.v_table_stats -- see AoU_create_crosswalk.sql in PhenotypeScripts directory
WHERE diffRaw > 0 or diffHash > 0;

--PERSON
--OUTPUT_FILE: PERSON.csv
SELECT
   c.pmid as PERSON_ID, -- p.person_id
   CASE 
      WHEN count(distinct GENDER_CONCEPT_ID) <=1  THEN MIN(GENDER_CONCEPT_ID)
      ELSE 0
   END as GENDER_CONCEPT_ID,
   COALESCE(YEAR_OF_BIRTH,DATE_PART('year', birth_datetime )) as YEAR_OF_BIRTH,
   COALESCE(MONTH_OF_BIRTH,DATE_PART('month', birth_datetime)) as MONTH_OF_BIRTH,
   COALESCE(DAY_OF_BIRTH,DATE_PART('day', birth_datetime)) as DAY_OF_BIRTH,
   birth_datetime,
   CASE 
      WHEN count(distinct RACE_CONCEPT_ID) <=1  THEN MIN(RACE_CONCEPT_ID)
      ELSE 0
   END as RACE_CONCEPT_ID,
   CASE 
      WHEN count(distinct ETHNICITY_CONCEPT_ID) <=1  THEN MIN(ETHNICITY_CONCEPT_ID)
      ELSE 0
   END as ETHNICITY_CONCEPT_ID,
   l.newID as LOCATION_ID,
   pr.newID as PROVIDER_ID,
   cs.newID as CARE_SITE_ID,
   NULL as PERSON_SOURCE_VALUE,
   CASE 
      WHEN count(distinct GENDER_SOURCE_VALUE) <=1  THEN MIN(GENDER_SOURCE_VALUE)
      ELSE '' -- LISTAGG(distinct GENDER_SOURCE_VALUE, ',') WITHIN GROUP (ORDER BY GENDER_SOURCE_VALUE)
   END as GENDER_SOURCE_VALUE,
   NULL as gender_source_concept_id,
   CASE 
      WHEN count(distinct RACE_SOURCE_VALUE) <=1  THEN MIN(RACE_SOURCE_VALUE)
      ELSE '' -- LISTAGG(distinct RACE_SOURCE_VALUE, ',') WITHIN GROUP (ORDER BY RACE_SOURCE_VALUE)
   END as RACE_SOURCE_VALUE,
   CASE 
      WHEN count(distinct RACE_SOURCE_CONCEPT_ID) <=1  THEN MIN(RACE_SOURCE_CONCEPT_ID)
      ELSE 0
   END as RACE_SOURCE_CONCEPT_ID,
   CASE 
      WHEN count(distinct ETHNICITY_SOURCE_VALUE) <=1  THEN MIN(ETHNICITY_SOURCE_VALUE)
      ELSE '' -- LISTAGG(distinct ETHNICITY_SOURCE_VALUE, ',') WITHIN GROUP (ORDER BY ETHNICITY_SOURCE_VALUE)
   END as ETHNICITY_SOURCE_VALUE,
   CASE 
      WHEN count(distinct ETHNICITY_SOURCE_CONCEPT_ID) <=1  THEN MIN(ETHNICITY_SOURCE_CONCEPT_ID)
      ELSE 0
   END as ETHNICITY_SOURCE_CONCEPT_ID
FROM @cdmDatabaseSchema.PERSON p
   JOIN @resultsDatabaseSchema.CLAD_COHORT c
     ON p.PERSON_ID = c.PERSON_ID
   LEFT JOIN @resultsDatabaseSchema.scram_LOCATION l
     ON p.location_id = l.oldid
   LEFT JOIN @resultsDatabaseSchema.scram_PROVIDER pr
     ON p.provider_id = pr.oldid
   LEFT JOIN @resultsDatabaseSchema.scram_CARE_SITE cs
     ON p.care_site_id = cs.oldid
GROUP BY
   p.person_id, c.pmid,
	birth_datetime,YEAR_OF_BIRTH,MONTH_OF_BIRTH,DAY_OF_BIRTH,
	p.location_id,l.newID,
	p.provider_id,pr.newID,
	p.care_site_id,cs.newID
;

--OBSERVATION_PERIOD
--OUTPUT_FILE: OBSERVATION_PERIOD.csv
SELECT
   n.newID as OBSERVATION_PERIOD_ID,
   c.pmid as PERSON_ID,
   CAST(OBSERVATION_PERIOD_START_DATE as TIMESTAMP) as OBSERVATION_PERIOD_START_DATE,
   CASE 
      WHEN CAST(OBSERVATION_PERIOD_END_DATE as TIMESTAMP) > TO_DATE('@endDate','YYYY-MM-DD')
      THEN NULL
      ELSE CAST(OBSERVATION_PERIOD_END_DATE as TIMESTAMP)
   END as OBSERVATION_PERIOD_END_DATE, -- fix for post study end dates
   PERIOD_TYPE_CONCEPT_ID
 FROM @cdmDatabaseSchema.OBSERVATION_PERIOD o
   JOIN @resultsDatabaseSchema.scram_OBSERVATION_PERIOD n
      ON o.OBSERVATION_PERIOD_ID = n.oldID
   JOIN @resultsDatabaseSchema.CLAD_COHORT c
      ON o.PERSON_ID = c.PERSON_ID
;

--VISIT_OCCURRENCE
--OUTPUT_FILE: VISIT_OCCURRENCE.csv
SELECT
   n.newID as VISIT_OCCURRENCE_ID,
   c.pmid as PERSON_ID,
   VISIT_CONCEPT_ID,
   CAST(VISIT_START_DATE as TIMESTAMP) as VISIT_START_DATE,
   CAST(VISIT_START_DATETIME as TIMESTAMP) as VISIT_START_DATETIME,
   CASE 
      WHEN CAST(VISIT_END_DATE as TIMESTAMP) > TO_DATE('@endDate','YYYY-MM-DD')
      THEN NULL
      ELSE CAST(VISIT_END_DATE as TIMESTAMP) 
   END as VISIT_END_DATE, -- fix for post study end dates
   CASE 
      WHEN CAST(VISIT_END_DATETIME as TIMESTAMP) > TO_DATE('@endDate','YYYY-MM-DD')
      THEN NULL
      ELSE CAST(VISIT_END_DATETIME as TIMESTAMP) 
   END as VISIT_END_DATETIME, -- fix for post study end dates
   VISIT_TYPE_CONCEPT_ID,
   pr.newID as PROVIDER_ID,
   cs.newID as CARE_SITE_ID,
   VISIT_SOURCE_VALUE,
   NULL as VISIT_SOURCE_CONCEPT_ID
   ,NULL as ADMITTING_SOURCE_CONCEPT_ID-- Admitted_from_concept_id as ADMITTING_SOURCE_CONCEPT_ID -- omop 5.4 vocab to omop 5.3
   ,Admitted_from_source_value as ADMITTING_SOURCE_VALUE -- see above
   ,NULL as DISCHARGE_TO_CONCEPT_ID-- Discharged_to_concept_id as DISCHARGE_TO_CONCEPT_ID -- see above
   ,Discharged_to_source_value as DISCHARGE_TO_SOURCE_VALUE -- see above
   ,pv.newID as PRECEDING_VISIT_OCCURRENCE_ID -- pv.newID as PRECEDING_VISIT_OCCURRENCE_ID
FROM @cdmDatabaseSchema.VISIT_OCCURRENCE o
JOIN @resultsDatabaseSchema.scram_VISIT_OCCURRENCE n
   ON o.VISIT_OCCURRENCE_ID = n.oldid
JOIN @resultsDatabaseSchema.CLAD_COHORT c
  ON o.PERSON_ID = c.PERSON_ID
LEFT JOIN @resultsDatabaseSchema.scram_PROVIDER pr
   ON o.provider_id = pr.oldid
LEFT JOIN @resultsDatabaseSchema.scram_CARE_SITE cs
   ON o.care_site_id = cs.oldid
LEFT JOIN @resultsDatabaseSchema.scram_VISIT_OCCURRENCE pv
   on o.PRECEDING_VISIT_OCCURRENCE_ID = pv.oldID
;

--CONDITION_OCCURRENCE
--OUTPUT_FILE: CONDITION_OCCURRENCE.csv
SELECT
   n.newID as CONDITION_OCCURRENCE_ID,
   c.pmid as PERSON_ID,
   CONDITION_CONCEPT_ID, -- AoU spec says to populate condition_code_source_value instead of this, but that is non-CDM
   CAST(CONDITION_START_DATE as TIMESTAMP) as CONDITION_START_DATE,
   CAST(CONDITION_START_DATETIME as TIMESTAMP) as CONDITION_START_DATETIME,
   CASE 
      WHEN CAST(CONDITION_END_DATE as TIMESTAMP) > TO_DATE('@endDate','YYYY-MM-DD')
      THEN NULL
      ELSE CAST(CONDITION_END_DATE as TIMESTAMP) 
   END as CONDITION_END_DATE, -- fix for post study end dates
   CASE 
      WHEN CAST(CONDITION_END_DATETIME as TIMESTAMP) > TO_DATE('@endDate','YYYY-MM-DD')
      THEN NULL
      ELSE CAST(CONDITION_END_DATETIME as TIMESTAMP) 
   END as CONDITION_END_DATETIME, -- fix for post study end dates
   CONDITION_TYPE_CONCEPT_ID,
   NULL as CONDITION_STATUS_CONCEPT_ID,
   STOP_REASON, -- NULL in N3C
   pr.newID as PROVIDER_ID,
   vo.newID as VISIT_OCCURRENCE_ID,
   vd.newID as VISIT_DETAIL_ID, --not present in AoU spec
   CONDITION_SOURCE_VALUE,
   NULL as CONDITION_SOURCE_CONCEPT_ID,  -- blanked in AoU spec
   CONDITION_STATUS_SOURCE_VALUE -- null in n3c
   
FROM @cdmDatabaseSchema.CONDITION_OCCURRENCE o
join @resultsDatabaseSchema.scram_CONDITION_OCCURRENCE n
   ON o.condition_occurrence_id = n.oldid
JOIN @resultsDatabaseSchema.CLAD_COHORT c
  ON o.PERSON_ID = c.PERSON_ID
LEFT JOIN @resultsDatabaseSchema.scram_PROVIDER pr
   ON o.provider_id = pr.oldid
LEFT JOIN @resultsDatabaseSchema.scram_VISIT_OCCURRENCE vo
   on o.VISIT_OCCURRENCE_ID = vo.oldID
LEFT JOIN @resultsDatabaseSchema.scram_VISIT_DETAIL vd
   on o.VISIT_DETAIL_ID = vd.oldID
;

--DRUG_EXPOSURE
--OUTPUT_FILE: DRUG_EXPOSURE.csv
SELECT
   n.newID as DRUG_EXPOSURE_ID,
   c.pmid as PERSON_ID,
   DRUG_CONCEPT_ID, -- AoU says to blank this and that they'll use drug_source_value, but that may also require drug_code_type_source_value
   CAST(DRUG_EXPOSURE_START_DATE as TIMESTAMP) as DRUG_EXPOSURE_START_DATE,
   CAST(DRUG_EXPOSURE_START_DATETIME as TIMESTAMP) as DRUG_EXPOSURE_START_DATETIME,
   CASE 
      WHEN CAST(DRUG_EXPOSURE_END_DATE as TIMESTAMP) > TO_DATE('@endDate','YYYY-MM-DD')
      THEN NULL
      ELSE CAST(DRUG_EXPOSURE_END_DATE as TIMESTAMP) 
   END as DRUG_EXPOSURE_END_DATE, -- fix for post study end dates
   CASE 
      WHEN CAST(DRUG_EXPOSURE_END_DATETIME as TIMESTAMP) > TO_DATE('@endDate','YYYY-MM-DD')
      THEN NULL
      ELSE CAST(DRUG_EXPOSURE_END_DATETIME as TIMESTAMP) 
   END as DRUG_EXPOSURE_END_DATETIME, -- fix for post study end dates
   DRUG_TYPE_CONCEPT_ID,
   CASE
      WHEN CAST(VERBATIM_END_DATE as TIMESTAMP) > TO_DATE('@endDate','YYYY-MM-DD')
      THEN NULL 
      ELSE CAST(VERBATIM_END_DATE as TIMESTAMP) 
   END as VERBATIM_END_DATE, -- fix for post study end dates
   STOP_REASON,
   REFILLS,
   QUANTITY,
   DAYS_SUPPLY,
   NULL as SIG, -- AoU wants this, but it is said to be a clob value, which is not readily CSV'd.  Null in N3C
   ROUTE_CONCEPT_ID,
   LOT_NUMBER,
   pr.newID as PROVIDER_ID,
   vo.newID as VISIT_OCCURRENCE_ID,
   vd.newID as VISIT_DETAIL_ID,
   -- AoU refers to a drug_code_type_source_value field here, but that is not in the OMOP spec
   DRUG_SOURCE_VALUE,
   DRUG_SOURCE_CONCEPT_ID, -- AoU says to blank this out, but it is probably related to drug_code_type_source_value
   ROUTE_SOURCE_VALUE,
   DOSE_UNIT_SOURCE_VALUE
FROM @cdmDatabaseSchema.DRUG_EXPOSURE O
JOIN @resultsDatabaseSchema.scram_drug_exposure n
   ON o.DRUG_EXPOSURE_ID = n.oldid
JOIN @resultsDatabaseSchema.CLAD_COHORT c
  ON o.PERSON_ID = c.PERSON_ID
left join @resultsDatabaseSchema.scram_PROVIDER pr
   on o.PROVIDER_ID = pr.oldid
LEFT JOIN @resultsDatabaseSchema.scram_VISIT_OCCURRENCE vo
   on o.VISIT_OCCURRENCE_ID = vo.oldID
LEFT JOIN @resultsDatabaseSchema.scram_VISIT_DETAIL vd
   on o.VISIT_DETAIL_ID = vd.oldID
;

--DEVICE_EXPOSURE
--OUTPUT_FILE: DEVICE_EXPOSURE.csv
SELECT
   n.newID as DEVICE_EXPOSURE_ID,
   c.pmid as PERSON_ID,
   DEVICE_CONCEPT_ID,
   CAST(DEVICE_EXPOSURE_START_DATE as TIMESTAMP) as DEVICE_EXPOSURE_START_DATE,
   CAST(DEVICE_EXPOSURE_START_DATETIME as TIMESTAMP) as DEVICE_EXPOSURE_START_DATETIME,
   CASE 
      WHEN CAST(DEVICE_EXPOSURE_END_DATE as TIMESTAMP) > TO_DATE('@endDate','YYYY-MM-DD')
      THEN NULL
      ELSE CAST(DEVICE_EXPOSURE_END_DATE as TIMESTAMP) 
   END as DEVICE_EXPOSURE_END_DATE, -- fix for post study end dates
   CASE 
      WHEN CAST(DEVICE_EXPOSURE_END_DATETIME as TIMESTAMP) > TO_DATE('@endDate','YYYY-MM-DD')
      THEN NULL
      ELSE CAST(DEVICE_EXPOSURE_END_DATETIME as TIMESTAMP) 
   END as DEVICE_EXPOSURE_END_DATETIME, -- fix for post study end dates
   DEVICE_TYPE_CONCEPT_ID,
   UNIQUE_DEVICE_ID, -- N3C sets this to null
   QUANTITY,
   pr.newID as PROVIDER_ID,
   vo.newID as VISIT_OCCURRENCE_ID,
   vd.newID as VISIT_DETAIL_ID,
   DEVICE_SOURCE_VALUE,
   DEVICE_SOURCE_CONCEPT_ID
FROM @cdmDatabaseSchema.DEVICE_EXPOSURE o
JOIN @resultsDatabaseSchema.scram_DEVICE_EXPOSURE n
   on o.DEVICE_EXPOSURE_ID = n.oldID
JOIN @resultsDatabaseSchema.CLAD_COHORT c
  ON o.PERSON_ID = c.PERSON_ID
left join @resultsDatabaseSchema.scram_PROVIDER pr
   on o.PROVIDER_ID = pr.oldid
LEFT JOIN @resultsDatabaseSchema.scram_VISIT_OCCURRENCE vo
   on o.VISIT_OCCURRENCE_ID = vo.oldID
LEFT JOIN @resultsDatabaseSchema.scram_VISIT_DETAIL vd
   on o.VISIT_DETAIL_ID = vd.oldID
;

--PROCEDURE_OCCURRENCE
--OUTPUT_FILE: PROCEDURE_OCCURRENCE.csv
SELECT
   n.newID as PROCEDURE_OCCURRENCE_ID,
   c.pmid as PERSON_ID,
   PROCEDURE_CONCEPT_ID,
   CAST(PROCEDURE_DATE as TIMESTAMP) as PROCEDURE_DATE,
   CAST(PROCEDURE_DATETIME as TIMESTAMP) as PROCEDURE_DATETIME,
   PROCEDURE_TYPE_CONCEPT_ID,
   MODIFIER_CONCEPT_ID,
   QUANTITY,
   pr.newID as PROVIDER_ID,
   vo.newID as VISIT_OCCURRENCE_ID,
   vd.newID as VISIT_DETAIL_ID,
   PROCEDURE_SOURCE_VALUE,
   PROCEDURE_SOURCE_CONCEPT_ID, -- AoU asks for a code_type_source_value instead of this one
   MODIFIER_SOURCE_VALUE
FROM @cdmDatabaseSchema.PROCEDURE_OCCURRENCE o
join @resultsDatabaseSchema.scram_PROCEDURE_OCCURRENCE n
   on o.PROCEDURE_OCCURRENCE_ID = n.oldID
JOIN @resultsDatabaseSchema.CLAD_COHORT c
  ON o.PERSON_ID = c.PERSON_ID
left join @resultsDatabaseSchema.scram_PROVIDER pr
   on o.PROVIDER_ID = pr.oldid
LEFT JOIN @resultsDatabaseSchema.scram_VISIT_OCCURRENCE vo
   on o.VISIT_OCCURRENCE_ID = vo.oldID
LEFT JOIN @resultsDatabaseSchema.scram_VISIT_DETAIL vd
   on o.VISIT_DETAIL_ID = vd.oldID
;

--MEASUREMENT
--OUTPUT_FILE: MEASUREMENT.csv
SELECT
   n.newID as MEASUREMENT_ID,
   c.pmid as PERSON_ID,
   MEASUREMENT_CONCEPT_ID,
   CAST(MEASUREMENT_DATE as TIMESTAMP) as MEASUREMENT_DATE,
   CAST(MEASUREMENT_DATETIME as TIMESTAMP) as MEASUREMENT_DATETIME,
   NULL as MEASUREMENT_TIME,
   MEASUREMENT_TYPE_CONCEPT_ID,
   OPERATOR_CONCEPT_ID, -- AoU says to leave this blank, but refers to a operator_concept_source_value which is not part of the OMOP CDM
   VALUE_AS_NUMBER,
   VALUE_AS_CONCEPT_ID,
   UNIT_CONCEPT_ID,
   RANGE_LOW,
   RANGE_HIGH,
   pr.newID as PROVIDER_ID,
   vo.newID as VISIT_OCCURRENCE_ID,
   vd.newID as VISIT_DETAIL_ID,
   MEASUREMENT_SOURCE_VALUE,
   NULL as MEASUREMENT_SOURCE_CONCEPT_ID,
   UNIT_SOURCE_VALUE, --null in n3c
   value_source_value --null in n3c
   --AoU refers to a operator_concept_source_value here, but that is not part of the OMOP CDM
FROM @cdmDatabaseSchema.MEASUREMENT o
join @resultsDatabaseSchema.scram_MEASUREMENT n
   on o.MEASUREMENT_ID = n.oldID
JOIN @resultsDatabaseSchema.CLAD_COHORT c
  ON o.PERSON_ID = c.PERSON_ID
left join @resultsDatabaseSchema.scram_PROVIDER pr
   on o.PROVIDER_ID = pr.oldid
LEFT JOIN @resultsDatabaseSchema.scram_VISIT_OCCURRENCE vo
   on o.VISIT_OCCURRENCE_ID = vo.oldID
LEFT JOIN @resultsDatabaseSchema.scram_VISIT_DETAIL vd
   on o.VISIT_DETAIL_ID = vd.oldID
WHERE @dateRangePartition
;

--OBSERVATION
--OUTPUT_FILE: OBSERVATION.csv
SELECT
   n.newID as OBSERVATION_ID,
   c.pmid as PERSON_ID,
   OBSERVATION_CONCEPT_ID,
   CAST(OBSERVATION_DATE as TIMESTAMP) as OBSERVATION_DATE,
   CAST(OBSERVATION_DATETIME as TIMESTAMP) as OBSERVATION_DATETIME,
   OBSERVATION_TYPE_CONCEPT_ID,
   VALUE_AS_NUMBER,
   VALUE_AS_STRING,
   VALUE_AS_CONCEPT_ID, --AoU says to have this blank, but refers later to a field that does not actually exist
   QUALIFIER_CONCEPT_ID,
   UNIT_CONCEPT_ID,
   pr.newID as PROVIDER_ID,
   vo.newID as VISIT_OCCURRENCE_ID,
   vd.newID as VISIT_DETAIL_ID,
   OBSERVATION_SOURCE_VALUE,
   OBSERVATION_SOURCE_CONCEPT_ID,
   UNIT_SOURCE_VALUE,
   QUALIFIER_SOURCE_VALUE
   -- observation_type_source_value is referred to in the AoU docs, but is not part of the CDM
FROM @cdmDatabaseSchema.OBSERVATION o
join @resultsDatabaseSchema.scram_OBSERVATION n
   ON o.OBSERVATION_ID = n.oldID
JOIN @resultsDatabaseSchema.CLAD_COHORT c
  ON o.PERSON_ID = c.PERSON_ID
left join @resultsDatabaseSchema.scram_PROVIDER pr
   on o.PROVIDER_ID = pr.oldid
LEFT JOIN @resultsDatabaseSchema.scram_VISIT_OCCURRENCE vo
   on o.VISIT_OCCURRENCE_ID = vo.oldID
LEFT JOIN @resultsDatabaseSchema.scram_VISIT_DETAIL vd
   on o.VISIT_DETAIL_ID = vd.oldID
WHERE @dateRangePartition;

--DEATH
--OUTPUT_FILE: DEATH.csv
SELECT
   c.pmid as PERSON_ID,
   CAST(DEATH_DATE as TIMESTAMP) as DEATH_DATE,
	CAST(DEATH_DATETIME as TIMESTAMP) as DEATH_DATETIME,
	DEATH_TYPE_CONCEPT_ID, -- AoU says to leave this blank and they will fill, but refers to a death_type_source_value that doesn't exist in the CDM
	CAUSE_CONCEPT_ID,
	CAUSE_SOURCE_VALUE,
	CAUSE_SOURCE_CONCEPT_ID
FROM @cdmDatabaseSchema.DEATH o
JOIN @resultsDatabaseSchema.CLAD_COHORT c
   ON o.PERSON_ID = c.PERSON_ID
   AND all_of_us.overlaps_bool(TO_DATE('@startDate','YYYY-MM-DD'),TO_DATE('@endDate','YYYY-MM-DD'),DEATH_DATE,NULL)
;

--LOCATION
--OUTPUT_FILE: LOCATION.csv
SELECT
   n.newID as LOCATION_ID,
   ADDRESS_1, -- N3C suppresses to avoid identifying information
   ADDRESS_2, -- N3C null to avoid identifying information
   CITY,
   STATE,
   ZIP,
   COUNTY,
   NULL as LOCATION_SOURCE_VALUE -- aou wants this, but I see no need
FROM @cdmDatabaseSchema.LOCATION o
JOIN @resultsDatabaseSchema.scram_LOCATION n
   on o.LOCATION_ID = n.oldID
--JOIN (
  --      SELECT DISTINCT p.LOCATION_ID
    --    FROM @cdmDatabaseSchema.PERSON p
      --  JOIN @resultsDatabaseSchema.CLAD_COHORT n
        --  ON p.person_id = n.person_id
--      ) a
  -- ON l.location_id = a.location_id
;

--CARE_SITE
--OUTPUT_FILE: CARE_SITE.csv
SELECT
   n.newID as CARE_SITE_ID,
   CARE_SITE_NAME,
   NULL as PLACE_OF_SERVICE_CONCEPT_ID,
   LOCATION_ID,
   CARE_SITE_SOURCE_VALUE,
   PLACE_OF_SERVICE_SOURCE_VALUE
FROM @cdmDatabaseSchema.CARE_SITE o
JOIN @resultsDatabaseSchema.scram_CARE_SITE n
   on o.CARE_SITE_ID = n.oldID
--JOIN (
  --      SELECT DISTINCT CARE_SITE_ID
    --    FROM @cdmDatabaseSchema.VISIT_OCCURRENCE vo
      --  JOIN @resultsDatabaseSchema.CLAD_COHORT n
        --  ON vo.person_id = n.person_id
--      ) a
  --ON cs.CARE_SITE_ID = a.CARE_SITE_ID
;

--PROVIDER
--OUTPUT_FILE: PROVIDER.csv
SELECT
   n.newID as PROVIDER_ID,
   PROVIDER_NAME, -- N3C suppresses to avoid accidentally identifying sites
   null as NPI, -- N3C suppresses to avoid accidentally identifying sites
   null as DEA, -- N3C suppresses to avoid accidentally identifying sites
   SPECIALTY_CONCEPT_ID,
   cs.newID as CARE_SITE_ID,
   YEAR_OF_BIRTH,
   GENDER_CONCEPT_ID,
   PROVIDER_SOURCE_VALUE, -- N3C suppresses to avoid accidentally identifying sites
   SPECIALTY_SOURCE_VALUE,
   SPECIALTY_SOURCE_CONCEPT_ID,
   GENDER_SOURCE_VALUE,
   GENDER_SOURCE_CONCEPT_ID
FROM @cdmDatabaseSchema.PROVIDER o
JOIN @resultsDatabaseSchema.scram_PROVIDER n
   ON o.PROVIDER_ID = n.oldID
left join @resultsDatabaseSchema.scram_CARE_SITE cs
   ON o.care_site_id = cs.oldID
;

--DRUG_ERA
--OUTPUT_FILE: DRUG_ERA.csv
SELECT
   n.newID as DRUG_ERA_ID,
   c.pmid as PERSON_ID,
   DRUG_CONCEPT_ID,
   CAST(DRUG_ERA_START_DATE as TIMESTAMP) as DRUG_ERA_START_DATE,
   CASE 
      WHEN CAST(DRUG_ERA_END_DATE as TIMESTAMP) > TO_DATE('@endDate','YYYY-MM-DD')
      THEN NULL
      ELSE CAST(DRUG_ERA_END_DATE as TIMESTAMP) 
   END as DRUG_ERA_END_DATE, -- fix for post study end dates
   DRUG_EXPOSURE_COUNT,
   GAP_DAYS
FROM @cdmDatabaseSchema.DRUG_ERA o
JOIN @resultsDatabaseSchema.scram_DRUG_ERA n
   ON o.DRUG_ERA_ID = n.oldID
JOIN @resultsDatabaseSchema.CLAD_COHORT c
  ON o.PERSON_ID = c.PERSON_ID
;

--CONDITION_ERA
--OUTPUT_FILE: CONDITION_ERA.csv
SELECT
   n.newID as CONDITION_ERA_ID,
   c.pmid as PERSON_ID,
   CONDITION_CONCEPT_ID,
   CAST(CONDITION_ERA_START_DATE as TIMESTAMP) as CONDITION_ERA_START_DATE,
   CASE 
      WHEN CAST(CONDITION_ERA_END_DATE as TIMESTAMP) > TO_DATE('@endDate','YYYY-MM-DD')
      THEN NULL
      ELSE CAST(CONDITION_ERA_END_DATE as TIMESTAMP) 
   END as CONDITION_ERA_END_DATE, -- fix for post study end dates
   CONDITION_OCCURRENCE_COUNT
FROM @cdmDatabaseSchema.CONDITION_ERA o
JOIN @resultsDatabaseSchema.scram_CONDITION_ERA n
   ON o.CONDITION_ERA_ID = n.oldID
JOIN @resultsDatabaseSchema.CLAD_COHORT c
  ON o.PERSON_ID = c.PERSON_ID
;

--VISIT_DETAIL
--OUTPUT_FILE: VISIT_DETAIL.csv
SELECT
   n.newID as VISIT_DETAIL_ID,
   c.pmid as person_id,
   visit_detail_concept_id,
   CAST(visit_detail_start_date as TIMESTAMP) as visit_detail_start_date,
   CAST(visit_detail_start_datetime as TIMESTAMP) as visit_detail_start_datetime,
   CASE 
      WHEN CAST(visit_detail_end_date as TIMESTAMP) > TO_DATE('@endDate','YYYY-MM-DD')
      THEN NULL
      ELSE CAST(visit_detail_end_date as TIMESTAMP) 
   END as visit_detail_end_date, -- fix for post study end dates
   CASE 
      WHEN CAST(visit_detail_end_datetime as TIMESTAMP) > TO_DATE('@endDate','YYYY-MM-DD')
      THEN NULL
      ELSE CAST(visit_detail_end_datetime as TIMESTAMP) 
   END as visit_detail_end_datetime, -- fix for post study end dates
   visit_detail_type_concept_id,
   pr.newID as provider_id,
   cs.newID as care_site_id,
   visit_detail_source_value,
   visit_detail_source_concept_id,
   admitted_from_concept_id,
   NULL as admitted_from_source_value,
   NULL as discharged_to_source_value,
   discharged_to_concept_id,
   pr.newID as preceding_visit_detail_id,
   pa.newID as parent_visit_detail_id,
   vo.newID as visit_occurrence_id
FROM @cdmDatabaseSchema.VISIT_DETAIL o
JOIN @resultsDatabaseSchema.scram_VISIT_DETAIL n
   ON o.VISIT_DETAIL_ID = n.oldID
JOIN @resultsDatabaseSchema.scram_VISIT_OCCURRENCE vo
   ON o.VISIT_OCCURRENCE_ID = vo.oldID
JOIN @resultsDatabaseSchema.CLAD_COHORT c
  ON o.PERSON_ID = c.PERSON_ID
left join @resultsDatabaseSchema.scram_PROVIDER pr
   on o.PROVIDER_ID = pr.oldid
left join @resultsDatabaseSchema.scram_CARE_SITE cs
   on o.CARE_SITE_ID = cs.oldid
left join @resultsDatabaseSchema.scram_VISIT_DETAIL vd
   on o.preceding_visit_detail_id = vd.oldID
left join @resultsDatabaseSchema.scram_VISIT_DETAIL pa
   on o.parent_visit_detail_id = pa.oldID
;

--n3c_control_map
--OUT_FILE: N3C_CONTROL_MAP.csv
--SELECT *
--FROM @resultsDatabaseSchema.N3C_CONTROL_MAP

--DATA_COUNTS TABLE
--OUTPUT_FILE: DATA_COUNTS.csv
SELECT 
   TABLE_NAME
   ,numRows
   ,numRaw
   ,numHash
   ,diffRaw
   ,diffHash
FROM @resultsDatabaseSchema.v_table_stats
;
