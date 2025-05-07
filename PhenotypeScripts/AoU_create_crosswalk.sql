--oldID = Emory enterprise omop _id column
--newID = CLAD _id column
    --CONDITION_OCCURRENCE_ID_EMORY INT NOT NULL,
    --CONDITION_OCCURRENCE_ID_CLAD bigint not null

CREATE TABLE IF NOT EXISTS @resultsDatabaseSchema.scram_condition_occurrence (
    oldID int not null, 
    newID bigint not null
)
DISTKEY(oldID);
TRUNCATE @resultsDatabaseSchema.scram_condition_occurrence;
INSERT INTO @resultsDatabaseSchema.scram_condition_occurrence
    SELECT
        CONDITION_OCCURRENCE_ID,
        @hash_fn(CONDITION_OCCURRENCE_ID,'CONDITION_OCCURRENCE_ID')
    FROM @cdmDatabaseSchema.CONDITION_OCCURRENCE co
        JOIN @resultsDatabaseSchema.@cohortTable n
            ON CO.person_id = n.person_id
            AND @overlap_fn(
                TO_DATE('@startDate','YYYY-MM-DD'),TO_DATE('@endDate','YYYY-MM-DD'),CONDITION_START_DATE,CONDITION_END_DATE
            )
;

CREATE TABLE IF NOT EXISTS @resultsDatabaseSchema.scram_drug_exposure (
    oldID INT NOT NULL,
    newID bigint not null
)
DISTKEY(oldID);
TRUNCATE @resultsDatabaseSchema.scram_drug_exposure;
INSERT INTO @resultsDatabaseSchema.scram_drug_exposure
    SELECT
        DRUG_EXPOSURE_ID,
        @hash_fn(DRUG_EXPOSURE_ID,'DRUG_EXPOSURE_ID')
    FROM @cdmDatabaseSchema.DRUG_EXPOSURE de
        JOIN @resultsDatabaseSchema.@cohortTable n
            ON de.PERSON_ID = n.PERSON_ID
            AND @overlap_fn(
                TO_DATE('@startDate','YYYY-MM-DD'),TO_DATE('@endDate','YYYY-MM-DD'),DRUG_EXPOSURE_START_DATE,DRUG_EXPOSURE_END_DATE
            )
    ;

CREATE TABLE IF NOT EXISTS @resultsDatabaseSchema.scram_location (
    oldID INT NOT NULL,
    newID bigint not null
)
DISTKEY(oldID);
TRUNCATE @resultsDatabaseSchema.scram_location;
INSERT INTO @resultsDatabaseSchema.scram_location
    SELECT DISTINCT
        l.LOCATION_ID,
        @hash_fn(l.LOCATION_ID,'LOCATION_ID')
    FROM @cdmDatabaseSchema.LOCATION l
        JOIN @cdmDatabaseSchema.PERSON p
            ON l.location_id = p.location_id
        JOIN @resultsDatabaseSchema.@cohortTable n
            ON p.person_id = n.person_id
;

CREATE TABLE IF NOT EXISTS @resultsDatabaseSchema.scram_provider (
    oldID INT NOT NULL,
    newID bigint not null
)
DISTKEY(oldID);
TRUNCATE @resultsDatabaseSchema.scram_provider;
INSERT INTO @resultsDatabaseSchema.scram_provider
    SELECT
        pr.PROVIDER_ID,
        @hash_fn(pr.PROVIDER_ID,'PROVIDER_ID')
    FROM @cdmDatabaseSchema.PROVIDER pr
        JOIN (
            SELECT DISTINCT PROVIDER_ID
            FROM @cdmDatabaseSchema.VISIT_OCCURRENCE vo
            JOIN @resultsDatabaseSchema.@cohortTable n
                ON vo.PERSON_ID = n.PERSON_ID 
                AND @overlap_fn(
                    TO_DATE('@startDate','YYYY-MM-DD'),TO_DATE('@endDate','YYYY-MM-DD'),VISIT_START_DATE,VISIT_END_DATE
                )
            UNION
            SELECT DISTINCT PROVIDER_ID
            FROM @cdmDatabaseSchema.DRUG_EXPOSURE de
            JOIN @resultsDatabaseSchema.@cohortTable n
                ON de.PERSON_ID = n.PERSON_ID
                AND @overlap_fn(
                    TO_DATE('@startDate','YYYY-MM-DD'),TO_DATE('@endDate','YYYY-MM-DD'),DRUG_EXPOSURE_START_DATE,DRUG_EXPOSURE_END_DATE
                )
            UNION
            SELECT DISTINCT PROVIDER_ID
            FROM @cdmDatabaseSchema.MEASUREMENT m
            JOIN @resultsDatabaseSchema.@cohortTable n
                ON m.PERSON_ID = n.PERSON_ID 
                AND @overlap_fn(
                    TO_DATE('@startDate','YYYY-MM-DD'),TO_DATE('@endDate','YYYY-MM-DD'),MEASUREMENT_DATE,NULL
                )
            UNION
            SELECT DISTINCT PROVIDER_ID
            FROM @cdmDatabaseSchema.PROCEDURE_OCCURRENCE po
            JOIN @resultsDatabaseSchema.@cohortTable n
                ON po.PERSON_ID = n.PERSON_ID 
                AND @overlap_fn(
                    TO_DATE('@startDate','YYYY-MM-DD'),TO_DATE('@endDate','YYYY-MM-DD'),PROCEDURE_DATE,PROCEDURE_END_DATE
                ) --non-standard column names
            UNION
            SELECT DISTINCT PROVIDER_ID
            FROM @cdmDatabaseSchema.OBSERVATION o
            JOIN @resultsDatabaseSchema.@cohortTable n
                ON o.PERSON_ID = n.PERSON_ID 
                AND @overlap_fn(
                    TO_DATE('@startDate','YYYY-MM-DD'),TO_DATE('@endDate','YYYY-MM-DD'),OBSERVATION_DATE,NULL
                )
        ) a
        ON pr.PROVIDER_ID = a.PROVIDER_ID
;

CREATE TABLE IF NOT EXISTS @resultsDatabaseSchema.scram_care_site (
    oldID INT NOT NULL,
    newID bigint not null
)
DISTKEY(oldID);
TRUNCATE @resultsDatabaseSchema.scram_care_site;
INSERT INTO @resultsDatabaseSchema.scram_care_site
    SELECT
        cs.CARE_SITE_ID,
        @hash_fn(cs.CARE_SITE_ID,'CARE_SITE_ID')
    FROM @cdmDatabaseSchema.CARE_SITE cs
        JOIN (
            SELECT DISTINCT CARE_SITE_ID
                FROM @cdmDatabaseSchema.VISIT_OCCURRENCE vo
                    JOIN @resultsDatabaseSchema.@cohortTable n
                    ON vo.person_id = n.person_id
            ) a
        ON cs.CARE_SITE_ID = a.CARE_SITE_ID
;

CREATE TABLE IF NOT EXISTS @resultsDatabaseSchema.scram_device_exposure (
    oldID INT NOT NULL,
    newID bigint not null
)
DISTKEY(oldID);
TRUNCATE @resultsDatabaseSchema.scram_device_exposure;
INSERT INTO @resultsDatabaseSchema.scram_device_exposure
    SELECT
        DEVICE_EXPOSURE_ID,
        @hash_fn(DEVICE_EXPOSURE_ID,'DEVICE_EXPOSURE_ID')
    FROM @cdmDatabaseSchema.DEVICE_EXPOSURE de
        JOIN @resultsDatabaseSchema.@cohortTable n
            ON de.PERSON_ID = n.PERSON_ID
            AND @overlap_fn(
                TO_DATE('@startDate','YYYY-MM-DD'),TO_DATE('@endDate','YYYY-MM-DD'),DEVICE_EXPOSURE_START_DATE,DEVICE_EXPOSURE_END_DATE
            )
;

-- unique_device_id does not cross-link with any OMOP table

CREATE TABLE IF NOT EXISTS @resultsDatabaseSchema.scram_observation_period (
    oldID INT NOT NULL,
    newID bigint not null
)
DISTKEY(oldID);
TRUNCATE @resultsDatabaseSchema.scram_observation_period;
INSERT INTO @resultsDatabaseSchema.scram_observation_period 
    SELECT
        OBSERVATION_PERIOD_ID,
        @hash_fn(OBSERVATION_PERIOD_ID,'OBSERVATION_PERIOD_ID')
    FROM @cdmDatabaseSchema.OBSERVATION_PERIOD p
        JOIN @resultsDatabaseSchema.@cohortTable n
            ON p.PERSON_ID = n.PERSON_ID
            AND @overlap_fn(
                TO_DATE('@startDate','YYYY-MM-DD'),TO_DATE('@endDate','YYYY-MM-DD'),OBSERVATION_PERIOD_START_DATE,OBSERVATION_PERIOD_END_DATE
            )
;

CREATE TABLE IF NOT EXISTS @resultsDatabaseSchema.scram_procedure_occurrence (
    oldID INT NOT NULL,
    newID bigint not null
)
DISTKEY(oldID);
TRUNCATE @resultsDatabaseSchema.scram_procedure_occurrence;
INSERT INTO @resultsDatabaseSchema.scram_procedure_occurrence
    SELECT
        PROCEDURE_OCCURRENCE_ID,
        @hash_fn(PROCEDURE_OCCURRENCE_ID,'PROCEDURE_OCCURRENCE_ID')
    FROM @cdmDatabaseSchema.PROCEDURE_OCCURRENCE po
        JOIN @resultsDatabaseSchema.@cohortTable n
            ON PO.PERSON_ID = N.PERSON_ID
            AND @overlap_fn(
                TO_DATE('@startDate','YYYY-MM-DD'),TO_DATE('@endDate','YYYY-MM-DD'),PROCEDURE_DATE,PROCEDURE_END_DATE
            ) -- non-standard column names
;

CREATE TABLE IF NOT EXISTS @resultsDatabaseSchema.scram_visit_occurrence (
    oldID INT NOT NULL,
    newID bigint not null
)
DISTKEY(oldID);
TRUNCATE @resultsDatabaseSchema.scram_visit_occurrence;
INSERT INTO @resultsDatabaseSchema.scram_visit_occurrence
    SELECT
        VISIT_OCCURRENCE_ID,
        @hash_fn(VISIT_OCCURRENCE_ID,'VISIT_OCCURRENCE_ID')
    FROM @cdmDatabaseSchema.VISIT_OCCURRENCE v
        JOIN @resultsDatabaseSchema.@cohortTable n
            ON v.PERSON_ID = n.PERSON_ID
            AND @overlap_fn(
                TO_DATE('@startDate','YYYY-MM-DD'),TO_DATE('@endDate','YYYY-MM-DD'),VISIT_START_DATE,VISIT_END_DATE
            )
;

CREATE TABLE IF NOT EXISTS @resultsDatabaseSchema.scram_visit_detail (
    oldID INT NOT NULL,
    newID bigint not null
)
DISTKEY(oldID);
TRUNCATE @resultsDatabaseSchema.scram_visit_detail;
INSERT INTO @resultsDatabaseSchema.scram_visit_detail
    SELECT
        VISIT_DETAIL_ID,
        @hash_fn(VISIT_DETAIL_ID,'VISIT_DETAIL_ID')
    FROM @cdmDatabaseSchema.VISIT_DETAIL vd
        JOIN @resultsDatabaseSchema.scram_visit_occurrence vo --JOIN @cdmDatabaseSchema.VISIT_OCCURRENCE vo
            ON vd.visit_occurrence_id = vo.oldID
;

CREATE TABLE IF NOT EXISTS @resultsDatabaseSchema.scram_measurement (
    oldID INT NOT NULL,
    newID bigint not null
)
DISTKEY(oldID);
TRUNCATE @resultsDatabaseSchema.scram_measurement;
INSERT INTO @resultsDatabaseSchema.scram_measurement
    SELECT
        MEASUREMENT_ID,
        @hash_fn(MEASUREMENT_ID,'MEASUREMENT_ID')
    FROM @cdmDatabaseSchema.MEASUREMENT m
        JOIN @resultsDatabaseSchema.@cohortTable n
            ON M.PERSON_ID = N.PERSON_ID
            AND @overlap_fn(
                TO_DATE('@startDate','YYYY-MM-DD'),TO_DATE('@endDate','YYYY-MM-DD'),MEASUREMENT_DATE,NULL
            )
;

CREATE TABLE IF NOT EXISTS @resultsDatabaseSchema.scram_observation (
    oldID INT NOT NULL,
    newID bigint not null
)
DISTKEY(oldID);
TRUNCATE @resultsDatabaseSchema.scram_observation;
INSERT INTO @resultsDatabaseSchema.scram_observation
    SELECT
        OBSERVATION_ID,
        @hash_fn(OBSERVATION_ID,'OBSERVATION_ID')
    FROM @cdmDatabaseSchema.OBSERVATION o
        JOIN @resultsDatabaseSchema.@cohortTable n
            ON O.PERSON_ID = N.PERSON_ID
            AND @overlap_fn(
                TO_DATE('@startDate','YYYY-MM-DD'),TO_DATE('@endDate','YYYY-MM-DD'),OBSERVATION_DATE,NULL
            )
;

CREATE TABLE IF NOT EXISTS @resultsDatabaseSchema.scram_drug_era (
    oldID INT NOT NULL,
    newID bigint not null
)
DISTKEY(oldID);
TRUNCATE @resultsDatabaseSchema.scram_drug_era;
INSERT INTO @resultsDatabaseSchema.scram_drug_era
    SELECT
        DRUG_ERA_ID,
        @hash_fn(DRUG_ERA_ID,'DRUG_ERA_ID')
    FROM @cdmDatabaseSchema.DRUG_ERA dre
        JOIN @resultsDatabaseSchema.@cohortTable n
            ON DRE.PERSON_ID = N.PERSON_ID
            AND @overlap_fn(
                TO_DATE('@startDate','YYYY-MM-DD'),TO_DATE('@endDate','YYYY-MM-DD'),DRUG_ERA_START_DATE,DRUG_ERA_END_DATE
            )
;

CREATE TABLE IF NOT EXISTS @resultsDatabaseSchema.scram_condition_era (
    oldID INT NOT NULL,
    newID bigint not null
)
DISTKEY(oldID);
TRUNCATE @resultsDatabaseSchema.scram_condition_era;
INSERT INTO @resultsDatabaseSchema.scram_condition_era
    SELECT
        CONDITION_ERA_ID,
        @hash_fn(CONDITION_ERA_ID,'CONDITION_ERA_ID')
    FROM @cdmDatabaseSchema.CONDITION_ERA ce 
        JOIN @resultsDatabaseSchema.@cohortTable n 
            ON CE.PERSON_ID = N.PERSON_ID
            AND @overlap_fn(
                TO_DATE('@startDate','YYYY-MM-DD'),TO_DATE('@endDate','YYYY-MM-DD'),CONDITION_ERA_START_DATE,CONDITION_ERA_END_DATE
            )
;

CREATE VIEW v_table_stats As
with person as (
   SELECT
      'PERSON' TABLE_NAME
      ,COUNT(*) numRows
      ,COUNT(DISTINCT x.person_id) numRaw
      ,COUNT(DISTINCT n3c.pmid) numHash
   FROM @cdmDatabaseSchema.PERSON x
      INNER JOIN @resultsDatabaseSchema.CLAD_COHORT n3c
      ON x.person_id = n3c.person_id
), observation as (
   SELECT
      'OBSERVATION_PERIOD' TABLE_NAME
      ,COUNT(*) numRows
      ,count(DISTINCT oldid) numRaw
      ,COUNT(DISTINCT newid) numHash
   FROM @resultsDatabaseSchema.scram_OBSERVATION_PERIOD
), visit_occurrence as (
   SELECT
      'VISIT_OCCURRENCE' TABLE_NAME
      ,COUNT(*) numRows
      ,count(DISTINCT oldid) numRaw
      ,COUNT(DISTINCT newid) numHash
   FROM @resultsDatabaseSchema.scram_VISIT_OCCURRENCE
), condition_occurrence as (
   SELECT
      'CONDITION_OCCURRENCE' TABLE_NAME
      ,COUNT(*) numRows
      ,count(DISTINCT oldid) numRaw
      ,COUNT(DISTINCT newid) numHash
   FROM @resultsDatabaseSchema.scram_CONDITION_OCCURRENCE
), DRUG_EXPOSURE as (
   SELECT
      'DRUG_EXPOSURE' TABLE_NAME
      ,COUNT(*) numRows
      ,count(DISTINCT oldid) numRaw
      ,COUNT(DISTINCT newid) numHash
   FROM @resultsDatabaseSchema.scram_DRUG_EXPOSURE
), DEVICE_EXPOSURE as (
   SELECT
      'DEVICE_EXPOSURE' TABLE_NAME
      ,COUNT(*) numRows
      ,count(DISTINCT oldid) numRaw
      ,COUNT(DISTINCT newid) numHash
   FROM @resultsDatabaseSchema.scram_DEVICE_EXPOSURE
), PROCEDURE_OCCURRENCE as (
   SELECT
      'PROCEDURE_OCCURRENCE' TABLE_NAME
      ,COUNT(*) numRows
      ,count(DISTINCT oldid) numRaw
      ,COUNT(DISTINCT newid) numHash
   FROM @resultsDatabaseSchema.scram_PROCEDURE_OCCURRENCE
), MEASUREMENT as (
   SELECT
      'MEASUREMENT' TABLE_NAME
      ,COUNT(*) numRows
      ,count(DISTINCT oldid) numRaw
      ,COUNT(DISTINCT newid) numHash
   FROM @resultsDatabaseSchema.scram_MEASUREMENT
), OBSERVATION as (
   SELECT
      'OBSERVATION' TABLE_NAME
      ,COUNT(*) numRows
      ,count(DISTINCT oldid) numRaw
      ,COUNT(DISTINCT newid) numHash
   FROM @resultsDatabaseSchema.scram_OBSERVATION
), "LOCATION" as (
   SELECT
      'LOCATION' TABLE_NAME
      ,COUNT(*) numRows
      ,count(DISTINCT oldid) numRaw
      ,COUNT(DISTINCT newid) numHash
   FROM @resultsDatabaseSchema.scram_LOCATION
), CARE_SITE as (
   SELECT
      'CARE_SITE' TABLE_NAME
      ,COUNT(*) numRows
      ,count(DISTINCT oldid) numRaw
      ,COUNT(DISTINCT newid) numHash
   FROM @resultsDatabaseSchema.scram_CARE_SITE
), "PROVIDER" as (      
   SELECT
      'PROVIDER' TABLE_NAME
      ,COUNT(*) numRows
      ,count(DISTINCT oldid) numRaw
      ,COUNT(DISTINCT newid) numHash
   FROM @resultsDatabaseSchema.scram_PROVIDER
), DRUG_ERA as (
   SELECT
      'DRUG_ERA' TABLE_NAME
      ,COUNT(*) numRows
      ,count(DISTINCT oldid) numRaw
      ,COUNT(DISTINCT newid) numHash
   FROM @resultsDatabaseSchema.scram_DRUG_ERA
), CONDITION_ERA as (
   SELECT
      'CONDITION_ERA' TABLE_NAME
      ,COUNT(*) numRows
      ,count(DISTINCT oldid) numRaw
      ,COUNT(DISTINCT newid) numHash
   FROM @resultsDatabaseSchema.scram_CONDITION_ERA
), VISIT_DETAIL as (
   SELECT
      'VISIT_DETAIL' TABLE_NAME
      ,COUNT(*) numRows
      ,count(DISTINCT oldid) numRaw
      ,COUNT(DISTINCT newid) numHash
   FROM @resultsDatabaseSchema.scram_VISIT_DETAIL
), all_tables as (
   SELECT *, (numRows-numRaw) diffRaw, (numRows-numHash) diffHash from person 
      UNION ALL
   SELECT *, (numRows-numRaw) diffRaw, (numRows-numHash) diffHash from observation
      UNION ALL
   SELECT *, (numRows-numRaw) diffRaw, (numRows-numHash) diffHash from visit_occurrence
      UNION ALL
   SELECT *, (numRows-numRaw) diffRaw, (numRows-numHash) diffHash from condition_occurrence
      UNION ALL
   SELECT *, (numRows-numRaw) diffRaw, (numRows-numHash) diffHash from DRUG_EXPOSURE 
      UNION ALL
   SELECT *, (numRows-numRaw) diffRaw, (numRows-numHash) diffHash from DEVICE_EXPOSURE 
      UNION ALL
   SELECT *, (numRows-numRaw) diffRaw, (numRows-numHash) diffHash from PROCEDURE_OCCURRENCE 
      UNION ALL
   SELECT *, (numRows-numRaw) diffRaw, (numRows-numHash) diffHash from MEASUREMENT 
      UNION ALL
   SELECT *, (numRows-numRaw) diffRaw, (numRows-numHash) diffHash from OBSERVATION 
      UNION ALL
   SELECT *, (numRows-numRaw) diffRaw, (numRows-numHash) diffHash from "LOCATION" 
      UNION ALL
   SELECT *, (numRows-numRaw) diffRaw, (numRows-numHash) diffHash from CARE_SITE 
      UNION ALL
   SELECT *, (numRows-numRaw) diffRaw, (numRows-numHash) diffHash from "PROVIDER" 
      UNION ALL
   SELECT *, (numRows-numRaw) diffRaw, (numRows-numHash) diffHash from DRUG_ERA 
      UNION ALL
   SELECT *, (numRows-numRaw) diffRaw, (numRows-numHash) diffHash from CONDITION_ERA 
      UNION ALL
   SELECT *, (numRows-numRaw) diffRaw, (numRows-numHash) diffHash from VISIT_DETAIL
) SELECT * from all_tables