CREATE or replace VIEW @resultsDatabaseSchema.v_table_stats As
with uniques as (
	select distinct
	 	ix.person_id,
	    ix.birth_datetime,ix.YEAR_OF_BIRTH,ix.MONTH_OF_BIRTH,ix.DAY_OF_BIRTH,
	    ix.location_id,
	    ix.provider_id,
	    ix.care_site_id
   FROM omop.PERSON ix
        INNER JOIN @resultsDatabaseSchema.CLAD_COHORT cc
            ON ix.person_id = cc.person_id
), innerperson as (
    select 
        COUNT(distinct u.*) numRows
        ,COUNT(DISTINCT x.person_id) numRaw
        ,COUNT(DISTINCT c.pmid) numHash
    FROM omop.PERSON x
        INNER JOIN @resultsDatabaseSchema.CLAD_COHORT c
            ON x.person_id = c.person_id
        INNER JOIN uniques u
            ON x.person_id = u.person_id 
    group by
        u.person_id,
	    u.birth_datetime,u.YEAR_OF_BIRTH,u.MONTH_OF_BIRTH,u.DAY_OF_BIRTH,
	    u.location_id,
	    u.provider_id,
	    u.care_site_id
), person as (
    SELECT
        'PERSON' TABLE_NAME,
        SUM(numRows) numRows,
        SUM(numRaw) numRaw,
        SUM(numHash) numHash
    from innerperson
), observation_period as (
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
   SELECT *, (numRows-numRaw) diffRaw, (numRows-numHash) diffHash from observation_period
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
) SELECT * from all_tables;