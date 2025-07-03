-- these functions are to see if two date ranges intersect at any points or are mutually exclusive
-- if an end time is left off, it is assumed that the window goes to infinity.  if start time left off, negative infinity
-- this function will also work if end is before start if both ranges are in the same direction and are closed (no infinities)

CREATE OR REPLACE FUNCTION @resultsDatabaseSchema.overlaps_bool(
    timestamptz, timestamptz, -- data window start and end
    timestamptz, timestamptz) -- start and end date columns for the table (e.g. OBSERVATION_PERIOD_START_DATE...)
	    RETURNS bool
	    LANGUAGE sql
	    IMMUTABLE
AS $$
    SELECT
        COALESCE($1, TIMESTAMPtz '-infinity') <= COALESCE($4, TIMESTAMPtz 'infinity')
        AND
        COALESCE($3, TIMESTAMPtz '-infinity') <= COALESCE($2, TIMESTAMPtz 'infinity')
$$
;

CREATE OR REPLACE FUNCTION @resultsDatabaseSchema.overlaps_bool(
    date, date, -- data window start and end 
    date, date) -- start and end date columns for the table (e.g. OBSERVATION_PERIOD_START_DATE...)
	    RETURNS bool
	    LANGUAGE sql
	    STABLE -- since we are potentially comparing a date to a timestamptz, re-casting happens, which cannot happen in immutable functions
AS $$
    SELECT
        COALESCE($1, TIMESTAMPtz '-infinity') <= COALESCE($4, TIMESTAMPtz 'infinity')
        AND
        COALESCE($3, TIMESTAMPtz '-infinity') <= COALESCE($2, TIMESTAMPtz 'infinity')
$$
;