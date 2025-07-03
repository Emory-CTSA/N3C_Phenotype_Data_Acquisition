CREATE OR REPLACE FUNCTION @resultsDatabaseSchema.my_hash(int4, varchar)
	RETURNS int8
	LANGUAGE sql
	STABLE
AS $$
	select FNV_HASH($1, FNV_HASH($2, FNV_HASH('YourCatchyPhraseHere')))
$$
;

CREATE OR REPLACE FUNCTION @resultsDatabaseSchema.my_hash(int8, varchar)
	RETURNS int8
	LANGUAGE sql
	STABLE
AS $$
	select FNV_HASH($1, FNV_HASH($2, FNV_HASH('YourCatchyPhraseHere')))
$$
;
