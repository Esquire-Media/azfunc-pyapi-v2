-- utils.attom
--
-- A projection of attom.taxassessor_normalized into a flat, renamed shape that
-- the audience builder and the neighbors tooling can query directly.
--
-- This change adds publication_date to the projection. publication_date is the
-- move date used by the campaign proposal reporting pipeline, which reads the
-- movers dataset out of this view instead of a separate table.
--
-- Postgres allows new columns to be appended to a view, so the existing
-- projection and column order are preserved and publication_date is added at
-- the end.
--
-- Run this as the owner of utils.attom. It briefly locks the view, so any
-- query reading it at the same moment will wait rather than fail.

CREATE OR REPLACE VIEW utils.attom AS
WITH extracted AS (
         SELECT n.situs_state_county_fips::bigint AS fips,
            COALESCE(NULLIF(btrim((n.normalized_address).address_alphanumeric::text), ''::text), (n.normalized_address).address::text) AS street_number,
            (n.normalized_address).predirabbrev::text AS street_pre_direction,
            (n.normalized_address).streetname::text AS street_name,
            (n.normalized_address).streettypeabbrev::text AS street_suffix,
            (n.normalized_address).postdirabbrev::text AS street_post_direction,
            NULLIF(btrim((n.normalized_address).internal::text), ''::text) AS unit_text,
            (n.normalized_address).location::text AS city,
            (n.normalized_address).stateabbrev::text AS state,
            NULLIF(btrim((n.normalized_address).zip::text), ''::text) AS zip_text,
            NULLIF(btrim((n.normalized_address).zip4::text), ''::text) AS zip4_text,
            n.latlong,
            n.census_tract,
            n.year_built,
            n.publication_date
           FROM attom.taxassessor_normalized n
        ), base AS (
         SELECT e.fips,
            e.street_number,
            e.street_pre_direction,
            e.street_name,
            e.street_suffix,
            e.street_post_direction,
                CASE
                    WHEN e.unit_text IS NULL THEN NULL::text
                    WHEN strpos(e.unit_text, ' '::text) > 0 THEN "left"(e.unit_text, strpos(e.unit_text, ' '::text) - 1)
                    ELSE e.unit_text
                END AS unit_type,
                CASE
                    WHEN e.unit_text IS NULL THEN NULL::text
                    WHEN strpos(e.unit_text, ' '::text) > 0 THEN NULLIF(btrim(substr(e.unit_text, strpos(e.unit_text, ' '::text) + 1)), ''::text)
                    ELSE NULL::text
                END AS unit_number,
            NULLIF(concat_ws(' '::text, NULLIF(btrim(e.street_number), ''::text), NULLIF(btrim(e.street_pre_direction), ''::text), NULLIF(btrim(e.street_name), ''::text), NULLIF(btrim(e.street_suffix), ''::text), NULLIF(btrim(e.street_post_direction), ''::text), e.unit_text), ''::text) AS formatted_street_address,
            e.city,
            e.state,
                CASE
                    WHEN e.zip_text IS NOT NULL AND length(e.zip_text) >= 5 AND translate("left"(e.zip_text, 5), '0123456789'::text, ''::text) = ''::text THEN "left"(e.zip_text, 5)
                    ELSE NULL::text
                END AS zip_code,
                CASE
                    WHEN e.zip4_text IS NOT NULL AND length(e.zip4_text) = 4 AND translate(e.zip4_text, '0123456789'::text, ''::text) = ''::text THEN e.zip4_text
                    WHEN length(e.zip_text) = 10 AND substr(e.zip_text, 6, 1) = '-'::text AND translate("left"(e.zip_text, 5) || "right"(e.zip_text, 4), '0123456789'::text, ''::text) = ''::text THEN "right"(e.zip_text, 4)
                    WHEN length(e.zip_text) = 9 AND translate(e.zip_text, '0123456789'::text, ''::text) = ''::text THEN "right"(e.zip_text, 4)
                    ELSE NULL::text
                END AS plus4_code,
            e.latlong,
            e.census_tract,
            e.year_built,
            e.publication_date
           FROM extracted e
        )
 SELECT fips,
    street_number,
    street_pre_direction,
    street_name,
    street_suffix,
    street_post_direction,
    unit_type,
    unit_number,
    formatted_street_address,
    city,
    state,
    zip_code::character varying(5) AS "zipCode",
    plus4_code::character varying(4) AS "plus4Code",
        CASE
            WHEN latlong IS NULL THEN NULL::double precision
            ELSE st_y(latlong::geometry)
        END AS latitude,
        CASE
            WHEN latlong IS NULL THEN NULL::double precision
            ELSE st_x(latlong::geometry)
        END AS longitude,
    census_tract::double precision AS census_tract,
    year_built,
    "left"(NULLIF(concat_ws(', '::text, formatted_street_address, city, NULLIF(concat_ws(' '::text, state,
        CASE
            WHEN zip_code IS NULL THEN NULL::text
            WHEN plus4_code IS NULL THEN zip_code
            ELSE (zip_code || '-'::text) || plus4_code
        END), ''::text)), ''::text), 255)::character varying(255) AS address,
    publication_date
   FROM base b;
