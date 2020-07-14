set role sathi;

drop view if exists sathi_registration_view;
create view sathi_registration_view as (
    SELECT individual.id                                   as id,
           individual.uuid                                 as uuid,
           individual.first_name                           as first_name,
           individual.last_name                            as last_name,
           g.name                                          as gender,
           to_char(individual.date_of_birth, 'dd-mm-yyyy') as dob,
           individual.date_of_birth_verified               as dob_verified,
           individual.registration_date                    as registration_date,
           translated_value('mr_IN', block.title::text)    as block,
           translated_value('mr_IN', district.title::text) as district,
           individual.is_voided                            as is_voided,
           st.name                                         as subject_name,
           observations
    FROM individual individual
             LEFT JOIN gender g ON g.id = individual.gender_id
             LEFT JOIN address_level block ON individual.address_id = block.id
             LEFT JOIN address_level district ON district.id = block.parent_id
             left join subject_type st on individual.subject_type_id = st.id
);

drop view if exists sathi_address_view;
create view sathi_address_view as (
    select block.id                                  block_id,
           district.id                               district_id,
           translated_value('mr_IN', block.title)    block_name,
           translated_value('mr_IN', district.title) district_name
    from address_level block
             left join address_level district ON district.id = block.parent_id
    where not block.is_voided
      and not district.is_voided
);

create or replace function sathi_reg_agg_for_coded(conceptName TEXT, subjectName Text, blockNames text,
                                                   startDate date, endDate date)
    returns table
            (
                "उत्तर"     text,
                मोजा        bigint,
                "टक्केवारी" numeric,
                "ओळ यादी"   text
            )
as
$body$
with data as (select answer_concept_name                                         indicator,
                     count(*) filter ( where case
                                                 when jsonb_typeof(observations -> concept_uuid) =
                                                      'array'
                                                     then (observations -> concept_uuid) @>  to_jsonb(answer_concept_uuid)
                                                 else observations ->> concept_uuid = answer_concept_uuid end) count,
                     count(distinct id)                                          total
              from concept_concept_answer
                       left join sathi_registration_view on true
              where concept_name = conceptName
                and subject_name = subjectName
                and block = ANY (STRING_TO_ARRAY(blockNames, ','))
                and registration_date between startDate and endDate
              group by 1, answer_order
              order by answer_order)
select translated_value('mr_IN', indicator),
       count,
       ((count * 100.0) / total),
       'https://reporting.avniproject.org/question/830?subject_name=' ||
       subjectName || '&concept_name=' || conceptName ||
       '&concept_answer=' || indicator || '&block=' || blockNames ||
       '&start_date=' || startDate::date || '&end_date=' || endDate::date
from data
$body$
    language sql;

--sample ranges jsonb example
--'[{"name": "<5", "max": 4.99, "row": 1 }, { "name": "5 to 10", "min": 5, "max": 10, "row": 2 }, { "name": ">10", "min": 10.001, "row": 3 } ]'::jsonb
create or replace function sathi_reg_agg_for_Integer(conceptName TEXT, subjectName Text, ranges jsonb, blockNames text,
                                                     startDate date, endDate date)
    returns table
            (
                "उत्तर"     text,
                मोजा        bigint,
                "टक्केवारी" numeric,
                "ओळ यादी"   text
            )
as
$body$
with data as (select my_range ->> 'name'                                                                                                                                       indicator,
                     my_range ->> 'min'                                                                                                                                        min_range,
                     my_range ->> 'max'                                                                                                                                        max_range,
                     my_range ->> 'row'                                                                                                                                        row_order,
                     count(*)
                     filter ( where case
                                        when my_range ->> 'min' isnull then
                                                (observations ->> concept_uuid(conceptName))::numeric <=
                                                (my_range ->> 'max')::numeric
                                        when my_range ->> 'max' isnull then
                                                (observations ->> concept_uuid(conceptName))::numeric >=
                                                (my_range ->> 'min')::numeric
                                        else
                                            (observations ->> concept_uuid(conceptName))::numeric between (my_range ->> 'min')::numeric and (my_range ->> 'max')::numeric end) count,
                     count(distinct id)                                                                                                                                        total
              from jsonb_array_elements(ranges) my_range
                       cross join sathi_registration_view
              where subject_name = subjectName
                and block = ANY (STRING_TO_ARRAY(blockNames, ','))
                and registration_date between startDate and endDate
              group by 1, 2, 3, 4
)
select indicator,
       count,
       ((count * 100.0) / total),
       'https://reporting.avniproject.org/question/879?subject_name=' ||
       subjectName || '&concept_name=' || conceptName ||
       '&min_range=' || coalesce(min_range, 'null') || '&max_range=' || coalesce(max_range, 'null') || '&block=' ||
       blockNames || '&start_date=' || startDate::date || '&end_date=' || endDate::date
from data
order by row_order
$body$
    language sql;
