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

create or replace function sathi_aggregate_for_coded_concept(conceptName TEXT, subjectName Text, blockNames text,
                                                             startDate date, endDate date)
    returns table
            (
                concept_name text,
                मोजा         bigint,
                "टक्केवारी"  numeric,
                "ओळ यादी"    text
            )
as
$body$
with data as (select answer_concept_name                                         indicator,
                     count(*) filter ( where multi_select_coded(observations -> concept_uuid(conceptName)) like
                                             '%' || answer_concept_name || '%' ) count,
                     (select count(*)
                      from sathi_registration_view
                      where subject_name = subjectName)                          total
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
