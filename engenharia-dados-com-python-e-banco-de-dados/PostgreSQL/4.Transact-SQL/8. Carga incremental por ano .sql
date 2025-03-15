-- Criando coluna de ano 
select
extract(year from "data_da_ocorrencia"::DATE) as ano,
* 
from public.anac_sqlalchemy;


-- Trazendo anos distintos
select distinct
extract(year from "data_da_ocorrencia"::DATE) as ano
from public.anac_sqlalchemy
order by 1 desc;


-- Filtrando ano atual 
select
extract(year from "data_da_ocorrencia"::DATE) as ano,
extract(year from current_date) as ano_atual,
* 
from public.anac_sqlalchemy
where extract(year from "data_da_ocorrencia"::DATE) = extract(year from current_date)


-- Criando delete ano atual
delete from public.anac_sqlalchemy
where extract(year from "data_da_ocorrencia"::DATE) = extract(year from current_date)



