-- Databricks notebook source
create or replace view datalake.diamond.vw_municipios as 
select 
m.uf as cod_uf,
p.uf as sigla_uf, 
TRANSLATE(m.nome_uf , 'áàãâéèêíìóòõôúùûçÁÀÃÂÉÈÊÍÌÓÒÕÔÚÙÛÇ', 'aaaaeeeiioooouucAAAAEEEIIOOOUUC') as estado,
m.codigo_municipio_completo,
p.cod_municipio,
p.nome_municipio,
s.cod_siafi,
p.` populacao ` as populacao
 from datalake.gold.tb_ibge_base_populacao p 
left join (select distinct nome_uf, uf,codigo_municipio_completo, nome_municipio from datalake.gold.tb_ibge_municipios) m on m.codigo_municipio_completo = p.cod_uf||p.cod_municipio
left join datalake.gold.tb_municipios_siafi_ibge s on s.cod_mun_ibge=p.cod_uf||p.cod_municipio

-- COMMAND ----------

select sum(populacao), estado from datalake.diamond.vw_municipios 
group by estado

-- COMMAND ----------

create or replace view datalake.diamond.vw_cadastro_unico as 
select
left(r.anomes_s, 4) as ano,
right(r.anomes_s, 2) as mes,
sum(r.cadun_qtd_familias_atualizadas_i) as cadun_qtd_familias_atualizadas,
sum(r.cadun_qtd_familias_atualizadas_renda_zero_i) as qtd_familias_renda_zero,
sum(r.cadun_qtd_familias_atualizadas_pobreza_pbf_i) as qtd_familias_pobreza,
sum(r.cadun_qtd_familias_atualizadas_baixa_renda_i) as qtd_familias_baixa_renda,
sum(r.cadun_qtd_familias_atualizadas_rfpc_ate_meio_sm_i) as qtd_familias_ate_meio_sm,
sum(r.cadun_qtd_familias_atualizadas_rfpc_acima_meio_sm_i) as qtd_familias_acima_meio_sm
from datalake.gold.tb_cad_unico_faixa_renda r
group by left(r.anomes_s, 4) ,right(r.anomes_s, 2)
order by 1, 2 

-- COMMAND ----------

select * from datalake.diamond.vw_cadastro_unico 

-- COMMAND ----------

create or replace table datalake.diamond.tb_beneficios_br_ano as 

select left(ab.mes_referencia, 4) as ano,
count(distinct nis_favorecido) qtde_beneficios,
'Auxilio Brasil' as nome_beneficio
from datalake.gold.tb_portal_transparencia_auxilio_brasil ab
group by left(ab.mes_referencia, 4)


union all

select left(mes_disponibilizacao, 4) as ano,
count(distinct nis_beneficiario ) qtde_beneficios,
'Auxilio Emergencial' as nome_beneficio
from datalake.gold.tb_portal_transparencia_auxilio_emergencial ae
group by left(mes_disponibilizacao, 4)

union all



select left(mes_referencia, 4) as ano,
count(distinct nis_favorecido) qtde_beneficios,
'Novo Bolsa Familia' as nome_beneficio
from datalake.gold.tb_portal_transparencia_novo_bolsa_familia nb
group by left(mes_referencia, 4)

union all

select left(mes_referencia, 4) as ano,
count(distinct nis_favorecido) qtde_beneficios,
'Bolsa Familia' as nome_beneficio
from datalake.gold.tb_portal_transparencia_bolsa_familia_pagamentos bp
group by left(mes_referencia, 4)

-- COMMAND ----------

select count(*) from datalake.diamond.tb_beneficios_ano

-- COMMAND ----------

create or replace view datalake.diamond.vw_beneficios_brasil as 
select  
m.uf as cod_uf,
ab.uf as sigla_uf, 
m.nome_uf as uf,
m.codigo_municipio_completo,
m.municipio as cod_municipio,
m.nome_municipio,
s.cod_siafi,
ab.mes_referencia,
ab.mes_competencia,
ab.cpf_favorecido as cpf_beneficiario,
ab.nis_favorecido as nis_beneficiario,
ab.nome_favorecido as nome_beneficiario,
null parcela,
cast(replace(ab.valor_parcela, ',','.') as double) valor_beneficio,
null as data_saque,
'Auxilio Brasil' as nome_beneficio
from datalake.gold.tb_portal_transparencia_auxilio_brasil ab
left join datalake.gold.tb_municipios_siafi_ibge s on s.cod_siafi=int(ab.CODIGO_MUNICIPIO_SIAFI)
left join (select distinct nome_uf, uf,codigo_municipio_completo,municipio, nome_municipio from datalake.gold.tb_ibge_municipios) m on m.codigo_municipio_completo = s.cod_mun_ibge

union all

select  
m.uf as cod_uf,
ae.uf as sigla_uf, 
m.nome_uf as uf,
m.codigo_municipio_completo,
m.municipio as cod_municipio,
m.nome_municipio,
s.cod_siafi,
ae.mes_disponibilizacao as mes_referencia,
null as mes_competencia,
ae.cpf_beneficiario cpf_beneficiario,
ae.nis_beneficiario nis_beneficiario,
ae.nome_beneficiario nome_beneficiario,
ae.parcela,
cast(replace(ae.valor_beneficio, ',','.') as double) valor_beneficio,
null as data_saque,
'Auxilio Emergencial' as nome_beneficio
from datalake.gold.tb_portal_transparencia_auxilio_emergencial ae
left join (select distinct nome_uf, uf,codigo_municipio_completo,municipio, nome_municipio from datalake.gold.tb_ibge_municipios) m on m.codigo_municipio_completo=ae.CODIGO_MUNICIPIO_IBGE
left join datalake.gold.tb_municipios_siafi_ibge s on m.codigo_municipio_completo = s.cod_mun_ibge

union all

select  
m.uf as cod_uf,
nb.uf as sigla_uf, 
m.nome_uf as uf,
m.codigo_municipio_completo,
m.municipio as cod_municipio,
m.nome_municipio,
s.cod_siafi,
nb.mes_referencia,
nb.mes_competencia,
nb.cpf_favorecido cpf_beneficiario,
nb.nis_favorecido nis_beneficiario,
nb.nome_favorecido nome_beneficiario,
null as parcela,
cast(replace(nb.valor_parcela, ',','.') as double) valor_beneficio,
null as data_saque,
'Novo Bolsa Familia' as nome_beneficio
from datalake.gold.tb_portal_transparencia_novo_bolsa_familia nb
left join datalake.gold.tb_municipios_siafi_ibge s on s.cod_siafi=int(nb.CODIGO_MUNICIPIO_SIAFI)
left join (select distinct nome_uf, uf,codigo_municipio_completo,municipio, nome_municipio from datalake.gold.tb_ibge_municipios) m on m.codigo_municipio_completo = s.cod_mun_ibge

union all 

select  
m.uf as cod_uf,
bp.uf as sigla_uf, 
m.nome_uf as uf,
m.codigo_municipio_completo,
m.municipio as cod_municipio,
m.nome_municipio,
s.cod_siafi,
bp.mes_referencia,
bp.mes_competencia,
bp.cpf_favorecido cpf_beneficiario,
bp.nis_favorecido nis_beneficiario,
bp.nome_favorecido nome_beneficiario,
null as parcela,
cast(replace(bp.valor_parcela, ',','.') as double) valor_beneficio,
null as data_saque,
'Bolsa Familia' as nome_beneficio
from datalake.gold.tb_portal_transparencia_bolsa_familia_pagamentos bp
left join datalake.gold.tb_municipios_siafi_ibge s on s.cod_siafi=int(bp.CODIGO_MUNICIPIO_SIAFI)
left join (select distinct nome_uf, uf,codigo_municipio_completo,municipio, nome_municipio from datalake.gold.tb_ibge_municipios) m on m.codigo_municipio_completo = s.cod_mun_ibge


-- COMMAND ----------

select count(*) from datalake.gold.tb_portal_transparencia_bolsa_familia_pagamentos
-- 1.466.060.695
select count(*) from datalake.gold.tb_portal_transparencia_novo_bolsa_familia
-- 183.064.060
select count(*) from datalake.gold.tb_portal_transparencia_auxilio_emergencial
-- 781.655.982
select count(*) from datalake.gold.tb_portal_transparencia_auxilio_brasil
-- 293.956.157

-- COMMAND ----------

create or replace table datalake.diamond.tb_beneficios_nv_bolsa_fam_ano as 
select  
TRANSLATE(m.nome_uf , 'áàãâéèêíìóòõôúùûçÁÀÃÂÉÈÊÍÌÓÒÕÔÚÙÛÇ', 'aaaaeeeiioooouucAAAAEEEIIOOOUUC')  as estado,
nb.uf as uf, 
m.codigo_municipio_completo,
m.nome_municipio,
left(nb.mes_referencia, 4) ano,
sum(cast(replace(nb.valor_parcela, ',','.') as double)) valor_beneficio,
DENSE_RANK () over (partition by left(nb.mes_referencia, 4) order by sum(cast(replace(nb.valor_parcela, ',','.') as double)) desc) as DenseRank,
'Novo Bolsa Familia' as nome_beneficio
from datalake.gold.tb_portal_transparencia_novo_bolsa_familia nb
left join datalake.gold.tb_municipios_siafi_ibge s on s.cod_siafi=int(nb.CODIGO_MUNICIPIO_SIAFI)
left join (select distinct nome_uf, uf,codigo_municipio_completo,municipio, nome_municipio from datalake.gold.tb_ibge_municipios) m on m.codigo_municipio_completo = s.cod_mun_ibge
group by TRANSLATE(m.nome_uf , 'áàãâéèêíìóòõôúùûçÁÀÃÂÉÈÊÍÌÓÒÕÔÚÙÛÇ', 'aaaaeeeiioooouucAAAAEEEIIOOOUUC'),
nb.uf, 
m.codigo_municipio_completo,
m.nome_municipio,
left(nb.mes_referencia, 4)

-- COMMAND ----------

create or replace table datalake.diamond.tb_beneficios_aux_br_ano as 
select  
TRANSLATE(m.nome_uf , 'áàãâéèêíìóòõôúùûçÁÀÃÂÉÈÊÍÌÓÒÕÔÚÙÛÇ', 'aaaaeeeiioooouucAAAAEEEIIOOOUUC')  as estado,
ab.uf as uf, 
m.codigo_municipio_completo,
m.nome_municipio,
left(ab.mes_referencia, 4) ano,
sum(cast(replace(ab.valor_parcela, ',','.') as double)) valor_beneficio,
DENSE_RANK () over (partition by left(ab.mes_referencia, 4) order by sum(cast(replace(ab.valor_parcela, ',','.') as double)) desc) as DenseRank,
'Auxilio Brasil' as nome_beneficio
from datalake.gold.tb_portal_transparencia_auxilio_brasil ab
left join datalake.gold.tb_municipios_siafi_ibge s on s.cod_siafi=int(ab.CODIGO_MUNICIPIO_SIAFI)
left join (select distinct nome_uf, uf,codigo_municipio_completo,municipio, nome_municipio from datalake.gold.tb_ibge_municipios) m on m.codigo_municipio_completo = s.cod_mun_ibge
group by TRANSLATE(m.nome_uf , 'áàãâéèêíìóòõôúùûçÁÀÃÂÉÈÊÍÌÓÒÕÔÚÙÛÇ', 'aaaaeeeiioooouucAAAAEEEIIOOOUUC'),
ab.uf, 
m.codigo_municipio_completo,
m.nome_municipio,
left(ab.mes_referencia, 4)

-- COMMAND ----------

create or replace table datalake.diamond.tb_beneficios_aux_emerg_ano as 
select  
TRANSLATE(m.nome_uf , 'áàãâéèêíìóòõôúùûçÁÀÃÂÉÈÊÍÌÓÒÕÔÚÙÛÇ', 'aaaaeeeiioooouucAAAAEEEIIOOOUUC')  as estado,
ae.uf as uf, 
m.codigo_municipio_completo,
m.nome_municipio,
left(ae.mes_disponibilizacao, 4) ano,
sum(cast(replace(ae.valor_beneficio, ',','.') as double)) valor_beneficio,
DENSE_RANK () over (partition by left(ae.mes_disponibilizacao, 4) order by sum(cast(replace(ae.valor_beneficio, ',','.') as double)) desc) as DenseRank,
'Auxilio Emergencial' as nome_beneficio
from datalake.gold.tb_portal_transparencia_auxilio_emergencial ae
left join (select distinct nome_uf, uf,codigo_municipio_completo,municipio, nome_municipio from datalake.gold.tb_ibge_municipios) m on m.codigo_municipio_completo=ae.CODIGO_MUNICIPIO_IBGE
left join datalake.gold.tb_municipios_siafi_ibge s on m.codigo_municipio_completo = s.cod_mun_ibge
group by TRANSLATE(m.nome_uf , 'áàãâéèêíìóòõôúùûçÁÀÃÂÉÈÊÍÌÓÒÕÔÚÙÛÇ', 'aaaaeeeiioooouucAAAAEEEIIOOOUUC'),
ae.uf, 
m.codigo_municipio_completo,
m.nome_municipio,
left(ae.mes_disponibilizacao, 4)


-- COMMAND ----------

create or replace table datalake.diamond.tb_beneficios_bolsa_fam_ano as 
select  
TRANSLATE(m.nome_uf , 'áàãâéèêíìóòõôúùûçÁÀÃÂÉÈÊÍÌÓÒÕÔÚÙÛÇ', 'aaaaeeeiioooouucAAAAEEEIIOOOUUC')  as estado,
bp.uf as uf, 
m.codigo_municipio_completo,
m.nome_municipio,
left(bp.mes_referencia, 4) ano,
sum(cast(replace(bp.valor_parcela, ',','.') as double)) valor_beneficio,
DENSE_RANK () over (partition by left(bp.mes_referencia, 4) order by sum(cast(replace(bp.valor_parcela, ',','.') as double)) desc) as DenseRank,
'Bolsa Familia' as nome_beneficio
from datalake.gold.tb_portal_transparencia_bolsa_familia_pagamentos bp
left join datalake.gold.tb_municipios_siafi_ibge s on s.cod_siafi=int(bp.CODIGO_MUNICIPIO_SIAFI)
left join (select distinct nome_uf, uf,codigo_municipio_completo,municipio, nome_municipio from datalake.gold.tb_ibge_municipios) m on m.codigo_municipio_completo = s.cod_mun_ibge
group by TRANSLATE(m.nome_uf , 'áàãâéèêíìóòõôúùûçÁÀÃÂÉÈÊÍÌÓÒÕÔÚÙÛÇ', 'aaaaeeeiioooouucAAAAEEEIIOOOUUC'),
bp.uf , 
m.codigo_municipio_completo,
m.nome_municipio,
left(bp.mes_referencia, 4)

-- COMMAND ----------

create or replace table datalake.diamond.tb_beneficios_br as 

select * from  datalake.diamond.tb_beneficios_aux_br_ano

union all

select * from datalake.diamond.tb_beneficios_nv_bolsa_fam_ano

union all

select * from datalake.diamond.tb_beneficios_aux_emerg_ano

union all

select * from datalake.diamond.tb_beneficios_bolsa_fam_ano

-- COMMAND ----------

select count(*) from datalake.diamond.vw_beneficios_brasil

