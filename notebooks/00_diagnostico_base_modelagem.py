# Databricks notebook source
# MAGIC %md
# MAGIC # Tech Challenge Fase 3
# MAGIC ## Diagnóstico da Base de Modelagem
# MAGIC
# MAGIC ### Objetivo
# MAGIC Validar se os dados construídos na Fase 2 possuem
# MAGIC qualidade, volume e estrutura suficientes para o
# MAGIC desenvolvimento do modelo supervisionado.
# MAGIC
# MAGIC Nesta etapa serão avaliados:
# MAGIC
# MAGIC - volume de dados;
# MAGIC - distribuição temporal;
# MAGIC - balanceamento do target;
# MAGIC - cobertura territorial;
# MAGIC - duplicidades;
# MAGIC - valores ausentes;
# MAGIC - potenciais riscos de data leakage.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_registros,
# MAGIC     COUNT(DISTINCT id_aluno) AS alunos_distintos,
# MAGIC
# MAGIC     COUNT(DISTINCT ano) AS qtd_anos,
# MAGIC     MIN(ano) AS primeiro_ano,
# MAGIC     MAX(ano) AS ultimo_ano,
# MAGIC
# MAGIC     COUNT(DISTINCT id_municipio) AS qtd_municipios,
# MAGIC     COUNT(DISTINCT sigla_uf) AS qtd_ufs,
# MAGIC     COUNT(DISTINCT rede) AS qtd_redes,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN alfabetizado = TRUE
# MAGIC             THEN 1 ELSE 0
# MAGIC         END
# MAGIC     ) AS alfabetizados,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN alfabetizado = FALSE
# MAGIC             THEN 1 ELSE 0
# MAGIC         END
# MAGIC     ) AS nao_alfabetizados,
# MAGIC
# MAGIC     ROUND(
# MAGIC         100 * AVG(
# MAGIC             CASE
# MAGIC                 WHEN alfabetizado = TRUE THEN 1.0
# MAGIC                 WHEN alfabetizado = FALSE THEN 0.0
# MAGIC             END
# MAGIC         ),
# MAGIC         2
# MAGIC     ) AS percentual_alfabetizados
# MAGIC
# MAGIC FROM workspace.alfabetizacao_silver.fato_alunos;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     ano,
# MAGIC     COUNT(*) AS total_alunos,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN alfabetizado = TRUE
# MAGIC             THEN 1 ELSE 0
# MAGIC         END
# MAGIC     ) AS alfabetizados,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN alfabetizado = FALSE
# MAGIC             THEN 1 ELSE 0
# MAGIC         END
# MAGIC     ) AS nao_alfabetizados,
# MAGIC
# MAGIC     ROUND(
# MAGIC         100 * AVG(
# MAGIC             CASE
# MAGIC                 WHEN alfabetizado = TRUE THEN 1.0
# MAGIC                 WHEN alfabetizado = FALSE THEN 0.0
# MAGIC             END
# MAGIC         ),
# MAGIC         2
# MAGIC     ) AS percentual_alfabetizados
# MAGIC
# MAGIC FROM workspace.alfabetizacao_silver.fato_alunos
# MAGIC
# MAGIC GROUP BY ano
# MAGIC ORDER BY ano;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS combinacoes_duplicadas
# MAGIC FROM (
# MAGIC
# MAGIC     SELECT
# MAGIC         ano,
# MAGIC         id_aluno,
# MAGIC         COUNT(*) AS quantidade
# MAGIC
# MAGIC     FROM workspace.alfabetizacao_silver.fato_alunos
# MAGIC
# MAGIC     GROUP BY
# MAGIC         ano,
# MAGIC         id_aluno
# MAGIC
# MAGIC     HAVING COUNT(*) > 1
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_teste,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN alfabetizado = TRUE
# MAGIC             THEN 1 ELSE 0
# MAGIC         END
# MAGIC     ) AS alfabetizados,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN alfabetizado = FALSE
# MAGIC             THEN 1 ELSE 0
# MAGIC         END
# MAGIC     ) AS nao_alfabetizados,
# MAGIC
# MAGIC     ROUND(
# MAGIC         100 * AVG(
# MAGIC             CASE
# MAGIC                 WHEN alfabetizado = TRUE THEN 1.0
# MAGIC                 WHEN alfabetizado = FALSE THEN 0.0
# MAGIC             END
# MAGIC         ),
# MAGIC         2
# MAGIC     ) AS percentual_alfabetizados
# MAGIC
# MAGIC FROM workspace.alfabetizacao_silver.fato_alunos a
# MAGIC
# MAGIC WHERE
# MAGIC     a.ano = 2024
# MAGIC
# MAGIC     AND NOT EXISTS (
# MAGIC         SELECT 1
# MAGIC
# MAGIC         FROM workspace.alfabetizacao_silver.fato_alunos b
# MAGIC
# MAGIC         WHERE
# MAGIC             b.ano = 2023
# MAGIC             AND b.id_aluno = a.id_aluno
# MAGIC     );