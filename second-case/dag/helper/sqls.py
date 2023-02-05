sql_TABELA_1 = '''
WITH TABELA1 AS (
  SELECT FORMAT_DATE("%Y", DATA_VENDA) AS ANO
  ,FORMAT_DATE("%m", DATA_VENDA) AS MES
  ,SUM(QTD_VENDA) AS QTD_VENDA
  FROM `TABLES.RAW`
  GROUP BY ANO, MES
)
SELECT * FROM TABELA1
ORDER BY ANO, MES
'''

sql_TABELA_2 = '''
WITH TABELA2 AS (
  SELECT MARCA
  ,LINHA
  ,SUM(QTD_VENDA) AS QTD_VENDA
  FROM `TABLES.RAW`
  GROUP BY MARCA, LINHA
)
SELECT * FROM TABELA2
ORDER BY MARCA, LINHA
'''

sql_TABELA_3 = '''
WITH TABELA3 AS (
  SELECT MARCA
  ,FORMAT_DATE("%Y", DATA_VENDA) AS ANO
  ,FORMAT_DATE("%m", DATA_VENDA) AS MES
  ,SUM(QTD_VENDA) AS QTD_VENDA
  FROM `TABLES.RAW`
  GROUP BY MARCA, ANO, MES
)
SELECT * FROM TABELA3
ORDER BY MARCA, ANO, MES
'''

sql_TABELA_4 = '''
WITH TABELA4 AS (
  SELECT LINHA
  ,FORMAT_DATE("%Y", DATA_VENDA) AS ANO
  ,FORMAT_DATE("%m", DATA_VENDA) AS MES
  ,SUM(QTD_VENDA) AS QTD_VENDA
  FROM `TABLES.RAW`
  GROUP BY LINHA, ANO, MES
)
SELECT * FROM TABELA4
ORDER BY LINHA, ANO, MES
'''