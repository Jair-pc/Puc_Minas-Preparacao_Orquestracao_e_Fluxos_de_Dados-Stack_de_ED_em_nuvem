# Importando bibliotecas e testando a instalação

import pandas as pd
import numpy as np
import findspark
import os
findspark.init()
findspark.find()
import pyspark


# In[2]:


# Iniciando Spark

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import when
from py4j.java_gateway import java_import

spark = SparkSession.builder.appName('trabalho_final').getOrCreate()


# In[5]:


# Leitura de dados

campeonato = (
              spark
              .read
              .format('csv')
              .load('s3://bucket-teste-905896794144/Dados/TabelaClubes.csv', sep =',', inferSchema=True, header=True)
)


# In[37]:



# In[16]:


campeonato.createOrReplaceTempView('campeonato')


# In[17]:


campeonato.printSchema()


# In[70]:


# TOP 20 dos Clubes mais Vitoriosos do campeonato ao longo dos anos
Vitoriosos = campeonato.groupBy('Clubes').agg(f.sum('Vitorias').alias("Total"))
V= Vitoriosos.select(Vitoriosos.Clubes, Vitoriosos.Total).orderBy(Vitoriosos.Total.desc()).limit(20)


# In[22]:



# In[73]:


# TOP 20 dos Clubes mais derrotas do campeonato ao longo dos anos
Derrotados = campeonato.groupBy('Clubes').agg(f.sum('Derrotas').alias("Total_D"))
D = Derrotados.select(Derrotados.Clubes, Derrotados.Total_D).orderBy(Derrotados.Total_D.desc()).limit(20)


# In[72]:



# In[77]:


#Transformação Parquet V
V.write.format('parquet').save("s3://bucket-teste-905896794144/Processamento/Consulta1/")


# In[78]:


#Transformação Parquet D
D.write.format('parquet').save("s3://bucket-teste-905896794144/Processamento/Consulta2/")


# In[79]:


# Leitura V
V_Leitura_Parquet = spark.read.parquet("s3://bucket-teste-905896794144/Processamento/Consulta1/")


# In[80]:


# Leitura D
D_Leitura_Parquet = spark.read.parquet("s3://bucket-teste-905896794144/Processamento/Consulta2/")


# In[84]:


#Juntando as leituras Parquet
tabela_final_Indicadores = (V_Leitura_Parquet.join(D_Leitura_Parquet, ['Clubes']))


# In[ ]:


# Salvando a Tabela Final Indicadores
tabela_final_Indicadores.write.format('parquet').save("s3://bucket-teste-905896794144/Processamento/Consulta3/")


# In[ ]:


# Lendo a Tabela Final Indicadores
D_Leitura_Tabela_Final = spark.read.parquet("s3://bucket-teste-905896794144/Processamento/Consulta3/")


# In[85]:



# In[ ]:




