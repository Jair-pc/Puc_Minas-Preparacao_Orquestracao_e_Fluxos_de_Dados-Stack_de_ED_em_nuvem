# Stack_de_ED_em_nuvem_Puc - Atividade 4
Professor Neylson Crepalde

https://github.com/neylsoncrepalde/pucminas-data-pipelines

Projeto sobre Processamento e Orquestração de dados referentes ao Campeonato Brasileiro de Futebol, utilizando Clusters AWS e Airflow.

O que deve ser feito:

- Criar um Bucket S3
- Criar um Cluster EMR
- Criar um cluster Kubernetes
- Deployar o Airflow no cluster Kubernetes
- Criar um usuário chamado `airflow-user` com permissão de administrador da conta
- Escolher um dataset (livre escolha)
- Subir o dataset em um bucket S3
- Pensar e implementar construção de indicadores e análises sobre esse dataset (produzir 2 ou mais indicadores no mesmo grão)
- Escrever no S3 arquivos parquet com as tabelas de indicadores produzidos
- Escrever outro job spark que lê todos os indicadores construídos, junta tudo em uma única tabela
- Escrever a tabela final de indicadores no S3
- Subir um notebook dentro do cluster EMR
- Ler a tabela final de indicadores e dar um `.show()`
- Todo o processo orquestrado pelo AIRFLOW no K8s

# Entregáveis
- link do repositório github com códigos
- print do bucket S3
- print do cluster EMR
- print da DAG no Airflow concluída (visão do Grid)
- print do notebook mostrando a tabela final com `.show()`
Para o envio, só serão aceitos arquivos com extensão `jpg`, `jpeg` ou `png`.

## Deadline: 31/05/2022
