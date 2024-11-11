# Access Log Analyzer 
Aplicação em que utiliza o Spark (Pyspark) para analisar logs de acesso de servidores web.

## Instruções de instalação
- Como este projeto foi desenvolvido utilizando um Macbook M1, é necessário configurar uma váriavel de ambiente para garantir que o docker-compose funcione corretamente. Para isso, execute o comando abaixo:
```bash
export DOCKER_DEFAULT_PLATFORM=linux/amd64
```
- Para rodar a aplicação, inicialmente é necessário utilizar o arquivo de `Dockerfile` para criar a imagem do container. Para isso, execute o comando abaixo:
```bash
docker build . -t zambotto-spark-3.4.0:v0.0.1
```

## Instruções de execução
- Após a criação da imagem, execute o comando abaixo para subir o container:
```bash
docker-compose up
```

## Arquitetura
- Para estas análises, foi utilizado uma imagem Docker com o Spark 3.4.0 e Python 3.8.
- Foi optado por utilizar o docker-compose.yaml para subir um CLUSTER Spark com 1 master, 2 workers e um outro container para execução do script python (spark-submit).
![image](./arquitetura.png)

## Script Python
- O script python criado é responsável por realizar a leitura do arquivo de log e realizar a contagem de acessos por IP. O script foi desenvolvido utilizando o Spark (Pyspark) e é executado dentro de um container Docker. 
- Optei por realizar as análises utilizando RDDs do Spark, pois acredito que seja mais performático para a análise de logs.
- Por questão de ser uma análise exploratória, não optei por utilizar armazenamento de dados em banco de dados, mas sim, realizar a análise, salvar os resultados e exibir os resultados no console.

