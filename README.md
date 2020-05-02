# Desafio Nasa

# Introdução

Este realização deste desafio tem como objetivo apresentar os domínio dos conhecimentos teóricos e práticos sobre a ferramenta Spark. Ele está dividido em duas partes. A primeira parte consiste na explicação de alguns conceitos fundamentais relacionados ao Spark. A segunda parte é a análise de um conjunto de dados da *Nasa Kennedy Space Center* através da elaboração de scripts.

# Dados

**Origem**: 

url: https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html

**Dados utilizados**:
* Jul 01 to Jul 31, ASCII format, 20.7 MB gzip compressed, 205.2 MB.
* Aug 04 to Aug 31, ASCII format, 21.8 MB gzip compressed, 167.8 MB.

**Sobre**:

Os dados correspondem às requisições HTTP para o servidor da *NASA Kennedy Space Center* na Flórida.


**Observação**:

O arquivo *access_log_Jul95* tem como último registro *"alyssa.p"*. Este dados solto não permitia a leitura correta dos dados pelo script, e teve de ser removido.

# Scripts e ambiente de execução

Para executar os scripts ***semantix.py*** e/ou ***semantix.ipynb*** que estão salvos neste repositório, é necessário ter instalado os seguintes itens no computador:

* Python3
* Java JDK 8
* Spark 2.4.5 (Windows ou Mac)
* Jupyter Notebook



# Parte 1: Conhecimentos dos fundamentos em Spark

**Qual o objetivo do comando cache em Spark?**

O comando cache é um método de otimização utilizado para acelerar o processamento dos dados no caso de ser necessário acessá-los múltiplas vezes.  Imagine que há um RDD dividido em várias partições. O RDD está sob lazy evaluation no Spark, ou seja, ele não é acessado até que um comando seja executado. Suponha que são executados alguns comandos como read e count do conjunto de dados. Em seguida é efetuado o cache. Todos os comandos subsequentes serão processados com os dados in-memory. Portanto, um RDD que não passou pelo cache, por exemplo, é acessado no disco toda vez que é executado uma função de transformação ou iteração. O cache permite que os dados sejam armazenados in-memory, e, embora auxilie na velocidade do processamento, nem sempre é a melhor solução, pois pode ocorrer de não ter memória suficiente disponível para armazenar os dados. O comando cache é recomendado para situações em que há (a) iteração de aplicações de machine learning, (b) reutilização de aplicações Spark e/ou (c) quando o custo de processamento do RDD é muito alto.

**O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em MapReduce. Por quê?**

Há uma série de fatores que justificam o fato de os códigos implementados em Spark apresentar maior velocidade do que em MapReduce. Abaixo alguns pontos a considerar:

*Processamento.* No caso do MapReduce os dados são gravados em cada etapa do processo - map, shuffle e reduce -, o que torna a operação onerosa quando é necessário acessar os dados várias vezes durante o processamento. 

*Transformações*. Toda vez que uma operação é efetuada utilizando o MapReduce, há um esforço que é direcionado para resgatar todo o dado presente na memória do disco para completar uma task. No caso do Spark isso não ocorre, pois há uma maior flexibilidade sobre como lidar com os dados em disco e/ou in-memory utilizando o cache, por exemplo.

*Arquitetura.* O MapReduce inicia uma instância JVM para cada task, e isso pode levar um tempo, uma vez que é necessário inicializar os arquivos JARS, efetuar XML parsing, etc. Já o Spark mantém o executor JVM funcionando em cada node, o que facilita no rápido processamento das tasks. Cada Spark job cria um DAG (Directed Acyclic Graph) de taks que podem ser processados no cluster. No Caso do MapReduce, há somente dois DAG’s pré-definidos, um para o map e outro para reduce. Os DAG’s em Spark podem ter vários estágios, e um job pode ser finalizado após apenas um estágio. Desta forma, alguns jobs podem ser finalizados mais rapidamente do que no MapReduce.

**Qual é a função do SparkContext?**

O *SparkContext* é uma classe da linguagem de programação Spark que permite a conexão do Spark com o *cluster*. Ele é utilizado para criar RDD’s,  transmitir variáveis  para o cluster, definir parâmetros de configuração do ambiente durante o processamento dos dados e outras propriedades do Spark. O SparkContext pode ser inicializado da seguinte forma:

```
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
sc = SparkContext.getOrCreate()
spark = SQLContext(sc)
```

**Explique com suas palavras o que é Resilient Distributed Datasets (RDD).**

*RDD* ou *Resilient Distributed Datasets* é uma estrutura de dados do Spark. O *RDD* pode ser compreendido como uma coleção de dados ou registros imutáveis que são distribuídos no disco de forma particionada, ou seja, divididos entre diferentes nodes no *cluster*. *O RDD* é *fault-tolerant*, ou seja, ele é bastante resistente a falhas, uma vez que sua estrutura permite que dados perdidos sejam recuperados caso ocorra alguma falha no ambiente. Por exemplo, caso ocorra a perda de uma partição do *RDD*, é possível reproduzir a operação desta partição específica, ao invés de fazer a replicação de dados em todos nós que compõem o *RDD*. Os *RDD’s* podem ser alterados somente através de operações de transformação dos dados. Essas operações podem ser executadas em paralelo, uma vez que o *RDD* está distribuído em diferentes partições. 


**GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê?**

Na operação efetuada em *reduceByKey*, os dados são combinados em cada partição, e apenas um output de cada partição, contendo pares *key-value*, por exemplo, são enviados através do *cluster* para a realização do cálculo final. Ou seja, o Spark executa o *reduceByKey* em cada partição antes de efetuar a redistribuição dos dados ou *shuffle*, resultando em um menor volume de transferência de dados. Já no caso do *groupByKey*, o cálculo de agregação para os pares *key-value* e o *shuffle* é efetuados de uma vez só, sem uma etapa intermediária, exigindo um grande esforço para transmitir os dados através do cluster.


**Explique o que o código Scala abaixo faz.**

```
1. val textFile = sc.textFile("hdfs://...")
2. val counts = textFile.flatMap(line => line.split( " " ))
3.                      .map(word => (word, 1))
4.                      .reduceByKey (_+_)
5                       .counts.saveAsTextFile("hdfs://...")
```

1.	Leitura de um arquivo texto localizado em um diretório do HDFS;
2.	Separação de cada linha utilizando “ “ (espaço) para obter as palavras individualmente;
3.	Cada palavra é mapeada para uma sequência de pares key-value, sendo a key a palavra e o value o número “1”.
4.	Através da função reduceByKey, os valores são agregados tendo como operação matemática a soma “+”;
5.	Por fim, os valores de cada chave são contados e somados. O output é armazenado em um arquivo texto em um diretório do HDFS. 



# Parte 2 - Manipulação e análise de dados em Spark

*Vide os scripts ***semantix.py*** e ***semantix.ipynb*** que estão neste repositório.*

**1. Número de hosts únicos.**

```
Number of unique hosts on july: 55.
Number of unique hosts on august: 53.
```

**2. O total de erros 404.**

```
Total number of 404 erros on july: 10845.
Total number of 404 erros on august: 10056.
```

**3. Os 5 URLs que mais causaram erro 404.**

```
Top 5 url with most frequent 404 errors on july:
/pub/winvn/readme.txt: 667
/pub/winvn/release.txt: 547
/history/apollo/apollo-13.html: 286
/shuttle/resources/orbiters/atlantis.gif: 232
/history/apollo/a-001/a-001-patch-small.gif: 230
```

```
Top 5 url with most frequent 404 errors on august:
/pub/winvn/readme.txt: 1337
/pub/winvn/release.txt: 1185
/shuttle/missions/STS-69/mission-STS-69.html: 683
/images/nasa-logo.gif: 319
/shuttle/missions/sts-68/ksc-upclose.gif: 253
```

**4. Quantidade de erros 404 por dia.**

```
Number of errors 404 per day on july:
13/Jul/1995: 532
21/Jul/1995: 334
25/Jul/1995: 461
09/Jul/1995: 348
15/Jul/1995: 254
16/Jul/1995: 257
18/Jul/1995: 465
17/Jul/1995: 406
07/Jul/1995: 570
12/Jul/1995: 471
19/Jul/1995: 639
22/Jul/1995: 192
23/Jul/1995: 233
03/Jul/1995: 474
05/Jul/1995: 497
10/Jul/1995: 398
14/Jul/1995: 413
01/Jul/1995: 316
02/Jul/1995: 291
04/Jul/1995: 359
06/Jul/1995: 640
08/Jul/1995: 302
11/Jul/1995: 471
20/Jul/1995: 428
24/Jul/1995: 328
26/Jul/1995: 336
27/Jul/1995: 336
28/Jul/1995: 94
```

```
Number of errors 404 per day on august:
01/Aug/1995: 243
07/Aug/1995: 537
09/Aug/1995: 279
10/Aug/1995: 315
21/Aug/1995: 305
27/Aug/1995: 370
30/Aug/1995: 571
03/Aug/1995: 304
06/Aug/1995: 373
08/Aug/1995: 391
16/Aug/1995: 259
20/Aug/1995: 312
05/Aug/1995: 236
11/Aug/1995: 263
12/Aug/1995: 196
13/Aug/1995: 216
15/Aug/1995: 327
17/Aug/1995: 271
22/Aug/1995: 288
23/Aug/1995: 345
24/Aug/1995: 420
26/Aug/1995: 366
28/Aug/1995: 410
14/Aug/1995: 287
18/Aug/1995: 256
19/Aug/1995: 209
31/Aug/1995: 526
04/Aug/1995: 346
25/Aug/1995: 415
29/Aug/1995: 420
```


**5. O total de bytes retornados**

```
Number of bytes of july file: 38695973491
Number of bytes of august file: 26828341424
```

