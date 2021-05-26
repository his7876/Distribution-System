##  분산시스템 (2021 - 1) using Hadoop :elephant:

-------

### 실습 과제 1 : Tf- Idf using Hadoop MapReduce 

  + First job
    + Mapper : document, content -> word@document,1 
    + Reducer : word@document, 1 -> word@document,n

  + Second job
    + Mapper : word@document, n -> word, document = n
    + Reducer : word, document = n -> word@document , TFIDF
  

----

### 실습 과제 2 :  PageRank
