# Right Person
iotec Machine learning with profile based classification
profiles can be thought of as amalgamated data with a common trait, defining the data amalgamation

## Data Mining
Right person relies on data mining to build it's input datasets. 
The data mining is powered by spark.
The spark cluster is configurable using the deployable scripts:
```console
$ right_person_cluster_manager --edit-vars --build-ami
// This will allow an update of the variables script and allow the user to build an ami for right person.
```
Accessing a cluster is easy with right person:
```python
>>> from right_person.data_mining.cluster.context_managers import get_spark_cluster_session
>>> with get_spark_cluster_session('cluster-id') as session:
...:    # do work using the session
```

## Models
The right profile models are Logistic regression models. 
The models are serializable and are stored on s3.

# TODOs:
- tests
- 2to3
- documentation
- hosting the package
- review of the cross validation
- more things that i've forgotten