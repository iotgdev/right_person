# Right Person
iotec Machine learning with user based classification.

Using a users auction history, we can create a rich dataset, 'profiles', that we can leverage for better performance.
Combining auction data provides a more fruitful dataset to use for training of models.

## Notes
Right person is powered by the spark_data_miner package which is required to run on an ec2 instance

## Components

### spark_data_miner
Right person relies on data mining to build it's input datasets/profiles. 
The data mining is powered by spark.

Creating a spark cluster is easy with right person:
```python
>>> from spark_data_miner.cluster.manager.context_managers import spark_data_mining_session
>>> from spark_data_miner.cluster.manager.access import ClusterPlan
>>> master_instance_type = 'r5.4xlarge'
>>> node_instance_type = 'r5.8xlarge'
>>> node_count = 100
>>> plan = ClusterPlan(master_instance_type, node_instance_type, node_count)
>>> with spark_data_mining_session(plan=plan) as session:
...     # do work using the session
```

Clusters require an AMI to be build in order to function. To build a suitable AMI compatible with right_person:
```commandline
$ build_right_person_ami
``` 
and if an appropriate AMI doesn't exist an exception will be raised.


Data miners have a config to specify the format of the data in the profiles.
An example of a miner config looks like this:
```python
>>> from spark_data_miner.core.config import MinerConfig, MinerField
>>> config = MinerConfig(
...     name='document name',
...     delimiter=',',
...     fields = [MinerField('field_name', [0], 'str', 'counter')],
...     id_field='user id field',
...     files_contain_headers=True,
...     s3_bucket = 'bucket',
...     s3_prefix = 'prefix_with_date_%Y-%m-%d',
... )
```

Miners are incredibly simple to use:
```python
>>> from spark_data_miner.cluster.manager.context_managers import spark_data_mining_session
>>> from spark_data_miner.core.miner import SparkDatasetMiner
>>> with spark_data_mining_session(plan=plan) as session:  # plan defined above
...     miner = SparkDatasetMiner('name', config, 'output_bucket')  # config defined above
...     miner.create_dataset(session)
```

### Models
The right profile models are Logistic regression models. 
All models are stored in the iotec labs API (https://api.ioteclabs.com/rest/)
Interfacing with the models is easy:
```python
>>> from right_person.models.core import RightPersonModel
>>> model = RightPersonModel('example_model_name', 'account')
>>> model.partial_fit([{'good_example': True}, {'bad_example': True}], [1, 0])
>>> model.predict({'good_example': True})  # returns a number between 0 and 1
```

Models can be stored in the api like so:
```python
>>> from right_person.models.store import RightPersonStore
>>> from right_person.stores.model_stores import S3RightPersonModelStore
>>> from right_person.data_mining.cluster.context_managers import right_person_cluster_session
>>>
>>> store = RightPersonStore()
>>>
>>> # list models
>>> models = store.list(as_list=True)  # if as_list kwarg is ommited, the return type is a generator
>>>
>>> # retrieve models
>>> model = store.retrieve(models[0].model_id)
>>>
>>> # create models
>>> del model.model_id
>>> new_model = store.create(model)
>>>
>>> # update models
>>> old_model = models[-1]
>>> old_model.new_name = 'new_name'
>>> store.update(old_model)
```

## Usage

### Installation
You need to configure the iotec package repository to pip install right_person.
The package repository is hosted at pypi.dsp.io

```commandline
$ pip install right_person
```

Alternatively, you can install the package from git. 
The production versions ship without tests.
