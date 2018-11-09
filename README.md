# Right Person
iotec Machine learning with profile based classification
profiles can be thought of as amalgamated data with a common trait.
Combining like data provides a more fruitful dataset to use for training of models

## Components

### Data Mining
Right person relies on data mining to build it's input datasets/profiles. 
The data mining is powered by spark.

Accessing a cluster is easy with right person:
```python
>>> from right_person.data_mining.cluster.context_managers import get_spark_cluster_session
>>> with get_spark_cluster_session('cluster-id') as session:
...     # do work using the session
```

In the event that the cluster needs modifiation, the code for the cluster is here: `right_person/data_mining/cluster/`


Miners are incredibly simple to use:
```python
>>> from right_person.data_mining.profiles.miner import RightPersonProfileMiner
>>> miner = RightPersonProfileMiner({}, '')
>>> miner.run()
```

Data miners have a config to specify the format of the data in the profiles.
An example of a miner config looks like this:
```python
>>> from right_person.data_mining.profiles.config import ProfileDocumentConfig, ProfileFieldConfig
>>> config = ProfileDocumentConfig(
...     doc_name='doc_name',
...     delimiter=',',
...     profile_id_field='profile_id_field',
...     files_contain_headers=True,
...     s3_bucket = 'bucket',
...     s3_prefix = 'prefix_with_date_%Y-%m-%d',
...     fields = [ProfileFieldConfig('field_name_in_profile', [0], 'str', 'Counter')]
... )
```

More information is available at the source.

### Models
The right profile models are Logistic regression models. 
The models are serializable and are stored on s3 by default.
Interfacing with the models is easy:
```python
>>> from right_person.machine_learning.models.profile_model import RightPersonModel
>>> model = RightPersonModel('example_model')
>>> model.partial_fit([{'good_example': True}, {'bad_example': True}], [1, 0])
>>> model.predict({'good_example': True})  # returns a number between 0 and 1
```

Models carry a config, which can be used to train the model. 
A good definition supplies instructions to define whether or not a user belongs in the `model.good_users` set.
An audience defines the profiles that should be used as the control group in training.
Config classes for the model are json serialisable and can be found at `right_person.machine_learning.models.config`:

```python
>>> from right_person.machine_learning.models.config import RightPersonModelConfig, ModelSignatureFilter
>>> config = RightPersonModelConfig(
...     [
...     ModelSignatureFilter('good_field', 'good_value', 2)
...     ], [
...     ModelSignatureFilter('audience_field', 'audience_value')
...     ], 10.0)
```

### Stores
All of the right_person assets are serializable to a dict and are stored on s3 for a low cost high persistence solution.
Both the miners and models have store classes that provide this interface:
```python
>>> from right_person.stores.miner_stores import S3ProfileMinerStore
>>> from right_person.stores.model_stores import S3RightPersonModelStore
>>> from right_person.data_mining.cluster.context_managers import get_spark_cluster_session
>>>
>>> model_store = S3RightPersonModelStore('', '', object)
>>> miner_store = S3ProfileMinerStore('', '', object)
>>>
>>> model_ids = model_store.list()
>>> miner = miner_store.retrieve('1234')
>>>
>>> with get_spark_cluster_session('cluster-id') as session:
...     miner.run(session)
...     profiles = miner.profiles(session)
>>>
>>> for model_id in model_ids:
...     model = model_store.retrieve(model_id)
...     model.partial_fit(profiles, [1] * len(profiles))
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

### Command line arguments
Right person ships with a few helpful utility functions to help configure and use the package:

#### edit_cluster_settings
```commandline
$ right_person_cluster_manager --edit-vars
```
This function opens a text interpreter that allows you to edit the right_person config. 
It then validates that config and offers the opportunity to keep editing in the event that the config is invalid.

#### build_ami
```commandline
$ right_person_cluster_manager --build-ami
```
This function rebuilds the ami image that is used by right_person for its spark cluster. 
It does not update the terraform variables.
