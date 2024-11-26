# EntityML

Generate synthetic data by using scikit-learn's make_regression and make_classification methods. Generated dataset will be used for the white paper focused on Entity based scaling for ML Applications. 

## Description

An in-depth paragraph about your project and overview of use.

## Getting Started

### Dependencies

* Ensure [poetry](https://python-poetry.org/docs/) is installed.

### Installing

* Clone and cd to root dir and run poetry install to install the dependencies

```
poetry install 
```

* To activate environment, first check the correct path 

```
poetry env info --path
```
* In case windows Copy path and append \Scripts\active for example
```
 C:\Users\vivjain\poetryvenvs\synthetic-data-generator-BBQ6GFCF-py3.10\Scripts\activate
```
* For wsl and linux 
```
 source /root/.cache/pypoetry/virtualenvs/synthetic-data-generator-z4uRWbP2-py3.10/bin/activate
```


### Executing program

* Check all the options 
```
 python .\synthetic_data_generator\app.py --help
```

* Generate data (by default it creates data for 20 entities)
```
python .\synthetic_data_generator\app.py generate-data --gen-type regression --sink-type delta
```



## Authors
Vivek Kumar Jain <vivekkumar.jain@asml.com>


## Version History


## notes

- Not using Ray data batches, as they are not random sampled