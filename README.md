# EntityML

Submitted Paper in the Industrial Track of ICDE 2025 titled: Entity-based System Design for Scaling Machine Learning Applications

## Description

Boilerplate code for reproducibility

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
 C:\Users\username\poetryvenvs\EntityML-BBQ6GFCF-py3.10\Scripts\activate
```
* For wsl and linux 
```
 source /root/.cache/pypoetry/virtualenvs/synthetic-data-generator-z4uRWbP2-py3.10/bin/activate
```


### Executing program

* Check all the options 
```
 python .\EntityML\app.py --help
```

* Generate data (by default it creates data for 20 entities)
```
python .\EntityML\app.py generate-data --gen-type regression --sink-type delta
```



## Authors
Vivek Kumar Jain <vivekkumar.jain@asml.com>
Sheng-Yi Hsu <sheng-yi.hsu@asml.com>
Akshay Verma <akshay.verma@asml.com>
Arunabha Choudhury <arunabha.choudhury@asml.com>


## Version History


## notes

- Not using Ray data batches, as they are not random sampled
