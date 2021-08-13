# Python client for Tair

> tairClient is still being developed. Please do not use it for any mission critical purpose at this time.

tairClient-py is a package that gives developers easy access to tair or tairModules. Tair is a redis-like Key/Value structure 
data storage system independently developed by Alibaba. It has added many powerful modules which also can load by redis
such as tairHash, tairString, etc.

The package extends [redis-py's](https://github.com/andymccurdy/redis-py) interface with Tair's API.

### Installation
``` 
$ pip install tairClient
```

### Usage example

```python
from tairClient.client import Client
tair = Client()
tair.exhset("key","field","value",nx=True,ver=2) # returns 1
tair.exhmset("key",{"field2":"value2","field3":"value3","field4":""}) # returns b'OK'
tair.exhlen("key")                 # return 4
tair.exhdel("key","field")         # return 1
tair.exhlen("key")                 # return 3
tair.exhgetall("key")              # returns [b'field3', b'value3', b'field2', b'value2', b'field4', b'']
tair.exhver("key","field3")        # returns 1
tair.exhset("key","field3","2")    # returns 1
tair.exhgetwithver("key","field3") # returns [b'2', 13]
```

### API
For complete documentation about tair's commands, refer to [tair's module website](https://help.aliyun.com/document_detail/146579.html).

```python
from tairClient.client import Client
tair = Client()
# use __doc__ can get a simple doc about tair's class and function
print(tair.__doc__) 
print(tair.exhget.__doc__)
```
### Development and test
install [tox](https://tox.readthedocs.io/en/latest/) , runs all tests as its default target. Ensure you have a running redis, with the module loaded.
```
pip install tox  
```



### License
[Apache License Version 2.0](https://github.com/631086083/tairClient/master/LICENSE)
