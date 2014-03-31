What is this
============

Adds a custom command which opens a blocking connection with an AMQP server and passes messages to a callback fuction

Settings
--------

This settings are requiered for the command run

``COM_BROKER``

AMQP server ip

``COM_VHOST``

AMQP server Virtual Host

``COM_USERNAME`` 

AMQP server User name for the virtual host


``COM_PASSWORD``

Username's password

``COM_QUEUE``

Which queue listen to

``CONSUMER_CALLBACK``

Path to the callback func. ex.: djamqpconsumer.printconsumer.printdata
Function's return value must be a dict with this format::

  {'result': 0/1,
   'msg': 'String for debug purpouse',
   'retry': True/Fals #Task will be requeued if this is True and task.expiration > 0
  }

``DJCONSUMER_TTL``

TTL for de delayed queue in miliseconds (default 20000). 
Task will be requeued to a delayed queue if its expiration time is not over and the callback function set the
retry flag

Install
-------

Use pip to install from PyPI::

  pip install djamqpconsumer


Usage
-----

Add ``djamqpconsumer`` to your settings.py file::

    INSTALLED_APPS = (
        ...
        'djamqpconsumer',
        ...
    )

Use with manage.py::

  manage.py consumer [debug]
