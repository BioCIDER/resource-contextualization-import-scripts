# resource-contextualization—import-scripts

Data import and control Layer

> Python scripts to import data from different sources. 

## Getting Started

You need a proper **python** interpreter for your system. Download it from [Python downloads](https://www.python.org/downloads/)

Version tested with this application is 2.7.

Also, you need to install **APScheduler** to be able to run the importing synchronization script. Download it from  [APScheduler downloads](https://pypi.python.org/pypi/APScheduler/) 

After downloading and extract it, you only need to execute

sudo python setup.py install

Last version tested is 3.0.3


Finally, if you want to execute the synchronizer as a service, you will need to install **python-daemon** library. Download it from  [python-daemon downloads](https://pypi.python.org/pypi/python-daemon/#downloads) 

In the same way, after downloading it you only need to execute

sudo python setup.py install

Last version tested is 2.0.6


### synchronizer.py

This script manages which specific scripts are executed, and when they are. To add a new one, you need to upload it to /specific folder and add its main() function to the proper UPDATING_XX_PROCESSES list.

Currently, there are different processes executed asynchronously with different schedules: every hour, every day or every week.

See CRON documentation at : [APScheduler docs](http://apscheduler.readthedocs.org/en/latest/modules/triggers/cron.html) 


### synchronizer-daemon.py

This script should be used when you desire to execute synchronizer.py as a service, without that process depends on the console.


### Specific importing scripts

Currently we have 3 different specific scripts into /specific folder: **ckanData.py** (to import ELIXIR training materials), **iannData.py** (events data) and **registryData.py** (ELIXIR records).

You can execute them independently or through synchronizer.py. Also, they have different entry points with different utility functions to facilitate using the main functionalities.


## Contributing

Please submit all issues and pull requests to the [elixirhub/resource-contextualization-import-scripts](https://github.com/elixirhub/resource-contextualization-import-scripts/) repository!


## Support
If you have any problem or suggestion please open an issue [here](https://github.com/elixirhub/resource-contextualization-import-scripts/issues).
