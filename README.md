# resource-contextualization—import-scripts

Data import and control Layer

> Python scripts to import data from different sources. 

## Getting Started

You need a proper **python** interpreter for your system. Download it from [Python downloads](https://www.python.org/downloads/)

Version tested with this application is 2.7.

Also, you need to install **APScheduler** to be able to run the importing synchronization script. Download it from  [APScheduler downloads](https://pypi.python.org/pypi/APScheduler/) 

After downloading it, you only need to execute

sudo python setup.py install

Last version tested is 3.0.3


### synchronizer.py

This script manages which specific scripts are executed, and when they are. To add a new one, you need to upload it to /specific folder and add it its main() function to PROCESSES list.

Currently, all processes are executed asynchronously every hour.

See CRON documentation at : [APScheduler docs](http://apscheduler.readthedocs.org/en/latest/modules/triggers/cron.html) 

### Specific importing scripts

Currently we have 3 different specific scripts into /specific folder: **ckanData.py** (to import ELIXIR training materials), **iannData.py** (events data) and **registryData.py** (ELIXIR records).

You can execute them independently or through synchronizer.py.


## Contributing

Please submit all issues and pull requests to the [elixirhub/resource-contextualization-import-scripts](https://github.com/elixirhub/resource-contextualization-import-scripts/) repository!


## Support
If you have any problem or suggestion please open an issue [here](https://github.com/elixirhub/resource-contextualization-import-scripts/issues).


## License 


This software is licensed under the Apache 2 license, quoted below.

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
