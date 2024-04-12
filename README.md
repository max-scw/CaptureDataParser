# CaptureDataParser
Parser to handle the recordings of the Siemens app Capture4Analysis  (now "Analyze My Workpiece 4 Analysis") for the SINUMERIK Edge.


## Quick start
````python
from CaptureDataParser import parse
from matplotlib import pyplot as plt

# load data
file = "./example/example_data_ff429d74-6482-4d4f-80e1-31419d9bd56c.json"
data = parse(file, rename_hfdata=True)

# plot signals
data.groupby("HFData", "CURRENT", index_as="timeseries").plot()
plt.show()
````
![example_data_renamed_groupby_CURRENT_timeseries.png](docs%2Fexample_data_renamed_groupby_CURRENT_timeseries.png)


### Project structure
There is a python modul that parses the JSON files

## Usage
### Installation
Install the required dependencies
````shell
pip install -r requirements.txt
````
and you are good to go.
Python 3.11 is used for development.

### CapturePayload - data format and methods
Parsing a message file is straight forward with the wrapper function `parse` that returns a `CapturePayload` object. This is basically a collection of dataframes as a python dictionary and some additional methods for convenience. The raw data is accessible like in a plain dictionary, e.g. `data["HFData"]` or `data["LFData]`.
One may select specific columns of the data by providing first the data group (e.g. "LFData", "HFData", "HFBlockEvent" etc.) and the column(s) as second input in both, the `CatpurePayload.get_item()` method and a bracket-style indexing `data["HFData", ["DES_POS|1", "DES_POS|2"]]`.
The keyword `index_as` sets the time or the HFProbeCounter value as the index of the returned pandas.DataFrame using the literals `timeseries` or `HFProbeCounter` as options (`CatpurePayload.get_item(..., index_as="timeseries")`).
Additional keywords such as `no_na=True` or `limit_to` are for convenience, ignoring rows where all entries are NaNs or limiting the returned table to either an HFProbeCounter value or to a given time stamp depending on the provided input type.


The method `CapturePayload.groupby()` conveniently returns all signals that have the queried name but may differ in the suffix of the axes.
An example is provided above at [quick start](##Quick start).
If you prefer the plain data without renaming the axes, use:
````python
# without renaming
data = parse(file)
# plot signals as default series
data.groupby("HFData", "CURRENT").plot()
plt.show()
````
![example_data_groupby_CURRENT.png](docs%2Fexample_data_groupby_CURRENT.png)
There is another method that might come in handy to identify comparable recordings. `CapturePayload.hash_g_code()` indexes the "HFBlockEvent" data w.r.t. the active G-code (`data["HFBlockEvent", "GCode"]`) calculates a unique hash for this sequence. 


### Additional functions
The functions [extract_recordings.py](extract_recordings.py) and [transform_recordings.py](transform_recordings.py) are not part of the module. They may help when processing the raw files by first extracting all [zip files](https://en.wikipedia.org/wiki/ZIP_(file_format)) as can be downloaded from *Capture4Analysis* / *AMW4Analysis*.
````shell
python extract_recordings.py --source ./downloads --destination ./Data
````
Afterward, you may want to transform the files in a more convenient format, e.g. a simple CSV file only of the high frequency signals. This is where [transform_recordings.py](transform_recordings.py) may help.
````shell
python transform_recordings.py --source ./Data --destination ./Export
````
This will also create an info file w.r.t. the tool used in order to better organize the exported data. Note that this also stores the hash of the G-code (`CapturePayload.hash_g_code()`) to identify files with the exact same NC code.

## Disclaimer
This is no official repository of any company (in particular Siemens). Therefore, there is no support or liability by those companies.

## Author
 - max-scw

## Status
active