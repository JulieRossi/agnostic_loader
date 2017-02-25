[![Build Status](https://travis-ci.org/JulieRossi/agnostic_loader.svg?branch=master)](https://travis-ci.org/JulieRossi/agnostic_loader)

#AGNOSTIC LOADER

Indifferently load and yield data from any type of input among the following:

- dictionary -> yield the dict untouched
- iterator (list, generator) -> yield each element untouched
- json (string) -> yield json loaded
- json file (file containing one json per line) -> loads and yields each line
- csv file -> loads and yields each line (with , as delimiter and does not differentiate header)
- gz file -> recursively json or csv on each line of the file
- directory -> recursively loads each file


### Why ?

This project has been designed in order to easily use the same code 
with different input types (from file or another python program for example).


### How to use

Considering the following:  
input_data: one of the types described above containing your data  
do_stuff: the function you want to apply to your data  

    from data_loader import DataLoader

    for input_element in DataLoader(input_data).load():
        do_stuff(input_element)