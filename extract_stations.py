import os
import json

"""
    This utility will plug in a query string into the input_string placeholder. It will then capture JSON output of the response and capture its data for enriching the final table.
"""

# api_key = 'AIzaSyBd9t_0bohMH5RybdFWaINeJYQf0dp-RPc'

# conn_string = f'https://maps.googleapis.com/maps/api/place/findplacefromtext/json?&input={input_string}&inputtype=textquery&fields=name,formatted_address,place_id&key={api_key}'


def main():
    # item_list = []
    # with open("C:\\Users\\steph\\Documents\\Lambda Projects\\Lambda-Comp-Sci\\python_projects\\data_pipeline\\source_files\\distinct_stations.json", 'r') as stations:
    #     json_stations = json.load(stations)
    #     for station in json_stations:
    #         # print(station['start_station_name'])
    #         item_list.append(station['start_station_name'])

    # with open('C:\\Users\\steph\\Documents\\Lambda Projects\\Lambda-Comp-Sci\\python_projects\\data_pipeline\\source_files\\station_names.txt', 'w') as file:
    #     for item in item_list:
    #         file.write(item + '\n')

    # This should be in a separate "Transform" step, not in the main function. Ideally each phase of the operation should live in its own area and the ETL can be composed by drawing methods out of it.

    with open('C:\\Users\\steph\\Documents\\Lambda Projects\\Lambda-Comp-Sci\\python_projects\\data_pipeline\\source_files\\station_names.txt', 'r') as file:
        for line in file.readlines():
            if 'Public Rack - ' in line:
                print(line)


if __name__ == '__main__':
    main()
