# -*- coding: utf-8 -*-
"""
Created on Sun Jan  8 00:32:47 2017

@author: Alex
"""

#Standard libraries
from datetime import date, timedelta
import numpy as np

#Non-standard libraries
import pandas as pd
from pandasql import sqldf
import folium
from bokeh.plotting import figure, output_file, show
from bokeh.resources import CDN
from bokeh.embed import file_html
from bokeh.models import DatetimeTickFormatter, HoverTool, BoxSelectTool, WheelZoomTool, PanTool

#source: http://projects.knmi.nl/klimatologie/daggegevens/selectie.cgi
#Can be loaded directly, no pre-processing is needed
file_name = r'D:\git\pandas-bokeh\data\KNMI_20161227.txt'

def load_weather_data(file_name,station_numbers):
    
    weather_header_list = []
    weather_dict = {}
    spatial_header_list = []
    spatial_dict = {}
    
    def clear_whitespace(element, characters_to_remove):
        characters_in_element = [character for character in list(element) if character not in characters_to_remove]
        cleaned_element = ''.join(characters_in_element)
        return cleaned_element
    
    def process_headers(header_line, header_list, header_dict, separator, unique_id = 'False'):
        selected_header_line = header_line
        split_header_list = selected_header_line.split(separator)
        #Determine headers in the main file (operations in order: remove hashtag, append main body, remove trailing line break)
        uncleaned_header_list = [split_header_list[0][2:]]\
                            + split_header_list[1:-1] \
                            + [split_header_list[-1][:-1]]
                
        for element in uncleaned_header_list:
            cleaned_header = clear_whitespace(element, (' '))
            #Ensure that only valid headers are added (empty = not appended)
            if len(cleaned_header) > 0:
                header_list.append(cleaned_header)
                header_dict[cleaned_header] = []
        if unique_id == True:
            header_list.append('ID')
            header_dict['ID'] = []
            
    
    with open(file_name) as data:
        loaded_data = data.readlines()
        
        '''Process weather data header'''
        process_headers(loaded_data[97], weather_header_list, weather_dict, ',', unique_id = True)
        
        '''Process weather data attributes'''
        uncleaned_weather_data = [line for line in loaded_data[100:] if line[2:5] in station_numbers]#stationNumber]
        weather_data = [line.split(',') for line in uncleaned_weather_data[:]]        
        
        for linenumber, lines in enumerate(weather_data):
            for element_number, elements in enumerate(lines):
                cleaned_element = clear_whitespace(elements,(' ', '\n'))
                #Error handling required to prevent unexpected EOF while parsing - no known alternatives         
                try:
                    evaluated_value = eval(cleaned_element)
                    weather_dict[weather_header_list[element_number]].append(evaluated_value)                    
                except:
                    weather_dict[weather_header_list[element_number]].append(cleaned_element)
            weather_dict['ID'].append(linenumber)
        
        '''Process spatial data headers'''   
        process_headers(loaded_data[4], spatial_header_list, spatial_dict, ' ')
        
        '''Process spatial data attributes'''
        uncleaned_spatial_data = loaded_data[5:55]
        #Splitting spatial data lines
        split_spatial_data = [line.split() for line in uncleaned_spatial_data[:]]    
        spatial_data = list(map(lambda values: values[1:], split_spatial_data))
        #Remove trailing colon after first element
        for lines in spatial_data:
            lines[0] = lines[0][:-1]
            while len(lines) > len(spatial_header_list):
                lines[len(spatial_header_list)-1] = str(lines[len(spatial_header_list)-1]) + ' ' + str(lines[len(spatial_header_list)])
                del lines[len(spatial_header_list)]
            for element_number, element in enumerate(lines):
                try:
                    spatial_dict[spatial_header_list[element_number]].append(eval(element))
                except:
                    spatial_dict[spatial_header_list[element_number]].append(element)
        
        '''Combine header lists'''
        headers = weather_header_list[:] + spatial_header_list[:]
            
    return weather_dict, spatial_dict, headers

perform_SQL = lambda q: sqldf(q, globals())

def get_unique_stations(file_name):
    with open(file_name) as data:
        loaded_data = data.readlines()
        unique_numbers = sorted(list(set(list([line[2:5] for line in loaded_data[100:]]))))
    return unique_numbers
unique_station_numbers = get_unique_stations(file_name)

#Gathers data using SQL queries which is then added to the markers

#Variable for testing. Comment out to loop over the entire set
unique_station_numbers = ['225']

#Initialize weather station map
save_map = True
weather_stations_map = folium.Map(location=[52.092560, 5.109378],zoom_start = 7)

for station_number in unique_station_numbers:
    #Get data for unique station numbers
    loaded_data = load_weather_data(file_name, station_number)

    #Convert data into Pandas Dataframes
    weather_DF = pd.DataFrame(loaded_data[0])[1:].apply(pd.to_numeric)
    spatial_DF = pd.DataFrame(loaded_data[1])    
    
    #Get weather data by querying newly made dataframes
    sql = """
            SELECT STN, MAX(FHX) FROM weather_DF
            WHERE STN = {unique_station_numbers}
          """.format(unique_station_numbers = station_number)
    max_wind_speed_list = perform_SQL(sql).to_csv(None, header=False, index=False).split('\n')[:-1]
    max_wind_speed_list = max_wind_speed_list[0].split(',')
    try:
        max_wind_speed_value = float(max_wind_speed_list[1])/10
    except:
        max_wind_speed_value = "no data"
    
    #Get temperature difference between two dates for each row in the weather_DF dataframe
    sql = """
            SELECT day1.YYYYMMDD, day1.ID, (day1.TG/10) AS day1temp, (day2.TG/10) AS day2temp, IFNULL(CAST(day1.TG - day2.TG AS float)/10, 0) AS difference
            FROM weather_DF AS day1
                JOIN weather_DF day2 ON day1.STN = day2.STN AND day2.ID - day1.ID == 1
          """
    tempdif_DF = perform_SQL(sql)
    tempdif_DF['YYYYMMDD'] = tempdif_DF['YYYYMMDD'].apply(lambda x: pd.to_datetime(str(x), format='%Y%m%d'))    
    biggest_tempdif_value = max(tempdif_DF['difference'])

    try:
        highest_tempdif_row = tempdif_DF.loc[tempdif_DF['difference'] == biggest_tempdif_value]
        highest_tempdif_date = str(highest_tempdif_row.iloc[0][0])

    except:
        biggest_tempdif_formatted = "no data"
        highest_tempdif_date = "no data"
    
    #Draw simple plot for temperature data
    #Create spectral colour palette column (http://bokeh.pydata.org/en/latest/docs/gallery/elements.html)
    differences = tempdif_DF['difference']
    palette = ["#053061", "#2166ac", "#4393c3", "#92c5de", "#d1e5f0",
               "#f7f7f7", "#fddbc7", "#f4a582", "#d6604d", "#b2182b", "#67001f"]
    lower_bound = min(differences)
    upper_bound = max(differences)
    if lower_bound != 0 and upper_bound !=0:
        diff_colours = [int(10*(value - lower_bound)/(upper_bound - lower_bound)) for value in differences]
        tempdif_DF['colours'] = [palette[i] for i in diff_colours]
    else:
        tempdif_DF['colours'] = palette[0]
    
    #Configure the hover tool
    hover = HoverTool(tooltips=[("Temp. difference", "@y{2.2}")])
    
    #Assign the tools and create the plot
    tools = [BoxSelectTool(), WheelZoomTool(), PanTool(), hover]    
    temp_dif_graph = figure(title = "temp. change at station {} in 2 days".format(station_number),\
                            x_axis_label = 'year',
                            y_axis_label = 'temperature difference',
                            tools = tools,
                            plot_width=300,
                            plot_height=300)

    temp_dif_graph.circle(tempdif_DF['YYYYMMDD'], tempdif_DF['difference'],
                          size = 8,
                          color = tempdif_DF["colours"],
                          alpha = 1.0)

    #Set years to x-axis (http://stackoverflow.com/questions/33869292/how-can-i-set-the-x-axis-as-datetimes-on-a-bokeh-plot)
    #Bokeh gets a bit strange with datetime ticks. I had to set months to the year value before it worked with a
    #subselection of the dataframe. If you want to analyse the entire dataframe once, comment out the top line and uncomment the bottom one.
    
    #temp_dif_graph.xaxis.formatter=DatetimeTickFormatter(formats=dict(months=["%Y"]))
    temp_dif_graph.xaxis.formatter=DatetimeTickFormatter(formats=dict(years=["%Y"]))
    
    #Generate HTML for the plot
    weather_dif_graph_html = file_html(temp_dif_graph, CDN, "temp dif plot")
    
    #Get spatial data from spatial_DF
    sql = """
            SELECT "LAT(north)", "LON(east)" FROM spatial_DF
            WHERE STN = {unique_station_numbers}
          """.format(unique_station_numbers = station_number)
    spatial_data_list = perform_SQL(sql).to_csv(None, header=False, index=False).split('\n')[:-1]
    spatial_data_list = spatial_data_list[0].split(',')

    #Create Folium marker and assign to pre-made map
    folium.Marker([spatial_data_list[0], spatial_data_list[1]],\
        popup = folium.Popup(folium.element.IFrame(
        html='''
                <b>Station:</b>            {stn} <br>
                <b>Maximum wind speed:</b> {fg} m/s<br>
             '''.format(stn = max_wind_speed_list[0],\
                   fg = max_wind_speed_value)\
                   + weather_dif_graph_html,\
        width=350, height=350),\
        max_width=350)).add_to(weather_stations_map)

if save_map == True:    
    weather_stations_map.save('weather_stations_map.html')

###Scratchpad###
#Joining the two datasets together - no longer used but kept for reference
def join_dataframes(weather_dict, spatial_dict):
    weather_DF = pd.DataFrame(weather_dict)[1:].apply(pd.to_numeric)
    spatial_DF = pd.DataFrame(spatial_dict)

    #Runtime is about 5mins for the full set, it is preferred to not join and handle the entire set at once
    sql = """
            SELECT * FROM weather_DF
            JOIN spatial_DF ON spatial_DF.stn = weather_DF.stn;
          """

    weather_and_spatial_data_DF = perform_SQL(sql)
    return weather_and_spatial_data_DF    
