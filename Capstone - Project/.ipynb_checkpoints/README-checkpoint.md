# Enriching covid data with region information
### Data Engineer Udacity Nanodegree - Final project

## Project purpose
Enrich a **covid** dataset with information about city, city micro and macro region using PySpark and Pandas.
Now new analysis can be build from those new region and areas information and maybe help city halls, state government 
or even the federal government to distribute resources(money, health equipments, vaccines and so on)
This project intend to get relations between the covid cases and cities regions in Brazil.

## Datasources
It will use 2 datasources, one which contain covid cases and another focused on city information to enrich the fisrt and get better insights.


1. **Covid data:** This dataset is from Kaggle.
   It's basically a csv file where we can find the data about covid cases and deaths in Brazil, by date and city.
   https://www.kaggle.com/rafaelherrero/covid19-brazil-full-cases-17062021
2. **Brazil city information:** In this dataset is where we can find more information about the city as the city state,
   macro and micro region. https://www.ibge.gov.br/explica/codigos-dos-municipios.php#PR
   
## Cleaning Steps
1. Perform the cast in order to have the right types for each field
1. Remove null values from some important fields (city, city_ibge_code..)

## Data quality will check for:
1. Integrity of the table schemas
1. Verify for empty tables

## Data Model
The model will be basically divided in three tables.
In this way we can keep each entity in a table, it's better to enrich and we avoid a lot of repeated data(as the city was in the original dataset).
For the queries we can use the 'city_ibge_code' (used here a ForeignKey) to link events with the city and all information about that. The same for the date using the 'date' field.

1. Cities
1. Events
1. Calendar<br>
<img src="schema.png">