Prompt 1: 
The Office of the Assessor-Recorder publishes a yearly secured property tax roll which includes information on the location of property, value of property, the unique property identifier, and specific property characteristics. 

Create a single combined analytics-ready dataset similar to what is published on our open data portal from the raw data made available by the Assessor-Recorder on their website. 

As a convenience we have pre-loaded the raw tables for this prompt in Snowflake under the ASR schema or you can download them here. 

Transformations to the dataset include:

Renaming columns using this dictionary as reference
Extracting Block and Lot as separate fields from RP1PRCLID. The Block and Lot values are found in the column RP1PRCLID. In the raw data, the string is encoded as up to 9 characters. Block values are within the first 5 characters and Lot numbers follow that and can be up to 4 characters.
Creating a field called Parcel Number by concatenating Block and Lot. The Parcel number is a concatenation of Block and Lot without any spaces. For example, if the Block is 0012 and Lot is 003A, the Parcel Number will be 0012003A.
Add information on 
a.                    Exemption codes using the lookup in this table: EXEMPTION_CODES

b.                   Property Class codes using the lookup in this table: PROPERTY_CODES

c.                    Neighborhood Codes using the lookup in this table: NEIGHBORHOOD_CODES

Add information on Analysis Neighborhood and Supervisor district using this table (they can be joined by Parcel Number): PARCELS
*Note: You only need to apply the transforms specified in the prompt and we do not expect your output to look exactly like the final published dataset for additional columns. For example, we do not expect you to format the date columns.

Prompt 2: 
Public Works has installed sensors on the city garbage cans to reduce the number of overflowing cans and increase the efficiency of trash pickup. The sensors capture data every 5 to 15 minutes on the current fill level of City cans.  As the can fills with waste, an alert is sent to the City’s waste management company, Recology, to service the can before it overflows. 

Create an analytics-ready dataset from the raw sensor data that indicates the time taken for the garbage can to be serviced after an alert was sent to the waste management company. 

The core challenge is to 

Unnest/Flatten the json
Apply logic to create the analytics dataset
Unnest/Flatten the json
The raw sensor readings are written as json files. The raw data are downloaded daily in json format via the sensor company’s API. You can access the raw json files  here. Alternatively, we have set up a table in snowflake (TIMESERIES in the SENSOR schema) with the nested data. 

There is a lot of information in the files but the sensor readings (relevant information) can be extracted from:  

container_id: indicates the specific garbage can 

fill_level_percentage: the fill level when the reading was taken

timestamp:  the date/time when the reading was taken. 

{ "data": {  "data": [  {“container_id”, “fill_level_percentage”, “timestamp” } ] }}

The company itself calculates some columns (e.g. fill rate) and you can ignore these as the information is not relevant for this prompt. For this prompt, only the container_id, fill_level_percentage and timestamp are needed.  

Apply logic to create the analytics dataset
The final dataset should like this:

container_id

alert_triggered_time 

serviced_time

pick_up_time_hrs

overflow

0004a030-5f35-11ea-9a4b-b3257a1bad25

2020-07-11T08:52:20Z

2020-07-11T17:52:17Z

8.99

0

 

The definitions for the 4 derived output columns are as follows:

Alert triggered time: an alert is triggered if the fill level goes above 80% for 4 consecutive readings
Serviced time: a can has been serviced if the fill level drops below 20% for 4 consecutive readings
Pickup time: Is the time difference between an alert going out and the can getting serviced
Overflow: A can overflows if the fill level goes above 100% for 3 consecutive readings between the alert and servicing. 
*Note: this is real data so there are errors for e.g. duplicates, missing time periods, broken sensors with irregular or no readings, cans/sensors being permanently removed from service. We don’t expect you to troubleshoot this in code but we would like to hear more about your ideas on how to put error checking into production in prompt 3. 

 

Prompt 3: 
Scenario: 

Public Works wants to build a set of dashboards based off your work in Prompt 2. They want a near real time view (updated every 15 min) of the overflowing cans and the time to pick up the trash and how that’s changed over time. 

The dataset has already been defined and the dashboards can be developed by Public Works (You DO NOT need to build a dashboard for this prompt). Your task is to propose a way to put this model into production sustainably. You’ll prepare a presentation that highlights tradeoffs and considerations that DataSF will have to think through in moving this to production. For example, handling errors and any other aspects you find important to consider when placing a data product into production.

Feel free to animate your response through examples from prior work and proposing any tools you feel appropriate to the task. You should not feel constrained by DataSF’s current technical stack.

Your audience is DataSF including staff with engineering or analytics background.

Create a brief presentation that:

Walks us through your approach/thinking
Concepts you believe are important
 

Presentation/Slidedeck Guidance:
You will have 15 minutes max to present your answers to the 3 prompts.  For prompt 1 and 2 you can walk us through your code however makes the most sense to you. Either screen share your code, paste it into the slide deck, or some other method that helps us understand your thinking.  

The bulk of your presentation time should be spent walking us through your thinking for Prompt 3.

A hypothetical time breakdown

60 sec: Introduction: Tell us a little about yourself and your interest in this position

2-3 min: Prompt 1

2-3 min: Prompt 2

9-11 min: Prompt 3

5 min: Presentation questions from Panel

