import simpy
import random
import pandas as pd
import csv
import warnings
import os
#from tqdm import tqdm
from functools import partial, wraps
warnings.filterwarnings('ignore')
import streamlit as st
import matplotlib.pyplot as plt

# r_file_name = "./resource_monitor_data.csv"
# r_headers = ["Run_Number", "Day", "Resource", "Daily_Use", "Total_Capacity", "Available_Capacity","Queue_Lenth"] 
# Check if the file exists
# if os.path.isfile(r_file_name):
#     os.remove(r_file_name)  # Delete the file if it exists

# # Create a new file
# with open(r_file_name, "w", newline='') as f:
#     writer = csv.writer(f, delimiter=",")
#     writer.writerow(r_headers)  # Write headers to the new file
    

# p_file_name = "./patient_monitor_data.csv"
# p_headers = ["Run_Number","Day_Number","Pat_ID","nicu_pat","nicu_prob","hdcu_pat","hdcu_prob","scbu_pat","scbu_prob"] 
# # Check if the file exists
# if os.path.isfile(p_file_name):
#     os.remove(p_file_name)  # Delete the file if it exists

# # Create a new file
# with open(p_file_name, "w", newline='') as f:
#     writer = csv.writer(f, delimiter=",")
#     writer.writerow(p_headers)  # Write headers to the new file

# Monkey-patching some of a resource’s methods allows you to gather all the data you need.
# Here we add callbacks to a resource that get called just before or after a get / request or a put / release event:
class NamedResource(simpy.PriorityResource):
    def __init__(self, *args, name=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name


st.set_page_config(page_title="Simulation Demo", page_icon="📈")

# st.markdown("""
#         <style>
#                .block-container {
#                     padding-top: 1rem;
#                     padding-bottom: 0rem;
#                     padding-left: 5rem;
#                     padding-right: 5rem;
#                 }
#         </style>
#         """, unsafe_allow_html=True)

st.title("Neonatal Critical Care Bed Use Modelling")

st.write("")

st.markdown(
    """
This is a stochastic discreet event simulation of each of the 3 levels of care provided in a neonatal critical care unit.

"""
)
st.write("")


# Class to store global parameter values.  We don't create an instance of this
# class - we just refer to the class blueprint itself to access the numbers inside
class g:
    
    with st.sidebar:
        st.image('./NECS_Cropped_Dots.png', caption=None, width=300, use_column_width=None, clamp=False, channels="RGB", output_format="auto")
        tab1, tab2 = st.tabs(["Simulation Run Settings", "Unit Parameters"])
        with tab1:
            # simulation run metrics
            st.markdown("""Set your preferred settings for the simulation run times.""")
            # number of cycles before starting data collection
            warm_up_duration = st.number_input("""Simulation warm up - recommended in this scenario we don't open 
                                            doors with an empty unit a large number of days will help us 
                                            account for existing patients of varying 
                                            lengths of stay duration""",None, None, 100, step=1)
            sim_duration = st.number_input("Simulation duration - days",None, None, 300, step=1) # duration of simulation 
            number_of_runs = st.number_input("""Number of times to run the simulation. We run the simulation many 
                                            times and then average out the results to account for busy periods 
                                            and slow periods that can occur in stochastic modelling)""",1, None, 50, step=1)
            
        with tab2:  
            
            st.markdown("""Here we can set our unit parameters""")  
            number_of_NICU_cots = st.slider("Number of NICU cots", 1, 10, 3)
            number_of_HDCU_cots = st.slider("Number of LCU cots", 1, 10, 3)
            number_of_SCBU_cots = st.slider("Number of SCBU cots", 1, 20, 12)
            annual_birth_rate = st.slider("Annual Birth Rate", 1, 6000, 3000)
            day_births_inter = round(annual_birth_rate / 365,2)  # number of births per day - 3000 per year is around 8.2 per day, 
            daily_births = f"This is the equivalent of an average {day_births_inter} per day"
            st.markdown(daily_births)
            
            st.markdown("""These are the average lengths of time spent in each setting, 
                        it is not advised to alter these as they are derived from badgernet data, 
                        but the could be altered if you wanted to look at certain 'what if' scenarios""") 
            avg_NICU_stay = st.number_input("Average length of stay in a LCU cot",12.67, None, 12.67) # average stay in care setting in whole days
            avg_HDCU_stay = st.number_input("Average length of stay in a LCU cot",12.69, None, 12.69) # average stay in care setting in whole days
            avg_SCBU_stay = st.number_input("Average length of stay in a SCBU cot",8.75, None, 8.75)  # average stay in care setting in whole days
            
            # As initial model was set up for time in minutes and we are modelling days
            # day_births_inter is representative of the number of births per day
                                    #this is randomly sampled on an exponential curve
            chance_need_NICU = 0.0233 # percentage chance NICU needed - 70 oo 3000 in 21-22 at warrington
            chance_need_HDCU = 0.0166 # percentage chance HDCU needed - 44 oo 3000 in 21-22 at warrington
            chance_need_SCBU = 0.0343 # percentage chance SCBU needed - 103 oo 3000 in 21-22 at warrington
            # percentage chance to discharge is remainder

            chance_need_HDCU_after_NICU = 0.028 # percentage chance HDCU needed after discharge from NICU
            chance_need_SCBU_after_NICU = 0.087 # percentage chance SCBU needed after discharge from NICU
            # percentage chance to discharge is remainder

            chance_need_NICU_after_HDCU = 0.024 # percentage chance NICU needed after discharge from HDCU
            chance_need_SCBU_after_HDCU = 0.125 # percentage chance SCBU needed after discharge from HDCU
            # percentage chance to discharge is remainder

            chance_need_NICU_after_SCBU = 0.003 # percentage chance NICU needed after discharge from SCBU
            chance_need_HDCU_after_SCBU = 0.017 # percentage chance SCBU needed after discharge from SCBU
            # percentage chance to discharge is remainder

            

# Class representing our births requiring additional care.
class Birth_Patient:
    def __init__(self, p_id, prob_NICU, prob_HDCU, prob_SCBU):
        self.id = p_id
        self.q_time_NICU = 0
        self.q_time_HDCU = 0
        self.q_time_SCBU = 0
        self.NICU_Pat = False
        self.HDCU_Pat = False
        self.SCBU_Pat = False
        self.prob_NICU = prob_NICU
        self.prob_HDCU = prob_HDCU
        self.prob_SCBU = prob_SCBU
        self.nicu_chance = 0
        self.hdcu_chance = 0
        self.scbu_chance = 0
        self.pat_monitor_df = pd.DataFrame()

            
    def determine_destiny(self, prob_var, var_pat, var_chance):
        sc = random.uniform(0, 1)
        if sc < prob_var:
            setattr(self, var_pat, True)
            setattr(self, var_chance, sc)

    # NOT NORMALL IN USE DUE TO SEVERE PERFORMANCE ISSUES, ONLY HERE TO MONITOR WHEN NECESSARY        
    def pat_monitor(self,run_number, day_number):
        id = self.id 
        nicu_pat = self.NICU_Pat
        nicu_prob = self.nicu_chance
        hdcu_pat = self.HDCU_Pat
        hdcu_prob = self.hdcu_chance
        scbu_pat = self.SCBU_Pat
        scbu_prob = self.scbu_chance
        rn = run_number
        dn = day_number
    
        # append data to the dataframe
        
        self.pat_monitor_df = self.pat_monitor_df.append({"Run_Number": rn,
                                                                    "Day_Number": dn,
                                                                    "Pat_ID": id,
                                                                    "nicu_pat": nicu_pat,
                                                                    "nicu_prob": nicu_prob,
                                                                    "hdcu_pat": hdcu_pat,
                                                                    "hdcu_prob": hdcu_prob,
                                                                    "scbu_pat": scbu_pat,
                                                                    "scbu_prob": scbu_prob},
                                                                ignore_index=True)

    def write_pat_details(self):
        with open("./patient_monitor_data.csv", "a", newline='') as f:
            writer = csv.writer(f, delimiter=",")    
            for index, row in self.pat_monitor_df.iterrows():
                writer.writerow(row)

# Class representing our model of Neonatal Unit.
class NCCU_Model:
    
    """1"""
    def __init__(self, run_number):
        self.env = simpy.Environment()
        self.patient_counter = 0

        self.NICU = NamedResource(self.env, capacity=g.number_of_NICU_cots, name='NICU')
        self.HDCU = NamedResource(self.env, capacity=g.number_of_HDCU_cots, name='HDCU')
        self.SCBU = NamedResource(self.env, capacity=g.number_of_SCBU_cots, name='SCBU')
        
        self.run_number = run_number

        self.mean_q_time_cot = 0

        """2"""
        self.results_df = pd.DataFrame()
        self.results_df["P_ID"] = []
        self.results_df["Start_Q_Cot"] = []
        self.results_df["Q_Time_NICU"] = []
        self.results_df["Q_Time_HDCU"] = []
        self.results_df["Q_Time_SCBU"] = []
        self.results_df.set_index("P_ID", inplace=True)

        self.resource_monitor_df = pd.DataFrame()
        self.results_df["Run_Number"] = []
        self.results_df["Day"] = []
        self.results_df["Resource"] = []
        self.results_df["Daily_Use"] = []
        self.results_df["Total_Capacity"] = []
        self.results_df["Available_Capacity"] = []
        self.results_df["Queue_Length"] = []

        self.NICU_usage = {}
        self.HDCU_usage = {}
        self.SCBU_usage = {}
        
    # A method that generates births 
    def generate_birth_arrivals(self):
        # Keep generating indefinitely (until the simulation ends)
        while True:
            # With day as our currency for timestamps here we need to generate multiple agents per day
            sampled_num = round(random.expovariate(1.0 / g.day_births_inter),0)
            sampled_num = int(sampled_num)
            for i in range(sampled_num):
                # Increment the patient counter by 1
                self.patient_counter += 1

                # Create a new patient instance of the Birth_Patient class, 
                # and give the patient an ID determined by the patient counter
                birth = Birth_Patient(self.patient_counter, g.chance_need_NICU, g.chance_need_HDCU, g.chance_need_SCBU)

                birth.determine_destiny(g.chance_need_NICU, 'NICU_Pat', 'nicu_chance')
                birth.determine_destiny(g.chance_need_HDCU, 'HDCU_Pat', 'hdcu_chance')
                birth.determine_destiny(g.chance_need_SCBU, 'SCBU_Pat', 'scbu_chance')

                #THIS MONITORING SIGNIFICANTLY DEGRADES PERFORMANCE ONLY UN_COMMENT 
                #TEMPORARILY TO CHECK VARIABLE ASSIGNMENTS
                # birth.pat_monitor(self.run_number, self.env.now)
                # birth.write_pat_details()

                # Get the SimPy environment to run the manage_birth_resource method
                # with this patient
                self.env.process(self.manage_birth_resource(birth))

                # Freeze this function until that time has elapsed
            yield self.env.timeout(1)
            
    def process_cot_request(self, cot_request, birth, start_cot_wait, avg_stay, next_chances, cot_pat):
        with cot_request:
            # Record the time the patient finished queuing
            end_wait = self.env.now

            # Calculate the time this patient spent queuing for a cot and
            # store in the patient's attribute
            birth.q_time_cot = end_wait - start_cot_wait

            # Randomly sample the time the patient will spend in cot
            sampled_cot_duration = round(random.expovariate(1.0 / avg_stay), 0)
            sampled_cot_duration = int(sampled_cot_duration)

            # Freeze this function until that time has elapsed
            yield self.env.timeout(sampled_cot_duration)

            # reset cot flagv
            setattr(birth, cot_pat, False)

            #calculate the new chances to need the other types of resource having exited one
            for chance, pat, chance_name in next_chances:
                birth.determine_destiny(chance, pat, chance_name)
                if not getattr(birth, pat):
                    break


    # A method that models the processes for births and assigning resources.
    # The method needs to be passed a patient who may require resources
    def manage_birth_resource(self, birth):
        # Record the time the patient started queuing for a cot
        start_cot_wait = self.env.now

        # Release immediately any agents that dont require any resource
        if not (birth.NICU_Pat or birth.HDCU_Pat or birth.SCBU_Pat): 
            return

        # Open a while so that any required cot can be processed while needed
        while birth.NICU_Pat or birth.HDCU_Pat or birth.SCBU_Pat:

            """Process NICU Requirement"""
            if birth.NICU_Pat == True:
                # Request a NICU cot only
                req = self.NICU.request()
                yield req
                yield from self.process_cot_request(
                    self.NICU.request(),
                    birth,
                    start_cot_wait,
                    g.avg_NICU_stay,
                    [
                        (g.chance_need_HDCU_after_NICU, 'HDCU_Pat', 'hdcu_chance'),
                        (g.chance_need_SCBU_after_NICU, 'SCBU_Pat', 'scbu_chance')
                    ],
                    'NICU_Pat'
                )
                self.NICU.release(req)
                break  

            # Reinitialise the cot wait on exiting the previous Cot
            start_cot_wait = self.env.now

            """Process HDU Requirement"""
            
            
            if birth.HDCU_Pat:

                hdu_req = self.HDCU.request(priority=0)
                nicu_req = self.NICU.request(priority=1)

                requests = {self.HDCU: hdu_req, self.NICU: nicu_req}

                # Make a request for each type of cot
                results = yield simpy.AnyOf(self.env, requests.values())

                # Check which request was successful and cancel the other
                used_req = next(iter(results))  # The request that succeeded
                used_res = next(res for res, req in requests.items() if req == used_req)  # The resource for that request

                for res, req in requests.items():
                    if req != used_req:
                        req.cancel()  # Cancel the unused request
                        res.release(req)

                # Now process the used request
                yield from self.process_cot_request(
                    used_req,
                    birth,
                    start_cot_wait,
                    g.avg_HDCU_stay if used_res == self.HDCU else g.avg_NICU_stay,
                    [
                        (g.chance_need_NICU_after_HDCU, 'NICU_Pat', 'nicu_chance'),
                        (g.chance_need_SCBU_after_HDCU, 'SCBU_Pat', 'scbu_chance')
                    ],
                    'HDCU_Pat'
                )
                used_res.release(used_req)

                break

            # Reinitialise the cot wait on exiting the previous Cot
            start_cot_wait = self.env.now
        
            """Process SCBU Requirement"""
            if birth.SCBU_Pat:
                
                scbu_req = self.SCBU.request(priority=0)
                hdu_req = self.HDCU.request(priority=1)
                nicu_req = self.NICU.request(priority=2)

                requests = {self.SCBU: scbu_req, self.HDCU: hdu_req, self.NICU: nicu_req}

                # Make a request for each type of cot
                results = yield simpy.AnyOf(self.env, requests.values())

                # Check which request was successful and cancel the other
                used_req = next(iter(results))  # The request that succeeded
                used_res = next(res for res, req in requests.items() if req == used_req)  # The resource for that request

                for res, req in requests.items():
                    if req != used_req:
                        req.cancel()  # Cancel the unused request
                        res.release(req)

                # Now process the used request
                yield from self.process_cot_request(
                    used_req,
                    birth,
                    start_cot_wait,
                    g.avg_HDCU_stay if used_res == self.HDCU else g.avg_NICU_stay,
                    [
                        (g.chance_need_NICU_after_SCBU, 'NICU_Pat', 'nicu_chance'),
                        (g.chance_need_HDCU_after_SCBU, 'HDCU_Pat', 'hdcu_chance')
                    ],
                    'SCBU_Pat'
                )
                used_res.release(used_req)

                break
            
            # Reinitialise the cot wait on exiting the previous Cot
            start_cot_wait = self.env.now
            
    def monitor(self, resource):
        if self.env.now > g.warm_up_duration:
            day = resource._env.now  # current simulation time
            usage = resource.count # resource count
            total_capacity = resource.capacity # resource capacity
            available_capacity = total_capacity - usage # available resource capacity
            resource_name = resource.name # What resource type?
            queue_length = len(resource.queue) # number of waiting

            # append data to the dataframe
            self.resource_monitor_df = self.resource_monitor_df.append({"Run_Number": self.run_number,
                                                                        "Day": day, 
                                                                        "Resource": resource_name, 
                                                                        "Daily_Use": usage,
                                                                        "Total_Capacity": total_capacity,
                                                                        "Available_Capacity": available_capacity,
                                                                        "Queue_Length": queue_length},
                                                                    ignore_index=True)
        
    def monitor_resource(self, resource):
        while True:
            self.monitor(resource)  
            yield self.env.timeout(1)  # Check resource usage every 1 time unit  

    # def write_run_results(self):
    #     with open("./resource_monitor_data.csv", "a", newline='') as f:
    #         writer = csv.writer(f, delimiter=",")    
    #         for index, row in self.resource_monitor_df.iterrows():
    #             writer.writerow(row)
    
    def daily_scheduler(self):
        while True:
            # Wait for 1 day
            yield self.env.timeout(1)
            # Call monitor for each resource
            self.monitor(self.NICU)
            self.monitor(self.HDCU)
            self.monitor(self.SCBU)           

    # The run method starts up the entity generators, and tells SimPy to start
    # running the environment for the duration specified in the g class. After
    # the simulation has run, it calls the methods that calculate run
    # results, and the method that writes these results to file
    def run(self):
        # Start entity generators
        self.env.process(self.generate_birth_arrivals())
        
        self.env.process(self.daily_scheduler())
        
        # Run simulation
        self.env.run(until=g.sim_duration)
        
        # Write run results to file
        #self.write_run_results()
        
# For the number of runs specified in the g class, create an instance of the
# NCCU_Model class, and call its run method
with st.form(key='my_form'):
    submit_button = st.form_submit_button(label='Run simulations')

if submit_button:
    with st.spinner('Running simulations...'): # adds a progress spinner  
        all_runs_data = pd.DataFrame()  # Initialize all_runs_data
        for run in range(g.number_of_runs):
            my_NCCU_model = NCCU_Model(run)
            my_NCCU_model.run()
            all_runs_data = pd.concat([all_runs_data, my_NCCU_model.resource_monitor_df])
            #all_runs_data = pd.concat(all_runs_data, my_NCCU_model.resource_monitor_df)

        # Read the data from the CSV file
        #data = pd.read_csv('./resource_monitor_data.csv')
        data = all_runs_data

        # Group the data by 'Day' and 'Resource', calculate mean 'Daily_Use'
        data_avg = data.groupby(['Day', 'Resource'])['Daily_Use'].mean().reset_index()

        # List the unique resources in the data
        resources = data_avg['Resource'].unique()

        # Create a new plot
        fig, ax = plt.subplots()

        # For each unique resource, plot a line graph of average daily use
        for resource in resources:
            # Filter data for the current resource
            resource_data = data_avg[data_avg['Resource'] == resource]
            # Plot average daily use for the current resource
            ax.plot(resource_data['Day'], resource_data['Daily_Use'], label=resource)

        ax.set_xlabel('Day') # Set the x-axis label
        ax.set_ylabel('Average Daily Use') # Set the y-axis label
        ax.set_title('Average Daily Use of Resources Over Time') # Set the plot title

        ax.legend()

        st.pyplot(fig)
    st.success('Done!')



# Run from terminal with 'py -m streamlit run DES_NCCU_Model_Streamlit.py'

