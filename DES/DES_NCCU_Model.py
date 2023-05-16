import simpy
import random
import pandas as pd
import csv
import warnings
import os
from tqdm import tqdm
from functools import partial, wraps
warnings.filterwarnings('ignore')


r_file_name = "./resource_monitor_data.csv"
r_headers = ["Run_Number", "Day", "Resource", "Daily_Use", "Total_Capacity", "Available_Capacity","Queue_Lenth"] 
# Check if the file exists
if os.path.isfile(r_file_name):
    os.remove(r_file_name)  # Delete the file if it exists

# Create a new file
with open(r_file_name, "w", newline='') as f:
    writer = csv.writer(f, delimiter=",")
    writer.writerow(r_headers)  # Write headers to the new file

# Monkey-patching some of a resource’s methods allows you to gather all the data you need.
# Here we add callbacks to a resource that get called just before or after a get / request or a put / release event:
class NamedResource(simpy.Resource):
    def __init__(self, *args, name=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name

def patch_resource(resource, pre=None, post=None):
    def get_wrapper(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if pre:
                pre(resource)
            ret = func(*args, **kwargs)
            if post:
                post(resource)
            return ret
        return wrapper

    for name in ['put', 'get', 'request', 'release']:
        if hasattr(resource, name):
            setattr(resource, name, get_wrapper(getattr(resource, name)))


# Class to store global parameter values.  We don't create an instance of this
# class - we just refer to the class blueprint itself to access the numbers
# inside
class g:
    # As initial model was set up for time in minutes and we are modelling days
    # day_births_inter is representative of the number of births per day
    day_births_inter = 8  # number of births per day - 3000 per year is around 8.2 per day, this is randomly sampled on an exponential curve
    chance_need_NICU = 0.01 # percentage chance NICU needed
    chance_need_HDCU = 0.035 # percentage chance HDCU needed
    chance_need_SCBU = 0.055 # percentage chance SCBU needed
    chance_discharge = 0.9 # percentage chance now additional care needed
    avg_NICU_stay = 4 # average stay in care setting in whole days
    avg_HDCU_stay = 3 # average stay in care setting in whole days
    avg_SCBU_stay = 2 # average stay in care setting in whole days
    number_of_NICU_cots = 5 # Unit capacity of cot type
    number_of_HDCU_cots = 10 # Unit capacity of cot type
    number_of_SCBU_cots = 30 # Unit capacity of cot type
    sim_duration = 730 # duration of simulation 
    number_of_runs = 20 # number of runs 
    warm_up_duration =365 # number of cycles before starting data collection
    
# Class representing our births requiring additional care.
class Birth_Patient:
    def __init__(self, p_id, prob_NICU, prob_HDCU, prob_SCBU):
        self.id = p_id
        self.q_time_NICU = 0
        self.q_time_HDCU = 0
        self.q_time_SCBU = 0
        self.NICU_Pat = 0
        self.HDCU_Pat = 0
        self.SCBU_Pat = 0
        self.prob_NICU = prob_NICU
        self.prob_HDCU = prob_HDCU
        self.prob_SCBU = prob_SCBU
        
    def determine_NICU_destiny(self):
        if random.uniform(0, 1) < self.prob_NICU:
            self.NICU_Pat = True
            
    def determine_HDCU_destiny(self):
        if random.uniform(0, 1) < self.prob_HDCU:
            self.HDCU_Pat = True
            
    def determine_SCBU_destiny(self):
        if random.uniform(0, 1) < self.prob_SCBU:
            self.SCBU_Pat = True
        
# Class representing our model of Neonatal Unit.
class NCCU_Model:
    
    """1"""
    def __init__(self, run_number):
        self.env = simpy.Environment()
        self.patient_counter = 0
        
        self.NICU = NamedResource(self.env, capacity=g.number_of_NICU_cots, name='NICU')
        self.HDCU = NamedResource(self.env, capacity=g.number_of_HDCU_cots, name='HDCU')
        self.SCBU = NamedResource(self.env, capacity=g.number_of_SCBU_cots, name='SCBU')
        
        patch_resource(self.NICU, post=self.monitor)
        patch_resource(self.HDCU, post=self.monitor)
        patch_resource(self.SCBU, post=self.monitor)

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
        
        self.resource_monitor_df = pd.DataFrame(columns=["Day", "Resource", "Daily_Use"])
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
                
                # Create a new patient instance of the Birth_Patient
                # class, and give the patient an ID determined by the patient
                # counter
                birth = Birth_Patient(self.patient_counter, g.chance_need_NICU, g.chance_need_HDCU, g.chance_need_SCBU)
                
                birth.determine_NICU_destiny()
                birth.determine_HDCU_destiny()
                birth.determine_SCBU_destiny()
                
                # Get the SimPy environment to run the manage_birth_resource method
                # with this patient
                self.env.process(self.manage_birth_resource(birth))
                
                # Randomly sample the time to the next birth
                #sampled_interarrival = random.expovariate(1.0 / g.day_births_inter)
                
                # Freeze this function until that time has elapsed
            yield self.env.timeout(1)
            
    # A method that models the processes for births and assigning resources.
    # The method needs to be passed a patient who may require resources
    def manage_birth_resource(self, birth):
        # Record the time the patient started queuing for a cot
        start_cot_wait = self.env.now
        
        if birth.NICU_Pat == True:
            # Request a NICU cot only
            with self.NICU.request() as req:
                # Freeze the function until the request for a cot can be met
                yield req
                
                # Record the time the patient finished queuing
                end_NICU_wait = self.env.now
                
                # Calculate the time this patient spent queuing for a cot and
                # store in the patient's attribute
                birth.q_time_NICU = end_NICU_wait - start_cot_wait
                
                # Randomly sample the time the patient will spend in NICU
                sampled_NICU_duration = round(random.expovariate(1.0 / g.avg_NICU_stay),0)
                
                # Freeze this function until that time has elapsed
                yield self.env.timeout(sampled_NICU_duration)
                
        # Reinitialist the cot wait on exiting the previous Cot
        start_cot_wait = self.env.now
                
        if birth.HDCU_Pat == True:
            # Request a HDCU cot, or a NICU cot
            available_bed_type = yield from self.request_any_available_cot(self.HDCU, self.NICU)
            with available_bed_type.request() as req:
                
                # Record the time the patient finished queuing
                end_HDCU_wait = self.env.now
                
                # Calculate the time this patient spent queuing for a cot and
                # store in the patient's attribute
                birth.q_time_HDCU = end_HDCU_wait - start_cot_wait
                
                # Randomly sample the time the patient will spend in HDCU
                sampled_HDCU_duration = round(random.expovariate(1.0 / g.avg_HDCU_stay),0)
                
                # Freeze this function until that time has elapsed
                yield self.env.timeout(sampled_HDCU_duration)
    
        # Reinitialist the cot wait on exiting the previous Cot
        start_cot_wait = self.env.now
        
        if birth.SCBU_Pat == True:
            # Request a SCBU cot, a HDCU, or a NICU_cot
            available_bed_type = yield from self.request_any_available_cot( self.SCBU, self.HDCU, self.NICU)
            with available_bed_type.request() as req:
                
                # Record the time the patient finished queuing
                end_SCBU_wait = self.env.now
                
                # Calculate the time this patient spent queuing for a cot and
                # store in the patient's attribute
                birth.q_time_SCBU = end_SCBU_wait - start_cot_wait
                
                # Randomly sample the time the patient will spend in NICU
                # The mean is stored in the g class.
                sampled_SCBU_duration = round(random.expovariate(1.0 / g.avg_SCBU_stay),0)
                
                # Freeze this function until that time has elapsed
                yield self.env.timeout(sampled_SCBU_duration)
                
        # if self.env.now > g.warm_up_duration:
        #     self.store_results(birth)
            
    def request_any_available_cot(self, *bed_types):
        # Create request events for each bed type
        requests = {bed_type: bed_type.request() for bed_type in bed_types}

        while True:
            # Freeze the function until the request for a cot can be met
            available_bed = yield simpy.AnyOf(self.env, requests.values())

            # Check the available bed and return the preferred bed type
            for bed_type in bed_types:
                request = requests[bed_type]
                if request in available_bed:
                    return bed_type
            

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
        
    def monitor_resource(self, resource, dic):
        while True:
            dic[self.env.now] = resource.count
            self.monitor(resource)  
            yield self.env.timeout(1)  # Check resource usage every 1 time unit  



    def write_run_results(self):
        with open("./resource_monitor_data.csv", "a", newline='') as f:
            writer = csv.writer(f, delimiter=",")    
            for index, row in self.resource_monitor_df.iterrows():
                writer.writerow(row)

    # The run method starts up the entity generators, and tells SimPy to start
    # running the environment for the duration specified in the g class. After
    # the simulation has run, it calls the methods that calculate run
    # results, and the method that writes these results to file
    def run(self):
        # Start entity generators
        self.env.process(self.generate_birth_arrivals())
        
        self.env.process(self.monitor_resource(self.NICU, self.NICU_usage))
        self.env.process(self.monitor_resource(self.HDCU, self.HDCU_usage))
        self.env.process(self.monitor_resource(self.SCBU, self.SCBU_usage))
        
        # Run simulation
        self.env.run(until=g.sim_duration)
        
            # Create a progress bar for simulation steps
        pbar = tqdm(range(g.sim_duration), desc="Running simulation steps")

        # Run simulation with progress bar
        for _ in pbar:
            self.env.timeout(1)
        
        # Write run results to file
        self.write_run_results()

# For the number of runs specified in the g class, create an instance of the
# NCCU_Model class, and call its run method
#for run in tqdm(range(g.number_of_runs), desc="Running simulations"):
for run in range(g.number_of_runs):
    print (f"Run {run+1} of {g.number_of_runs}")
    my_NCCU_model = NCCU_Model(run)
    my_NCCU_model.run()
    print ()

