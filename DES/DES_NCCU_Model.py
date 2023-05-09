import simpy
import random
import pandas as pd
import csv
import warnings
import os
warnings.filterwarnings('ignore')


file_name = "./trial_results.csv"
headers = ["Run_Number", "patient_counter", "Mean_Q_Time_NICU", "Mean_Q_Time_HDCU", "Mean_Q_Time_SCBU"]
        
# Check if the file exists
if not os.path.isfile(file_name):
    with open(file_name, "w", newline='') as f:
        writer = csv.writer(f, delimiter=",")
        writer.writerow(headers)  # Write headers if the file does not exist


# Class to store global parameter values.  We don't create an instance of this
# class - we just refer to the class blueprint itself to access the numbers
# inside
class g:
    # As initial model was set up for time in minutes and we are modelling days
    # the inter arrival time set below zero as fraction of day passed between births
    day_births_inter = 0.125 #3000/365 = 8.219178..... 1/8 = 0.125 as we are incrementing in days 
    chance_need_NICU = 0.02
    chance_need_HDCU = 0.08
    chance_need_SCBU = 0.1
    chance_discharge = 0.8
    avg_NICU_stay = 20
    avg_HDCU_stay = 20
    avg_SCBU_stay = 20
    number_of_NICU_cots = 10
    number_of_HDCU_cots = 10
    number_of_SCBU_cots = 10
    sim_duration = 200
    number_of_runs = 20
    warm_up_duration =50
    
# Class representing our patients coming in for the weight loss clinic.
# This time we've added another attribute, that will store the calculated
# queuing time for the nurse for each patient (each instance of this class)
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
        
# Class representing our model of the GP Surgery.
class NCCU_Model:
    
    """1"""
    def __init__(self, run_number):
        self.env = simpy.Environment()
        self.patient_counter = 0
        
        self.NICU = simpy.Resource(self.env, capacity=g.number_of_NICU_cots)
        self.HDCU = simpy.Resource(self.env, capacity=g.number_of_HDCU_cots)
        self.SCBU = simpy.Resource(self.env, capacity=g.number_of_SCBU_cots)
        
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
        
    # A method that generates births 
    def generate_birth_arrivals(self):
        # Keep generating indefinitely (until the simulation ends)
        while True:
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
            
            # Randomly sample the time to the next patient arriving for the
            # weight loss clinic.  The mean is stored in the g class.
            sampled_interarrival = random.expovariate(1.0 / g.day_births_inter)
            
            # Freeze this function until that time has elapsed
            yield self.env.timeout(sampled_interarrival)
            
    # A method that models the processes for births and assigning resources.
    # The method needs to be passed a patient who will go through these
    # processes
    def manage_birth_resource(self, birth):
        # Record the time the patient started queuing for a cot
        start_cot_wait = self.env.now
        
        if birth.NICU_Pat == True:
            # Request a NICU cot
            with self.NICU.request() as req:
                # Freeze the function until the request for a cot can be met
                yield req
                
                # Record the time the patient finished queuing for a nurse
                end_NICU_wait = self.env.now
                
                # Calculate the time this patient spent queuing for a cot and
                # store in the patient's attribute
                birth.q_time_NICU = end_NICU_wait - start_cot_wait
                
                # Randomly sample the time the patient will spend in NICU
                # The mean is stored in the g class.
                sampled_NICU_duration = random.expovariate(1.0 / g.avg_NICU_stay)
                
                # Freeze this function until that time has elapsed
                yield self.env.timeout(sampled_NICU_duration)
                
        # Reinitialist the cot wait on exiting the previous Cot
        start_cot_wait = self.env.now
                
        if birth.HDCU_Pat == True:
            # Request a HDCU cot
            with self.HDCU.request() as req:
                # Freeze the function until the request for a cot can be met
                yield req
                
                # Record the time the patient finished queuing for a nurse
                end_HDCU_wait = self.env.now
                
                # Calculate the time this patient spent queuing for a cot and
                # store in the patient's attribute
                birth.q_time_HDCU = end_HDCU_wait - start_cot_wait
                
                # Randomly sample the time the patient will spend in NICU
                # The mean is stored in the g class.
                sampled_HDCU_duration = random.expovariate(1.0 / g.avg_HDCU_stay)
                
                # Freeze this function until that time has elapsed
                yield self.env.timeout(sampled_HDCU_duration)
    
        # Reinitialist the cot wait on exiting the previous Cot
        start_cot_wait = self.env.now
        
        if birth.SCBU_Pat == True:
            # Request a SCBU cot
            with self.SCBU.request() as req:
                # Freeze the function until the request for a cot can be met
                yield req
                
                # Record the time the patient finished queuing for a nurse
                end_SCBU_wait = self.env.now
                
                # Calculate the time this patient spent queuing for a cot and
                # store in the patient's attribute
                birth.q_time_SCBU = end_SCBU_wait - start_cot_wait
                
                # Randomly sample the time the patient will spend in NICU
                # The mean is stored in the g class.
                sampled_SCBU_duration = random.expovariate(1.0 / g.avg_SCBU_stay)
                
                # Freeze this function until that time has elapsed
                yield self.env.timeout(sampled_SCBU_duration)
                
        if self.env.now > g.warm_up_duration:
            self.store_results(birth)
            
    
    def store_results(self, patient):        
        if patient.NICU_Pat == True or patient.HDCU_Pat == True or patient.SCBU_Pat == True:
            patient.q_time_ed_assess = float("nan")
        else:
            patient.q_time_acu_assess = float("nan")
            
        df_to_add = pd.DataFrame({"P_ID":[patient.id],
                                        "Q_Time_NICU":[patient.q_time_NICU],
                                        "Q_Time_HDCU":[patient.q_time_HDCU],
                                        "Q_Time_SCBU":[patient.q_time_SCBU]})
        
        df_to_add.set_index("P_ID", inplace=True)
        self.results_df = self.results_df.append(df_to_add)            
    # A method that calculates the average queing time for the cots.  We can
    # call this at the end of each run
    """3"""
    def calculate_mean_q_time_bed(self):
        self.mean_q_time_NICU = self.results_df["Q_Time_NICU"].mean()
        self.mean_q_time_HDCU = self.results_df["Q_Time_HDCU"].mean()
        self.mean_q_time_SCBU = self.results_df["Q_Time_SCBU"].mean()
        
    # A method to write run results to file.  Here, we write the run number
    # against the the calculated mean queuing time for the nurse across
    # patients in the run.  Again, we can call this at the end of each run
    """4"""

    def write_run_results(self):

        with open(file_name, "a", newline='') as f:
            writer = csv.writer(f, delimiter=",")
            results_to_write = [self.run_number,
                                self.patient_counter,
                                self.mean_q_time_NICU,
                                self.mean_q_time_HDCU,
                                self.mean_q_time_SCBU
                                ]
            writer.writerow(results_to_write)

    # The run method starts up the entity generators, and tells SimPy to start
    # running the environment for the duration specified in the g class. After
    # the simulation has run, it calls the methods that calculate run
    # results, and the method that writes these results to file
    def run(self):
        # Start entity generators
        self.env.process(self.generate_birth_arrivals())
        
        # Run simulation
        self.env.run(until=g.sim_duration)
        
        """5"""
        # Calculate run results
        self.calculate_mean_q_time_bed()
        
        # Write run results to file
        self.write_run_results()

# Class to store, calculate and manipulate trial results in a Pandas DataFrame
"""6"""
class Trial_Results_Calculator:
    # The constructor creates a new Pandas DataFrame, and stores this as an
    # attribute of the class instance
    def __init__(self):
        self.trial_results_df = pd.DataFrame()
        
    # A method to read in the trial results (that we wrote out elsewhere in the
    # code) and print them for the user
    def print_trial_results(self):
        print("TRIAL RESULTS")
        print("-------------")
        
        self.trial_results_df = pd.read_csv("./trial_results.csv")
        
        # Take average over runs
        trial_mean_q_time_NICU = (
            self.trial_results_df["Mean_Q_Time_NICU"].mean())
        trial_mean_q_time_HDCU = (
            self.trial_results_df["Mean_Q_Time_HDCU"].mean())
        trial_mean_q_time_SCBU = (
            self.trial_results_df["Mean_Q_Time_SCBU"].mean())
        
        print ("Mean Wait Days for NICU over Trial :",
               f"{trial_mean_q_time_NICU:.2f}")
        print ("Mean Wait Days for HDCU over Trial :",
               f"{trial_mean_q_time_HDCU:.2f}")
        print ("Mean Wait Days for SCBU over Trial :",
               f"{trial_mean_q_time_SCBU:.2f}")


# Everything above is definition of classes and functions, but here's where
# the code will start actively doing things.        

# Create a file to store trial results, and write the column headers
"""7"""
with open("./trial_ed_results.csv", "w") as f:
    writer = csv.writer(f, delimiter=",")
    column_headers = ["Run",
                      "trial_mean_q_time_NICU",
                      "trial_mean_q_time_HDCU",
                      "trial_mean_q_time_SCBU"]
    writer.writerow(column_headers)

# For the number of runs specified in the g class, create an instance of the
# GP_Surgery_Model class, and call its run method
for run in range(g.number_of_runs):
    print (f"Run {run+1} of {g.number_of_runs}")
    my_gp_model = NCCU_Model(run)
    my_gp_model.run()
    print ()

# Once the trial is complete, we'll create an instance of the
# Trial_Result_Calculator class and run the print_trial_results method
"""8"""
my_trial_results_calculator = Trial_Results_Calculator()
my_trial_results_calculator.print_trial_results()

