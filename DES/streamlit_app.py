import streamlit as st
from DES_NCCU_Model_Streamlit import NCCU_Model, g 

def main():
    st.title('Neonatal Critical Care Unit Model')
    
    # Input parameters
    number_of_NICU_cots = st.slider('Number of NICU cots', 1, 10, 3)
    number_of_HDCU_cots = st.slider('Number of HDCU cots', 1, 10, 3)
    number_of_SCBU_cots = st.slider('Number of SCBU cots', 1, 20, 12)
    sim_duration = st.slider('Simulation duration', 0, 1000, 730)
    number_of_runs = st.slider('Number of runs', 1, 50, 30)
    warm_up_duration = st.slider('Warm-up duration', 0, 500, 365)
    annual_birth_rate = st.slider('Annual birth rate', 500, 6000, 3000)
    
    # Create instance of g
    global_vars = g()

    # Modify variables of g
    global_vars.number_of_NICU_cots = number_of_NICU_cots
    global_vars.number_of_HDCU_cots = number_of_HDCU_cots
    global_vars.number_of_SCBU_cots = number_of_SCBU_cots
    global_vars.sim_duration = sim_duration
    global_vars.number_of_runs = number_of_runs
    global_vars.warm_up_duration = warm_up_duration
    global_vars.annual_birth_rate = annual_birth_rate

    # button to start simulation
    if st.button('Run Simulation'):
        for run in g.number_of_runs:
            my_NCCU_model = NCCU_Model(run)
            my_NCCU_model.run()
        # Output results
        # st.write(result)  # replace this with actual output 

if __name__ == "__main__":
    main()
