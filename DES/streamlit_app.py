import streamlit as st
from DES_NCCU_Model import NCCU_Model, g 

def main():
    st.title('SimPy Model Result Visualization')
    
    # Input parameters
    duration = st.slider('Duration', 0, 100, 50)  # replace 0, 100, 50 with your values

    # button to start simulation
    if st.button('Run Simulation'):
        result = NCCU_Model(duration)
        # Output results
        st.write(result)  # replace this with actual output 

if __name__ == "__main__":
    main()