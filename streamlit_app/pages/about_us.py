
import streamlit as st

# Page title
st.title('About Us')

# Define the layout with two columns
col1, col2 = st.columns(2)

# Team member 1
with col1:
    st.header('Eneko Eguiguren')
    st.image("./files/profil_eneko.jpeg", caption='Eneko Eguiguren', width=250)  
    st.write('**Position:** ')
    st.write('[Linkedin profile](https://www.linkedin.com/in/enekoegiguren/)') 

# Team member 2
with col2:
    st.header('Ryan Krouchi')
    st.image("./files/profil_ryan.jpg", caption='Ryan Krouchi', width=250) 
    st.write('**Position:** ')
    st.write('[Linkedin profile](https://www.linkedin.com/in/ryan-krouchi/)')  


