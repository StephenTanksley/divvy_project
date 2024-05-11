import streamlit as st
"""
MONTHLY VIEW

Statistics to calculate for monthly ridership:

1) Average daily ridership
2) Max daily ridership
3) Min daily ridership
4) Most popular stations by monthly ridership (by percentile - top 10%, 20%, etc)
5) Calculate estimated carbon savings from daily ridership.
    a) calculate physical space savings from daily ridership (using min/avg/max figures to provide contrast)
6) Explain the limitations of the data (only bikeshare data, not all cyclists in the city)

    - CO2 emissions averages for motor vehicles:
        https://www.epa.gov/greenvehicles/greenhouse-gas-emissions-typical-passenger-vehicle

    - CO2 emissions averages for bicycles and e-bikes:
        http://large.stanford.edu/courses/2022/ph240/schutt2/ (read bibliography)

    - Commute Carbon Calculator - http://transportation-forms.stanford.edu/cost/
"""