import streamlit as st
from snowflake.snowpark.context import get_active_session

DAYS_QUERY = """
SELECT REGISTRATION_DATE, NEW_USERS
FROM USER_REGISTRATION_DB.ANALYTICS.DAILY_REGISTRATIONS_TOTAL
ORDER BY REGISTRATION_DATE DESC
"""

session = get_active_session()

st.set_page_config(page_title="User Registrations", layout="centered")


def render_days() -> None:
    days = session.sql(DAYS_QUERY).to_pandas()
    days = days.rename(
        columns={
            "REGISTRATION_DATE": "Date",
            "NEW_USERS": "New users",
        }
    )

    st.subheader("Registrations per day")
    if len(days):
        st.dataframe(days)
    else:
        st.info("No registrations yet.")


st.title("User Registrations")
st.caption("One row per day. The list is written when the day ends at 00:00 UTC.")

render_days()
st.button("Refresh counts")
