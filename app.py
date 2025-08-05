import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

from subitem_processor import SubitemProcessor  # your class from earlier

st.title("Monday Subitem Ratings Reporter")

# 1) File uploader
uploaded = st.file_uploader("Upload your CSV export", type="csv")
if not uploaded:
    st.stop()

# 2) Read and process
df = pd.read_csv(uploaded)
# (pass any corrections dict if you like)
proc = SubitemProcessor(df)
flat_df, counts = proc.process()  # CSVs are saved on the server if you want
# You could also skip saving and just use flat_df, counts directly

# 3) Show results
st.header("Flattened Subitems")
st.dataframe(flat_df)

st.header("Rating Counts per Subitem")
st.dataframe(counts)

# 4) Download buttons
st.download_button(
    "Download flattened CSV",
    data=flat_df.to_csv(index=False).encode("utf-8"),
    file_name="flat_subitems.csv",
    mime="text/csv"
)
st.download_button(
    "Download counts CSV",
    data=counts.to_csv().encode("utf-8"),
    file_name="subitem_rating_counts.csv",
    mime="text/csv"
)

# 5) Simple bar chart for a selected subitem
subitem = st.selectbox("Pick subitem to plot", counts.index.tolist())
vals = counts.loc[subitem]
fig, ax = plt.subplots()
vals.plot(kind="bar", ax=ax)
ax.set_title(f"Ratings for {subitem}")
ax.set_ylabel("Count")
st.pyplot(fig)
