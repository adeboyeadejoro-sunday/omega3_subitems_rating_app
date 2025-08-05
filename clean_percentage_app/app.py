import json
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

st.set_page_config(page_title="Clean & % Subitems", layout="wide")
st.title("Clean Subitem Counts & Compute Percentages")

# 1) Upload the raw counts CSV
counts_file = st.file_uploader("1) Upload your subitem_rating_counts.csv", type="csv")
if not counts_file:
    st.info("Waiting for counts CSV…")
    st.stop()
counts = pd.read_csv(counts_file, index_col=0)

# Show raw counts
st.subheader("Raw Counts")
st.dataframe(counts)

# 2) Optional corrections
st.subheader("2) (Optional) Provide typo corrections")

json_input = st.text_area(
    "Either paste a JSON map, e.g.:  {\"Actves\":\"Actives\",\"Haevy metals\":\"Heavy metals\"}",
    height=100,
)

mapping_file = st.file_uploader("Or upload a 2‑column CSV (typo,correct)", type="csv")

# Build corrections dict
corrections = {}
if json_input.strip():
    try:
        corrections = json.loads(json_input)
    except Exception as e:
        st.error(f"Invalid JSON: {e}")
        st.stop()
elif mapping_file:
    df_map = pd.read_csv(mapping_file, header=None, names=["typo", "correct"])
    corrections = dict(zip(df_map.typo.astype(str), df_map.correct.astype(str)))

# Apply corrections if provided
if corrections:
    counts = (
        counts
          .reset_index()
          .assign(subitem_name=lambda d: d.subitem_name.replace(corrections))
          .groupby("subitem_name", as_index=True)
          .sum()
    )

# 3) Show cleaned counts
st.subheader("Cleaned Counts")
st.dataframe(counts)

# 4) Compute & show percentages
st.subheader("Percentages (row‑wise)")
percentages = (
    counts
      .div(counts.sum(axis=1), axis=0)
      .multiply(100)
      .round(2)
)
st.dataframe(percentages)

# 5) Download buttons
csv_counts = counts.to_csv().encode("utf-8")
csv_pct    = percentages.to_csv().encode("utf-8")

st.download_button(
    "Download cleaned counts CSV",
    data=csv_counts,
    file_name="cleaned_subitem_counts.csv",
    mime="text/csv"
)
st.download_button(
    "Download percentages CSV",
    data=csv_pct,
    file_name="subitem_percentages.csv",
    mime="text/csv"
)

# 6) Simple bar chart per subitem
st.subheader("Plot a Subitem’s Distribution")
subitem = st.selectbox("Choose subitem", counts.index.tolist())
mode    = st.radio("Show:", ("Counts", "Percentages"))
values = counts.loc[subitem] if mode == "Counts" else percentages.loc[subitem]

fig, ax = plt.subplots()
values.plot(kind="bar", ax=ax)
ax.set_title(f"{mode} for {subitem}")
ax.set_ylabel(mode)
ax.set_ylim(0, values.max() * 1.1)
st.pyplot(fig)
