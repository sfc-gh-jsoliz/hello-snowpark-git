# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
import snowflake.snowpark.functions as F

# Write directly to the app
st.title("Monthly Pricing App :truck:")
st.write(
    """Navigate to a food truck brand and menu item. Set the day-of-week 
    pricing for the upcoming month. Click **"Update Prices"** to 
    submit finalized pricing.
    """
)

# Get the current credentials
session = get_active_session()

# Get data and add a comment for columns
df = session.table("frostbyte_tasty_bytes_v2.analytics.pricing").with_column("comment", F.lit(""))  # recommended prices pre-calculated in 01

# Dynamic filters
brand = st.selectbox("Brand:", df.select("brand").distinct())
item = st.selectbox(
    "Item:", df.filter(F.col("brand") == brand).select("item").distinct()
)

# Provide instructions for updating pricing and using recommendtions
st.write(
    """
    View price recommendations and profit lift over current month pricing.
    Adjust **NEW_PRICE** to see the impact on demand and profit.
    """
)

# Display and get updated prices from the data editor object
set_prices = session.create_dataframe(                          # IN MEMORY MANIPULATION OF RECOMENDATION TABLE
    st.experimental_data_editor(
        df.filter((F.col("brand") == brand) & (F.col("item") == item))
    )
)

# Add a subheader
st.subheader("Forecasted Item Demand Based on Price")

# Define model input features
feature_cols = [
    "new_price",
    "base_price",
    "price_hist_dow",
    "price_year_dow",
    "price_month_dow",
    "price_change_hist_dow",
    "price_change_year_dow",
    "price_change_month_dow",
    "price_hist_roll",
    "price_year_roll",
    "price_month_roll",
    "price_change_hist_roll",
    "price_change_year_roll",
    "price_change_month_roll",
]

# Call the inference udf to get demand
df_demand = set_prices.join(
    session.table("frostbyte_tasty_bytes_v2.analytics.pricing_detail"), ["brand", "item", "day_of_week"]    # wider table to append facts
).select(
    "day_of_week",
    "current_price_demand",
    "new_price",
    "item_cost",
    "average_basket_profit",
    "current_price_profit",
    #  CALL udf to get new demand based on new price set in the in-memory set_prices table
    (F.call_udf("udf_demand_price", [F.col(c) for c in feature_cols])).alias("new_price_demand"),
)

# Demand lift
demand_lift = df_demand.select(
    F.round(
        (
            (F.sum("new_price_demand") - F.sum("current_price_demand"))
            / F.sum("current_price_demand")
        )
        * 100,
        1,
    )
).collect()[0][0]

# Profit lift
profit_lift = (
    df_demand.with_column(
        "new_price_profit",
        F.col("new_price_demand")
        * (F.col("new_price") - F.col("item_cost") + F.col("average_basket_profit")),
    )
    .select(
        F.round(
            (
                (F.sum("new_price_profit") - F.sum("current_price_profit"))
                / F.sum("current_price_profit")
            )
            * 100,
            1,
        )
    )
    .collect()[0][0]
)

# Show KPIs
col1, col2 = st.columns(2)
col1.metric("Total Weekly Demand Lift (%)", demand_lift)
col2.metric("Total Weekly Profit Lift (%)", profit_lift)

# Plot demand
st.line_chart(
    df_demand.with_column("current_price_demand", F.col("current_price_demand") * 0.97),
    x="DAY_OF_WEEK",
    y=["NEW_PRICE_DEMAND", "CURRENT_PRICE_DEMAND"],
)

# Button to submit pricing
if st.button("Update Prices"):
    set_prices.with_column("timestamp", F.current_timestamp()).write.mode(
        "append"
    ).save_as_table("frostbyte_tasty_bytes_v2.analytics.pricing_final")

# Expander to view submitted pricing
with st.expander("View Current Prices in 'pricing_final' table"):
    st.table(session.table("frostbyte_tasty_bytes_v2.analytics.pricing_final").order_by(F.col("timestamp").desc()))
