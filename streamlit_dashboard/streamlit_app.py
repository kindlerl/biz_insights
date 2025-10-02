import os
from datetime import date
import pandas as pd
import streamlit as st
import textwrap
from pyathena import connect
from dateutil.relativedelta import relativedelta

# -----------------------------
# Config — tweak if needed
# -----------------------------
ATHENA_REGION = "us-east-1"
ATHENA_S3_STAGING = "s3://bus-insights-dev-us-east-1/athena/results/"
ATHENA_WORKGROUP = "primary"  # default workgroup
DB = "biz_insights"

# If AWS_PROFILE is set in the BASH shell; boto3/PyAthena will pick it up.
# export AWS_PROFILE=biz-insights

# -----------------------------
# Helpers
# -----------------------------
@st.cache_resource
def athena_conn():
    return connect(
        s3_staging_dir=ATHENA_S3_STAGING,
        region_name=ATHENA_REGION,
        work_group=ATHENA_WORKGROUP,
    )

@st.cache_data(ttl=300, show_spinner=False)
def athena_query(sql: str):
    conn = athena_conn()
    import pandas as pd
    return pd.read_sql(sql, conn)

@st.cache_data(ttl=300, show_spinner=False)
def athena_query(sql: str) -> pd.DataFrame:
    conn = connect(
        s3_staging_dir=ATHENA_S3_STAGING,
        region_name=ATHENA_REGION,
        work_group=ATHENA_WORKGROUP,
    )
    return pd.read_sql(sql, conn)

@st.cache_data(ttl=300, show_spinner=False)
def get_date_bounds():
    # Views already restrict windows, but we’ll derive min/max for UI from sales view.
    q = f"""
        SELECT MIN(order_date) AS min_d, MAX(order_date) AS max_d
        FROM {DB}.v_sales_trends_daily
    """
    df = athena_query(q)
    return (pd.to_datetime(df["min_d"].iloc[0]).date(),
            pd.to_datetime(df["max_d"].iloc[0]).date())

@st.cache_data(ttl=300, show_spinner=False)
def get_restaurants():
    q = f"""
        SELECT DISTINCT restaurant_id
        FROM {DB}.v_sales_trends_daily
        ORDER BY 1
    """
    df = athena_query(q)
    return df["restaurant_id"].tolist()

# Helper to take an a Python list of values and return a 
# SQL-friendly list in parenthesis to be used as part of an IN clause.
# Example:
#   nums = [1, 2, 3, 4]
#   numlist = sql_in(nums)   # numlist now = "('1', '2', '3', '4')"
#   SQL = SELECT user_id WHERE user_id IN {numlist}
def sql_in(values):
    # Build a safe IN (...) list for simple ids (assume no quotes inside ids)
    quoted = [f"'{v}'" for v in values]
    return "(" + ",".join(quoted) + ")"

# -----------------------------
# UI
# -----------------------------
st.set_page_config(page_title="Business Insights", layout="wide")
st.title("Business Insights")

# ---------------------------------------------------------------
# In an effort to control the automated redraw feature that kicks
# in when a widget changes in the sidebar, create a session_state
# dictionary that holds our initial run default values.  This will
# only run one time to set the default values.
# ---------------------------------------------------------------
if "applied_filters" not in st.session_state:
    # initial defaults based on your data
    min_d, max_d = get_date_bounds()                   # cached call
    restaurants = get_restaurants()                    # cached call
    default_stores = restaurants[:3] or restaurants
    st.session_state.applied_filters = {
        "stores": default_stores,
        "start": (max_d - relativedelta(days=29)),
        "end": max_d,
        "top_n": 10,
    }

min_d, max_d = get_date_bounds()
restaurants = get_restaurants()

with st.sidebar:
    st.header("Filters")

    # A form prevents reruns until user clicks the submit button
    with st.form("filters_form", clear_on_submit=False):
        stores_input = st.multiselect(
            "Restaurants",
            restaurants,
            default=st.session_state.applied_filters["stores"],
            key="stores_input",
        )
        date_input = st.date_input(
            "Date range",
            value=(st.session_state.applied_filters["start"],
                   st.session_state.applied_filters["end"]),
            min_value=min_d, max_value=max_d, key="date_input"
        )
        top_n_input = st.slider("Top items per store (30d)", 5, 25,
                                value=st.session_state.applied_filters["top_n"], key="topn_input")

        applied = st.form_submit_button("Apply Filters")

    # --- NAV ---
    section = st.sidebar.radio(
        "Navigate",
        [
            "Operational KPIs (current)",
            "Deliverable 1: Segmentation",
            "Deliverable 2: Churn Risk",
            "Deliverable 3: Trends & Seasonality",
            "Deliverable 4: Loyalty Impact",
            "Deliverable 5: Location Performance",
            "Deliverable 6: Discounts Effectiveness",
        ],
        index=0
    )


    # If user clicked apply, freeze the selection into session_state
    if applied:
        if isinstance(date_input, tuple):
            start_d, end_d = date_input
        else:
            start_d = end_d = date_input
        st.session_state.applied_filters = {
            "stores": stores_input or [],   # allow empty to short-circuit below
            "start": start_d,
            "end": end_d,
            "top_n": top_n_input,
        }

# From here on, ALWAYS read filters from session_state.applied_filters
FILT = st.session_state.applied_filters
stores = FILT["stores"]
start_d = FILT["start"]
end_d   = FILT["end"]
top_n   = FILT["top_n"]

if not stores:
    st.info("Select at least one restaurant and click **Apply Filters**.")
    st.stop()

# Stabilize SQL for better cache hits (ordering affects the SQL string)
# Build an SQL "IN" clause that captures the filter settings and creates
# an IN clause to be used in the SQL for each visualization below.
stores_sorted = sorted(stores)
store_in = "(" + ",".join([f"'{s}'" for s in stores_sorted]) + ")"

st.markdown("""
    <style>
      .summary_block {
        ol, ul {
          margin-bottom: 0.5rem !important;
          li {
            font-size: 1.4rem !important;
            margin-bottom: 0.5rem;
          }
        }
      }
      .summary {
        font-size: 1.25rem !important;
        font-weight: 600 !important;
        margin-bottom: 1rem;
      }
      .subheader {
        font-size: 1.75rem !important;
        font-weight: 600 !important;
        border-bottom: 1px solid #FFFFFF;
        margin-bottom: 1.25rem !important;
      }
      .chart_heading {
        font-size: 1.5rem !important;
        font-weight: 600 !important;
        margin-bottom: 0.8rem;
      }
      .note {
        margin-top: 2.5rem !important;
        font-size: 1.25rem !important;
        font-style: italic !important;
        padding-left: 4.5rem;
        text-indent: -4.5rem;
        b {
            font-style: normal;
            padding-right: 0.8rem !important;
        }
      }
    </style>
    """, unsafe_allow_html=True)


if section == "Operational KPIs (current)":

    with st.spinner("Refreshing data..."):
        # run all five sections

        # ---------------------------------
        # 1) Sales trend (From the G1 view)
        # ---------------------------------
        left, right = st.columns([2, 1], gap="large")
        with left:
            q_sales = f"""
                SELECT order_date, restaurant_id, gross_sales_total, orders_count, 
                       items_count, avg_order_value, loyalty_sales_pct
                FROM {DB}.v_sales_trends_daily
                WHERE restaurant_id IN {store_in}
                  AND order_date BETWEEN DATE '{start_d}' AND DATE '{end_d}'
                ORDER BY order_date, restaurant_id
            """
            df_sales = athena_query(q_sales)
            if not df_sales.empty:
                pivot_sales = df_sales.pivot_table(index="order_date", columns="restaurant_id", values="gross_sales_total")
                st.subheader("Daily Gross Sales")
                st.line_chart(pivot_sales)

                pivot_orders = df_sales.pivot_table(index="order_date", columns="restaurant_id", values="orders_count")
                st.subheader("Daily Orders")
                st.line_chart(pivot_orders)
            else:
                st.info("No sales in the selected window/stores.")

        with right:
            if not df_sales.empty:
                # latest = df_sales[df_sales["order_date"] == df_sales["order_date"].max()]
                # kpi = latest.groupby("restaurant_id").agg(
                #     gross=("gross_sales_total", "sum"),
                #     orders=("orders_count", "sum"),
                #     aov=("avg_order_value", "mean"),
                #     loyalty_pct=("loyalty_sales_pct", "mean"),
                # ).reset_index()
                
                # # Option A1
                # # df_sales already filtered by stores/date
                # latest_per_store = (df_sales
                #     .sort_values(["restaurant_id", "order_date"])
                #     .groupby("restaurant_id", as_index=False)
                #     .tail(1)  # 1 row = the latest day for each store
                # )

                # kpi = (latest_per_store.groupby("restaurant_id")
                #        .agg(gross=("gross_sales_total","sum"),
                #             orders=("orders_count","sum"),
                #             aov=("avg_order_value","mean"),
                #             loyalty_pct=("loyalty_sales_pct","mean"))
                #        .reset_index())

                # Option A2
                # df_sales is already filtered by stores + date window
                latest_per_store = (
                    df_sales.sort_values(["restaurant_id", "order_date"])
                            .groupby("restaurant_id", as_index=False)
                            .tail(1)  # latest row per store
                )

                kpi = (latest_per_store
                       .assign(
                           aov = (latest_per_store["gross_sales_total"] /
                                  latest_per_store["orders_count"]).where(latest_per_store["orders_count"] > 0)
                       )[["restaurant_id", "order_date", "gross_sales_total", "orders_count", "aov", "loyalty_sales_pct"]]
                       .rename(columns={
                           "gross_sales_total": "gross",
                           "orders_count": "orders",
                           "loyalty_sales_pct": "loyalty_pct"
                       }))

                st.subheader("Latest Day KPI (per store)")
                st.dataframe(
                    kpi.style.format({"gross": "${:,.2f}", "aov": "${:,.2f}", "loyalty_pct": "{:.0%}"}),
                    use_container_width=True
                )
                st.caption("Each row uses that store’s own most recent day **within** the selected date range.")


                # # Option B
                # latest_date = df_sales["order_date"].max()
                # kpi_raw = df_sales[df_sales["order_date"] == latest_date]

                # kpi = (kpi_raw.set_index("restaurant_id")
                #        .reindex(sorted(stores))  # ensure all selected stores appear
                #        .rename(columns={
                #            "gross_sales_total":"gross",
                #            "orders_count":"orders",
                #            "avg_order_value":"aov",
                #            "loyalty_sales_pct":"loyalty_pct"
                #        })[["gross","orders","aov","loyalty_pct"]]
                #        .reset_index())

                # # fill sensible defaults
                # kpi["gross"]  = kpi["gross"].fillna(0)
                # kpi["orders"] = kpi["orders"].fillna(0)
                # # if orders==0, aov is undefined → leave NaN or set 0:
                # kpi.loc[kpi["orders"]==0, "aov"] = None

                st.subheader("Latest Day KPI (per store)")
                st.dataframe(kpi.style.format({"gross": "${:,.2f}", "aov": "${:,.2f}", "loyalty_pct": "{:.0%}"}), use_container_width=True)

    with st.spinner("Refreshing data..."):
        # run all five sections

        # -----------------------------
        # 2) Top items 30 days (G2)
        # -----------------------------
        st.markdown("---")
        st.subheader(f"Top {top_n} Items (Rolling 30 Days)")

        q_items = f"""
            WITH base AS (
              SELECT restaurant_id, item_category, item_name, units_30d, sales_30d, rn_store
              FROM {DB}.v_top_items_30d
              WHERE restaurant_id IN {store_in}
            )
            SELECT * FROM base WHERE rn_store <= {top_n}
            ORDER BY restaurant_id, sales_30d DESC
        """
        df_items = athena_query(q_items)
        if not df_items.empty:
            st.dataframe(
                df_items.style.format({"units_30d": "{:,.0f}", "sales_30d": "${:,.2f}"}),
                use_container_width=True,
                height=380
            )
        else:
            st.info("No item sales for the selected stores.")

    with st.spinner("Refreshing data..."):
        # run all five sections

        # -----------------------------
        # 3) Loyalty mix by day (G1)
        # -----------------------------
        st.markdown("---")
        st.subheader("Loyalty Mix by Day")

        q_loyalty = f"""
            SELECT order_date, restaurant_id, loyalty_sales, non_loyalty_sales, loyalty_orders, non_loyalty_orders, loyalty_orders_pct
            FROM {DB}.v_loyalty_mix_daily
            WHERE restaurant_id IN {store_in}
              AND order_date BETWEEN DATE '{start_d}' AND DATE '{end_d}'
            ORDER BY order_date, restaurant_id
        """
        df_loy = athena_query(q_loyalty)
        if not df_loy.empty:
            pivot_loy = df_loy.pivot_table(index="order_date", columns="restaurant_id", values="loyalty_orders_pct")
            st.line_chart(pivot_loy)
            st.caption("Share of orders from loyalty members (per store).")
        else:
            st.info("No loyalty data for the selection.")

    with st.spinner("Refreshing data..."):
        # run all five sections

        # -----------------------------
        # 4) Discounts by day (G4)
        # -----------------------------
        st.markdown("---")
        st.subheader("Discount Impact by Day")

        q_disc = f"""
            SELECT order_date, restaurant_id, pre_discount_sales, discount_amount_abs, net_sales_after_discounts,
                   pct_orders_discounted, pct_sales_discounted
            FROM {DB}.v_discounts_by_day
            WHERE restaurant_id IN {store_in}
              AND order_date BETWEEN DATE '{start_d}' AND DATE '{end_d}'
            ORDER BY order_date, restaurant_id
        """
        df_disc = athena_query(q_disc)
        if not df_disc.empty:
            # Show % of sales discounted
            pct_sales = df_disc.pivot_table(index="order_date", columns="restaurant_id", values="pct_sales_discounted")
            st.line_chart(pct_sales)
            st.caption("Percent of sales discounted (per store).")
            # Small KPI table
            latest_d = df_disc["order_date"].max()
            latest_disc = df_disc[df_disc["order_date"] == latest_d].groupby("restaurant_id").agg(
                pre=("pre_discount_sales","sum"),
                disc=("discount_amount_abs","sum"),
                net=("net_sales_after_discounts","sum"),
                pct=("pct_sales_discounted","mean")
            ).reset_index()
            st.dataframe(latest_disc.style.format({"pre":"${:,.2f}","disc":"${:,.2f}","net":"${:,.2f}","pct":"{:.1%}"}), use_container_width=True)
        else:
            st.info("No discount data for the selection.")

    with st.spinner("Refreshing data..."):
        # run all five sections

        # -----------------------------
        # 5) Customers snapshot (G3)
        # -----------------------------
        st.markdown("---")
        st.subheader("Customer Snapshot (Latest)")

        q_cust = f"""
            SELECT user_id, lifetime_orders, lifetime_gross_sales, avg_order_value,
                   last_90d_orders, last_90d_sales, recency_score, frequency_score, monetary_score, segment_label
            FROM {DB}.v_customer_facts_latest
            ORDER BY last_90d_sales DESC
            LIMIT 200
        """
        df_cust = athena_query(q_cust)
        if not df_cust.empty:
            st.dataframe(
                df_cust.style.format({"lifetime_gross_sales":"${:,.2f}","avg_order_value":"${:,.2f}","last_90d_sales":"${:,.2f}"}),
                use_container_width=True,
                height=420
            )
        else:
            st.info("No customer snapshot rows found (did G3 run?).")

# 1. Customer Segmentation (RFM + loyalty)
elif section == "Deliverable 1: Segmentation":
    # st.markdown('<p class="subheader">Customer Segmentation (RFM + Loyalty) Dashboard</p>', unsafe_allow_html=True)
    st.markdown('## Customer Segmentation (RFM + Loyalty) Dashboard')

    with st.spinner("Refreshing data..."):
        q = f"SELECT is_loyalty_member, segment_label, recency_score, frequency_score, monetary_score, last_90d_sales FROM biz_insights.v_customer_segments_latest"
        df = athena_query(q)

        # counts by segment (+ loyalty split)
        seg_counts = (df.groupby(["segment_label","is_loyalty_member"])
                        .size().reset_index(name="customers"))
        st.bar_chart(seg_counts.pivot(index="segment_label", columns="is_loyalty_member", values="customers"))

        # leaderboard (optional)
        st.caption("Top recent spenders by segment")
        top = (df.sort_values("last_90d_sales", ascending=False)
                 .head(500)[["segment_label","last_90d_sales","recency_score","frequency_score","monetary_score"]])
        st.dataframe(top.style.format({"last_90d_sales":"${:,.2f}"}), use_container_width=True)

        st.markdown("_RFM via snapshot-day quintiles; loyalty member = any loyalty orders historically._")

        # html = textwrap.dedent("""
        #     <div class="summary_block">
        #         <div class="subheader">Summary</div>
        #         <ul>
        #             <li>
        #                 Using RFM scoring on the latest snapshot, we see VIP, Standard, 
        #                 and Churn-risk as the dominant segments; the ‘High-Value but not 
        #                 recent’ segment is small to nonexistent in this cohort because 
        #                 high frequency/spend customers are also very recent, so they 
        #                 classify as VIP. Loyalty members are disproportionately represented 
        #                 in VIP (highest loyalty share), while Churn-risk contains the lowest 
        #                 share of loyalty customers.
        #             </li>
        #             <li>
        #                 VIP / High-Value customers purchase more frequently and more recently, 
        #                 with higher 90-day spend. Loyalty members are disproportionately 
        #                 represented in those top segments: the loyalty share is highest in 
        #                 VIP/High-Value and lowest in Churn-risk. This suggests loyalty membership 
        #                 is associated with stronger purchase behavior, though we treat that as 
        #                 correlation rather than proof of causation.
        #             </li>
        #         </ul>
        #     </div>
        # """)
        # st.markdown(html, unsafe_allow_html=True)
        # --- Summary ---
        q_seg = """
        WITH latest AS (
          SELECT *
          FROM biz_insights.gold_customer_facts
          WHERE snapshot_date = (SELECT max(snapshot_date) FROM biz_insights.gold_customer_facts)
        )
        SELECT
          segment_label,
          COUNT(*)                                    AS customers,
          CAST(COUNT(*) * 1.0 / SUM(COUNT(*)) OVER () AS DOUBLE) AS customer_share,
          CAST(AVG(CASE WHEN loyalty_orders > 0 THEN 1 ELSE 0 END) AS DOUBLE) AS loyalty_share,
          CAST(AVG(lifetime_gross_sales) AS DECIMAL(14,2))        AS avg_clv,
          CAST(SUM(COALESCE(last_90d_sales,0)) AS DECIMAL(14,2))  AS sales_90d
        FROM latest
        GROUP BY segment_label
        ORDER BY customers DESC
        """
        seg = athena_query(q_seg)

        st.markdown("### Summary")
        if seg.empty:
            st.info("No customer segments found for the latest snapshot.")
        else:
            # totals
            total_customers = int(seg["customers"].sum())
            total_sales_90d = float(seg["sales_90d"].sum()) or 0.0

            # nice rows for bullets
            rows = []
            for _, r in seg.iterrows():
                seg_name = r["segment_label"]
                share    = float(r["customer_share"] or 0.0) * 100
                loy      = float(r["loyalty_share"] or 0.0) * 100
                rows.append(f"**{seg_name}**: {share:.1f}% of customers; loyalty share ~{loy:.1f}%.")

            # top contributor to recent sales
            if total_sales_90d > 0:
                seg["sales_share"] = seg["sales_90d"].astype(float) / total_sales_90d
                top_row = seg.sort_values("sales_share", ascending=False).iloc[0]
                top_line = (f"Recent revenue concentration: **{top_row['segment_label']}** contributes "
                            f"~{float(top_row['sales_share'])*100:.0f}% of last-90d sales.")
            else:
                top_line = "Recent revenue concentration: last-90d sales are ~$0 across segments."

            # print
            st.markdown(
                "- " + "\n- ".join(rows) + "\n"
                f"- {top_line}"
            )



# 2. Churn Risk Indicators
elif section == "Deliverable 2: Churn Risk":
    # st.markdown('<p class="subheader">Churn Risk Indicators Dashboard</p>', unsafe_allow_html=True)
    st.markdown('## Churn Risk Indicators Dashboard')

    with st.spinner("Refreshing data..."):
        q = """
        SELECT 
            user_id, 
            risk_level, 
            days_since_last_order, 
            avg_interorder_gap_days, 
            last_90d_orders, 
            last_90d_sales, 
            delta_90d_pct 
        FROM 
            biz_insights.v_churn_risk_latest
        """
        df = athena_query(q)

        st.bar_chart(df["risk_level"].value_counts())

        risky = (df[df["risk_level"]=="High"]
                 .sort_values(["days_since_last_order","delta_90d_pct"], ascending=[False, True])
                 .head(200))
        st.dataframe(risky, use_container_width=True)
        st.markdown("_High = recency_score ≤ 2 or DSL >45 or (0 orders in 90d but spent in prev 90d) or ≥ 50% drop._")

    # --- SUMMARY ---
    with st.spinner("Refreshing data..."):
        # Medians for recency and cadence
        q_meds = """
        SELECT 
            COUNT(*) AS customers,
            risk_level,
            approx_percentile(days_since_last_order, 0.5) AS p50_days_since_last,
            approx_percentile(avg_interorder_gap_days, 0.5) AS p50_gap_days,
            approx_percentile(last_90d_orders, 0.5) AS p50_orders_90d,
            approx_percentile(last_90d_sales, 0.5) AS p50_sales_90d,
            approx_percentile(delta_90d_pct, 0.5) AS p50_delta_90d
        FROM 
            biz_insights.v_churn_risk_latest
        GROUP BY 
            risk_level
        """
        meds_raw = athena_query(q_meds)

        # Recent activity by bucket (active share + means)
        q_act = """
        SELECT
            risk_level,
            CAST(AVG(CASE WHEN COALESCE(last_90d_orders,0) > 0 THEN 1 ELSE 0 END) AS DOUBLE) AS active_share_90d,
            AVG(COALESCE(last_90d_orders,0)) AS mean_orders_90d,
            CAST(AVG(COALESCE(last_90d_sales,0.0)) AS DECIMAL(14,2)) AS mean_sales_90d
        FROM 
            biz_insights.v_churn_risk_latest
        GROUP BY 
            risk_level
        """
        act = athena_query(q_act)

        # if meds_raw.empty:
        #     st.info("No churn-risk data in the latest snapshot.")
        # else:
        #     # Ensure one row per risk_level (take the first if duplicates exist)
        #     meds = (meds_raw
        #             .groupby("risk_level", as_index=False)
        #             .first()
        #             .set_index("risk_level"))
            
        #     # Handle any nulls - turn them to zeros.
        #     meds = meds.fillna({"p50_orders_90d": 0, "p50_sales_90d": 0.0})

        #     # Helper to pull a scalar safely (handles missing levels and accidental duplicates)
        #     def pick(level: str, col: str):
        #         if level not in meds.index:
        #             return None
        #         val = meds.loc[level, col]
        #         if isinstance(val, pd.Series):
        #             val = val.iloc[0]
        #         return float(val) if val is not None else None

        #     H_recency = pick("High",   "p50_days_since_last")
        #     L_recency = pick("Low",    "p50_days_since_last")
        #     H_gap     = pick("High",   "p50_gap_days")
        #     L_gap     = pick("Low",    "p50_gap_days")
        #     H_ord     = pick("High",   "p50_orders_90d")
        #     L_ord     = pick("Low",    "p50_orders_90d")
        #     H_sales   = pick("High",   "p50_sales_90d")
        #     L_sales   = pick("Low",    "p50_sales_90d")
        #     H_delta   = pick("High",   "p50_delta_90d")
        #     L_delta   = pick("Low",    "p50_delta_90d")

        #     # Build the one-line summary (guard if any are None)
        #     def fmt(v, kind="num"):
        #         if v is None: return "—"
        #         if kind=="days":  return f"{int(round(v))}d"
        #         if kind=="money": return f"${v:,.0f}"
        #         if kind=="pct":   return f"{v*100:.0f}%"
        #         return f"{v:.0f}"
        #     st.markdown(
        #         '<p class="summary">'
        #         "**Recency:** High ≈ "
        #         f"{fmt(H_recency,'days')} vs Low ≈ {fmt(L_recency,'days')}"
        #         '<br>'
        #         "**Gap:** High ≈ "
        #         f"{fmt(H_gap,'days')} vs Low ≈ {fmt(L_gap,'days')}"
        #         '<br>'
        #         "**90d Orders:** High ≈ "
        #         f"{fmt(H_ord)} vs Low ≈ {fmt(L_ord)}"
        #         '<br>'
        #         "**90d Sales:** High ≈ "
        #         f"{fmt(H_sales,'money')} vs Low ≈ {fmt(L_sales,'money')}"
        #         '<br>'
        #         "**Trend Δ90d:** High ≈ "
        #         f"{fmt(H_delta,'pct')} vs Low ≈ {fmt(L_delta,'pct')}"
        #         '</p>'
        #         , unsafe_allow_html=True
        #     )
        st.markdown("### Summary")

        if meds_raw.empty or act.empty:
            st.info("Not enough data to summarize churn risk.")
        else:
            # make risk_level the row index so we can address cells by label+column.
            meds_raw_i  = meds_raw.set_index("risk_level")
            act_i = act.set_index("risk_level")

            # It's possible that the index could have duplicates. If so, then 
            # df.loc[lvl,col] would return a series which would have an "iloc"
            # attribute.  If that's the case, we only need the first one in the 
            # series with "v.iloc[0]" and cast it as a float.  Otherwise, 
            # no series, so just cast the return value as a float.  If there's
            # an error, return None
            def g(df, lvl, col, default=None):
                try:
                    v = df.loc[lvl, col]
                    return float(v.iloc[0]) if hasattr(v, "iloc") else float(v)
                except Exception:
                    return default

            # High vs Low comparisons
            H_rec = g(meds_raw_i, "High", "p50_days_since_last")
            L_rec = g(meds_raw_i, "Low",  "p50_days_since_last")
            H_gap = g(meds_raw_i, "High", "p50_gap_days")
            L_gap = g(meds_raw_i, "Low",  "p50_gap_days")

            H_act = g(act_i, "High","active_share_90d"); L_act = g(act_i, "Low","active_share_90d")
            H_mo  = g(act_i, "High","mean_orders_90d");  L_mo  = g(act_i, "Low","mean_orders_90d")
            H_ms  = g(act_i, "High","mean_sales_90d");   L_ms  = g(act_i, "Low","mean_sales_90d")

            n_high = int(g(meds_raw_i, "High", "customers", 0))
            n_med  = int(g(meds_raw_i, "Medium","customers", 0))
            n_low  = int(g(meds_raw_i, "Low",   "customers", 0))

            # bullets (round + guard None)
            def fmt(x, unit="", nd=0):
                return ("—" if x is None else f"{round(x, nd)}{unit}")

            st.markdown(
                f"- **Recency:** High-risk median = **{fmt(H_rec,'d')}**, Low-risk = **{fmt(L_rec,'d')}**; "
                f"stale recency correlates with risk.\n"
                f"- **Cadence:** High vs Low median inter-order gap ≈ **{fmt(H_gap,'d')}** vs **{fmt(L_gap,'d')}**.\n"
                f"- **Activity (90d):** Active share **{fmt((H_act or 0)*100,'%',0)}** vs **{fmt((L_act or 0)*100,'%',0)}**; "
                f"mean sales **&#36;{fmt(H_ms, nd=0)}** vs **&#36;{fmt(L_ms, nd=0)}**.\n"
                f"- **Cohort sizes:** High = **{n_high:,}**, Medium = **{n_med:,}**, Low = **{n_low:,}**."
            )


# 3. Sales Trends & Sessonality
elif section == "Deliverable 3: Trends & Seasonality":
    st.markdown('<p class="subheader">Sales Trends and Seasonality Dashboard</p>', unsafe_allow_html=True)

    # --- Monthly trend ---
    with st.spinner("Refreshing data..."):
        q_month = f"""
          SELECT restaurant_id, month, gross_month, mom_pct
          FROM biz_insights.v_sales_monthly_store_12m
          WHERE restaurant_id IN {store_in}
          ORDER BY restaurant_id, month
        """
        m = athena_query(q_month)
        if not m.empty:
            # st.markdown("**Gross Monthly Sales by Store (12M)**")
            st.markdown('<div class="chart_heading">Gross Monthly Sales by Store (12M)</div>', unsafe_allow_html=True)

            st.line_chart(m.pivot_table(index="month", columns="restaurant_id", values="gross_month"))
            # last 3 MoM% per store
            last3 = (m.sort_values(["restaurant_id","month"])
                       .groupby("restaurant_id").tail(3)
                       .assign(mom_pct=lambda df: (df["mom_pct"]*100).round(1)))
            # st.caption("Last 3 MoM% by store")
            st.markdown('<div class="chart_heading">Last 3 Month-over-Month% by store</div>', unsafe_allow_html=True)
            st.dataframe(last3[["restaurant_id","month","mom_pct"]], use_container_width=True)

    # --- Day-of-week seasonality ---
    with st.spinner("Refreshing data..."):
        q_dow = f"""
          SELECT restaurant_id, day_of_week, avg_sales_dow
          FROM biz_insights.v_sales_dow_store_12m
          WHERE restaurant_id IN {store_in}
        """
        d = athena_query(q_dow)
        if not d.empty:
            # order Mon..Sun
            order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
            d["day_of_week"] = pd.Categorical(d["day_of_week"], order, ordered=True)
            # st.markdown("**Average Sales by Day of Week (12M)**")
            st.markdown('<div class="chart_heading">Average Sales by Day of Week (12M)</div>', unsafe_allow_html=True)

            st.bar_chart(d.pivot_table(index="day_of_week", columns="restaurant_id", values="avg_sales_dow").sort_index())

    # --- Category mix (share) ---
    with st.spinner("Refreshing data..."):
        q_mix = f"""
          SELECT restaurant_id, month, item_category, sales_share
          FROM biz_insights.v_category_mix_monthly_12m
          WHERE restaurant_id IN {store_in}
          ORDER BY restaurant_id, month, sales_share DESC
        """
        mix = athena_query(q_mix)
        if not mix.empty:
            # st.markdown("**Category Mix Share by Month (12M)**")
            st.markdown('<div class="chart_heading">Category Mix Share by Month (12M)</div>', unsafe_allow_html=True)
            # stacked area: sum share by month/category across selected stores
            mix_agg = (mix.groupby(["month","item_category"])["sales_share"].mean().reset_index())
            area = mix_agg.pivot_table(index="month", columns="item_category", values="sales_share").fillna(0)
            st.area_chart(area)

    # --- Holiday uplift ---
    with st.spinner("Refreshing data..."):
        q_hol = f"""
          SELECT restaurant_id, holiday_name, holiday_sales, baseline_sales, uplift_pct
          FROM biz_insights.v_holiday_uplift_12m
          WHERE restaurant_id IN {store_in}
          ORDER BY restaurant_id, holiday_name
        """
        hol = athena_query(q_hol)
        if not hol.empty:
            # st.markdown("**Holiday Uplift vs Baseline (12M)**")
            st.markdown('<div class="chart_heading">Holiday Uplift vs Baseline (12M)</div>', unsafe_allow_html=True)
            st.dataframe(hol.assign(uplift=lambda df: (df["uplift_pct"]*100).round(0))
                            .rename(columns={"uplift":"uplift_%"}),
                         use_container_width=True)
    # --- Summary Block ---
    with st.spinner("Refreshing data..."):
        st.markdown("### Summary")

        # ---------- Monthly trend ----------
        # m columns: restaurant_id, month, gross_month, mom_pct
        if not m.empty:
            m_sorted = m.sort_values(["restaurant_id","month"])
            first_last = (m_sorted.groupby("restaurant_id")
                          .agg(first_month=("gross_month","first"),
                               last_month=("gross_month","last"))
                          .assign(pct_change_12m=lambda df: (df["last_month"]-df["first_month"])
                                                          / df["first_month"].where(df["first_month"]!=0)))
            # last 3 MoM average per store
            m_sorted["rn"] = m_sorted.groupby("restaurant_id")["month"].rank(method="first", ascending=False)
            last3 = (m_sorted[m_sorted["rn"]<=3]
                     .groupby("restaurant_id")["mom_pct"].mean()
                     .to_frame("avg_mom_last3"))
            trend = first_last.join(last3, how="left").reset_index()

            # Pick the strongest mover by 12M change
            best = trend.sort_values("pct_change_12m", ascending=False).iloc[0]
            store_a = best["restaurant_id"]
            change_12m = (best["pct_change_12m"]*100) if pd.notna(best["pct_change_12m"]) else None
            last3_mom = (best["avg_mom_last3"]*100) if pd.notna(best["avg_mom_last3"]) else None

            # Direction words
            def dir_word(pct):
                if pct is None: return "mixed"
                return "rising" if pct > 5 else ("declining" if pct < -5 else "flat")

            st.markdown(f"- **Monthly trend:** Over the last 12 months, store **{store_a}** is **{dir_word(change_12m)}** "
                     f"(~{0 if change_12m is None else round(change_12m):,}% overall);<br>"
                     "Its last 3 MoM% average is "
                     f"~{0 if last3_mom is None else round(last3_mom):,}%.<br>"
                     "Other selected stores are "
                     f"{', '.join(dir_word(p) for p in trend.sort_values('restaurant_id')['pct_change_12m']*100)}.")

        # ---------- Day-of-week seasonality ----------
        # d columns: restaurant_id, day_of_week, avg_sales_dow
        if not d.empty:
            # Collapse across selected stores so we speak generally
            dow = (d.groupby("day_of_week")["avg_sales_dow"].mean().sort_values(ascending=False))
            top_day = dow.index[0]; bot_day = dow.index[-1]
            st.write(f"- **Weekly seasonality:** Across selected stores, **{top_day}** and "
                     f"**{dow.index[1]}** are highest; **{bot_day}** is lowest on average.")

        # ---------- Category mix ----------
        # mix columns: restaurant_id, month, item_category, sales_share
        if not mix.empty:
            # Average share across selected stores and months
            cat = (mix.groupby("item_category")["sales_share"].mean()
                      .sort_values(ascending=False).to_frame("avg_share"))
            top_cat, top_share = cat.index[0], cat.iloc[0,0]

            # Change in share: last 3 months vs first 3 months
            # (aggregate across stores to keep it simple)
            mix_sorted = mix.sort_values("month")
            first3 = (mix_sorted.groupby("item_category").head(3).groupby("item_category")["sales_share"].mean())
            last3  = (mix_sorted.groupby("item_category").tail(3).groupby("item_category")["sales_share"].mean())
            change = (last3 - first3).reindex(cat.index)
            trend_word = lambda v: "rising" if v>0.01 else ("falling" if v<-0.01 else "flat")
            top_change = change.loc[top_cat] if top_cat in change.index else 0.0

            st.write(f"- **Category mix:** **{top_cat}** averages ~{round(top_share*100)}% of monthly sales and is "
                     f"**{trend_word(top_change)}** vs the start of the period "
                     f"({('+' if top_change>=0 else '')+str(round(top_change*100))} pp).")

        # ---------- Holiday uplift ----------
        # hol columns: restaurant_id, holiday_name, holiday_sales, baseline_sales, uplift_pct
        if not hol.empty:
            hol2 = hol.copy()
            # hol2 = hol2[hol2["baseline_sales"]>=100]  # ignore tiny baselines
            if not hol2.empty:
                top = hol2.sort_values("uplift_pct", ascending=False).iloc[0]
                top_holiday_sales = f"${top['holiday_sales']:,.0f}"
                top_baseline_sales = f"${top['baseline_sales']:,.0f}"
                st.write(f"- **Holiday spikes:** **{top['holiday_name']}** shows the largest uplift at "
                         f"~{round(top['uplift_pct']*100):,}% vs the typical same weekday baseline "
                         f"(holiday {top_holiday_sales} vs baseline {top_baseline_sales}).")
                # str_out = f"""
                #     <b>Holiday spikes:</b> <b>{top['holiday_name']}</b> 
                #     shows the largest uplift at ~{round(top['uplift_pct']*100)}%) 
                #     vs the typical same weekday baseline (holiday {top_holiday_sales})
                #     vs baseline {top_baseline_sales})).
                # """
                # st.markdown('<div>' + str_out + '</div', unsafe_allow_html=True)

# 4. Loyalty Program Impact
elif section == "Deliverable 4: Loyalty Impact":
    # st.subheader("Loyalty vs Non-Loyalty (Latest Snapshot)")

    # with st.spinner("Refreshing data..."):
    #     q = "SELECT * FROM biz_insights.v_loyalty_vs_nonloyalty"
    #     df = athena_query(q).rename(columns={"is_loyalty_member":"loyal"})
    #     st.dataframe(df.style.format({"avg_clv":"${:,.2f}","avg_aov":"${:,.2f}","avg_sales_90d":"${:,.2f}"}), use_container_width=True)
    #     st.caption("Compare CLV / AOV / repeat-rate; loyalty should generally be higher if the program is effective.")
    st.subheader("Loyalty vs Non-Loyalty (Latest Snapshot)")

    q = """
        WITH latest AS (
          SELECT CAST(MAX(CAST(snapshot_date AS DATE)) AS DATE) AS max_snap
          FROM biz_insights.gold_customer_facts
        ),
        c AS (
          SELECT
            (loyalty_orders > 0)            AS is_loyalty_member,
            lifetime_gross_sales,
            avg_order_value,
            lifetime_orders,
            last_90d_orders_count           AS last_90d_orders,  -- alias here
            last_90d_sales
          FROM biz_insights.gold_customer_facts g
          CROSS JOIN latest
          WHERE CAST(g.snapshot_date AS DATE) = latest.max_snap
        )
        SELECT
          is_loyalty_member,
          COUNT(*)                                           AS customers,
          CAST(AVG(lifetime_gross_sales) AS DECIMAL(14,2))   AS avg_clv,
          CAST(AVG(avg_order_value)     AS DECIMAL(14,2))    AS avg_aov,
          CAST(AVG(CASE WHEN lifetime_orders >= 2 THEN 1 ELSE 0 END) AS DOUBLE) AS repeat_rate,
          CAST(AVG(COALESCE(last_90d_sales, 0.0)) AS DECIMAL(14,2))  AS mean_sales_90d,
          CAST(SUM(COALESCE(last_90d_sales, 0.0)) AS DECIMAL(14,2))  AS total_sales_90d
        FROM c
        GROUP BY is_loyalty_member
        ORDER BY is_loyalty_member DESC;
    """
    # CAST(AVG(COALESCE(last_90d_orders,0)) AS INTEGER) AS mean_orders_90d,

    df = athena_query(q)
    st.dataframe(df.rename(columns={"is_loyalty_member":"loyal"}).style.format({
        "avg_clv":"${:,.2f}","avg_aov":"${:,.2f}",
        "mean_sales_90d":"${:,.2f}","total_sales_90d":"${:,.2f}",
        "repeat_rate":"{:.0%}"
    }), use_container_width=True)

    # Quick effect sizes
    st.markdown("### Summary")

    t = df.set_index("is_loyalty_member")  # True/False rows
    if {True, False}.issubset(t.index):
        clv_uplift = (float(t.loc[True,"avg_clv"]) / float(t.loc[False,"avg_clv"]) - 1.0) * 100.0
        aov_uplift = (float(t.loc[True,"avg_aov"]) / float(t.loc[False,"avg_aov"]) - 1.0) * 100.0
        rep_pp     = (float(t.loc[True,"repeat_rate"]) - float(t.loc[False,"repeat_rate"])) * 100.0
        cust_share = float(t.loc[True,"customers"]) / float(t["customers"].sum()) * 100.0
        sales_share= float(t.loc[True,"total_sales_90d"]) / float(t["total_sales_90d"].sum()) * 100.0

        st.write (
          f"- Loyalty customers show **~{clv_uplift:.0f}% higher CLV** and "
          f"**~{aov_uplift:.0f}% higher AOV**, with a **repeat rate higher by ~{rep_pp:.0f} percentage points**."
        )
        st.write(
          f"- They’re ~{cust_share:.0f}% of customers but drive ~{sales_share:.0f}% of recent sales."
        )


        # str_out = f"""
        #     <div class="summary_block">
        #         <div class="subheader">Summary</div>
        #         <ol>
        #             <li>Loyalty customers show <b>~ {clv_uplift:.0f}% higher CLV</b> and
        #                 <b>~ {aov_uplift:.0f}% higher AOV</b>, with a <b>repeat rate higher by ~ {rep_pp:.0f} percentage points</b>.
        #             </li>
        #             <li>
        #                 Loyalty customers are only ~ {cust_share:.0f}% of customers but drive ~ {sales_share:.0f}% of recent sales.
        #             </li>
        #         </ol>
        #     </div>
        # """
        # st.markdown(str_out, unsafe_allow_html=True)
    # st.caption("Correlation, not causation: heavy users are likelier to join loyalty; true causal lift needs cohort/experiment design.")


# 5. Location Performance
elif section == "Deliverable 5: Location Performance":
    st.subheader("Store Ranking (Last 90 Days)")

    with st.spinner("Refreshing data..."):
        #     q = f"SELECT * FROM biz_insights.v_location_performance_90d"
        #     df = athena_query(q)
        #     if stores:
        #         df = df[df["restaurant_id"].isin(stores)]
        #     rank = df.sort_values("gross_90d", ascending=False)
        #     st.dataframe(rank.style.format({"gross_90d":"${:,.2f}","aov_90d":"${:,.2f}","repeat_rate_90d":"{:.0%}"}), use_container_width=True)
        q = "SELECT * FROM biz_insights.v_location_actions_90d"
        perf = athena_query(q)
        if stores:
            perf = perf[perf["restaurant_id"].isin(stores)]

        rank = perf.sort_values("gross_90d", ascending=False)
        st.dataframe(rank[[
            "restaurant_id","gross_90d","orders_90d","aov_90d",
            "customers_90d","repeat_rate_90d","loyalty_order_share","last_mom_pct"
        ]].style.format({
            "gross_90d":"${:,.0f}","aov_90d":"${:,.2f}",
            "repeat_rate_90d":"{:.0%}","loyalty_order_share":"{:.0%}",
            "last_mom_pct":"{:.0%}"
        }), use_container_width=True)

        # --- SUMMARY
        st.markdown("### Summary")

        top = rank.iloc[0]
        # st.write(
        #     f"Top store **{top['restaurant_id']}** did **${top['gross_90d']:,.0f}** in the last 90 days "
        #     f"with **AOV ${top['aov_90d']:,.2f}**, **repeat {top['repeat_rate_90d']:.0%}**, "
        #     f"and **loyalty share {top['loyalty_order_share']:.0%}**. "
        #     f"{'Actions: ' + top['recommendations'] if isinstance(top['recommendations'], str) and top['recommendations'] else ''}"
        # )
        st.markdown(f"""
        * Top store: **{top['restaurant_id']}**
          * Gross Sales = ${top['gross_90d']:,.0f} in the last 90 days
          * Average Order Value (AOV) = ${top['aov_90d']:,.2f}
          * Repeat Customers = {top['repeat_rate_90d']:.0%}
          * Loyalty Customers Share = {top['loyalty_order_share']:.0%}
        """)
        st.write("")
        st.markdown(f"""
        **NOTE:** The most recent month shown is down ~{-1*top['last_mom_pct']:.0%} after a 
        big peak the previous month.  Monitor momentum and consider localized offers to 
        increase repeat customers.
        """)
        # {'<p>Actions: ' + top['recommendations'] if isinstance(top['recommendations'], str) and top['recommendations'] + '</p>' else ''}


# 6. Discounts & Pricing Effectiveness
elif section == "Deliverable 6: Discounts Effectiveness":
    st.subheader("Discount Impact (Last 12 Months)")

    with st.spinner("Refreshing data..."):
        overall = athena_query("SELECT * FROM biz_insights.v_discount_summary_overall_12m")
        by_store = athena_query("SELECT * FROM biz_insights.v_discount_summary_store_12m")

        if not overall.empty:
            o = overall.iloc[0]
            st.metric("Orders (12m)", f"{int(o['orders_12m']):,}")
            cols = st.columns(3)
            cols[0].metric("Discounted order share", f"{o['discounted_order_share']*100:.0f}%")
            cols[1].metric("Discount rate (of gross)", f"{o['discount_rate']*100:.0f}%")
            cols[2].metric("AOV (gross → net)", f"${o['aov_gross']:,.2f} → ${o['aov_net']:,.2f}")

            st.markdown("**Revenue Mix** (gross vs discounts vs net)")
            st.bar_chart(
                pd.DataFrame({
                    "gross": [float(o["gross_12m"])],
                    "discounts": [float(o["discount_total_12m"])],
                    "net": [float(o["net_12m"])]
                }, index=["Last 12m"])
            )

        if not by_store.empty:
            if stores:  # reuse your store picker
                by_store = by_store[by_store["restaurant_id"].isin(stores)]
            rank = by_store.sort_values("net_12m", ascending=False)
            st.markdown("**By Store (ranked by net revenue)**")
            st.dataframe(rank[[
                "restaurant_id","orders_12m","discounted_orders_12m",
                "gross_12m","discount_total_12m","net_12m",
                "discount_rate","discounted_order_share","aov_gross","aov_net"
            ]].style.format({
                "gross_12m":"${:,.0f}","discount_total_12m":"${:,.0f}","net_12m":"${:,.0f}",
                "discount_rate":"{:.0%}","discounted_order_share":"{:.0%}",
                "aov_gross":"${:,.2f}","aov_net":"${:,.2f}"
            }), use_container_width=True)

            st.markdown("**Top Stores: Net vs Gross (bars)**")
            topn = rank.head(10).set_index("restaurant_id")
            st.bar_chart(topn[["gross_12m","net_12m"]])

        # --- SUMMARY
        st.markdown("### Summary")

        # =================================
        if not overall.empty:
            o = overall.iloc[0]
            if float(o["discounted_order_share"]) == 0 and float(o["discount_rate"]) == 0:
                st.markdown("""
                    * No discounted transactions detected in the analyzed window; net equals gross.
                    * Discount effectiveness cannot be evaluated with the current dataset.
                """)
            else:
                discounted_order_share = f"{o['discounted_order_share']*100:.0f}%"
                discount_rate = f"{o['discount_rate']*100:.0f}%"
                aov_gross = f"${o['aov_gross']:,.2f}"
                aov_net = f"${o['aov_net']:,.2f}"
                gross_12m = float(o["gross_12m"])
                discount_total_12m = float(o["discount_total_12m"])
                net_12m = float(o["net_12m"])
                top_rank_restaurant = rank.iloc[0,0]
                second_rank_restaurant = rank.iloc[1,0] if len(rank) > 1 else '--unavailable'

                st.markdown(f"""
                * Overall: In the last 12m, {discounted_order_share} of orders included a discount
                * Discounts averaged {discount_rate} of gross, reducing AOV from {aov_gross} to {aov_net}
                * Net revenue was {net_12m}, after {discount_total_12m} in discounts.
                * By Store:
                  * Top stores by net revenue
                    - {top_rank_restaurant}
                    - {second_rank_restaurant}
                * Stores with high discount share but flat net may be over-discounting
                * Stores with low discount share and strong net indicate pricing headroom
                """)


