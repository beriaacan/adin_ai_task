import uvicorn
import pandas as pd
from fastapi import FastAPI, Query
from sqlalchemy import create_engine
from typing import Optional
from datetime import datetime

# Database connection details
DB_USER = "hr-task-user"
DB_PASSWORD = "adinTask2024!"
DB_HOST = "hr-task-db.cqbarc8xc1jj.us-east-1.rds.amazonaws.com"
DB_PORT = 3306
DB_NAME = "hr-task"

# SQLAlchemy connection string
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Create the FastAPI app
app = FastAPI()

# Create a DB engine
engine = create_engine(DATABASE_URL)


@app.get("/api/campaigns")
def get_campaigns(
    campaign_id: Optional[str] = Query(None, description="Campaign ID (optional)"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)")
):
    """
    Returns a JSON response that combines data from tbl_daily_campaigns and tbl_daily_scores
    in the specified format.

    If campaign_id is None, returns aggregated data for all campaigns (label: "All").
    If start_date or end_date are not provided, uses the min/max from the data in the database.
    """

    # 1. Load data from DB
    try:
        df_campaigns = pd.read_sql("SELECT * FROM tbl_daily_campaigns", con=engine)
        df_scores = pd.read_sql("SELECT * FROM tbl_daily_scores", con=engine)
    except Exception as e:
        return {
            "success": False,
            "message": f"Error reading from database: {str(e)}",
            "data": {}
        }

    # 2. Convert date columns to datetime
    df_campaigns["date"] = pd.to_datetime(df_campaigns["date"], errors="coerce")
    df_scores["date"] = pd.to_datetime(df_scores["date"], errors="coerce")

    # 3. Merge dataframes
    df_merged = pd.merge(
        df_campaigns,
        df_scores,
        on=["campaign_id", "date"],
        how="inner"
    )

    # If there's no data, return a message
    if df_merged.empty:
        return {
            "success": True,
            "message": "No data found in both tables.",
            "data": {}
        }

    # 4. Filter by campaign_id if provided
    if campaign_id:
        df_merged = df_merged[df_merged["campaign_id"] == campaign_id]

    # If no campaign found, return early
    if df_merged.empty:
        return {
            "success": True,
            "message": "No data found for the given campaign_id.",
            "data": {}
        }

    # 5. Determine date range filters
    #    If user does not provide start_date or end_date, we use the min/max from the filtered data
    actual_start = df_merged["date"].min()
    actual_end = df_merged["date"].max()

    if start_date:
        try:
            user_start = datetime.strptime(start_date, "%Y-%m-%d")
            if user_start > actual_end:
                return {
                    "success": True,
                    "message": f"No data found. start_date {start_date} is after all campaign data.",
                    "data": {}
                }
            actual_start = max(actual_start, user_start)
        except ValueError:
            return {
                "success": False,
                "message": f"Invalid start_date format: {start_date}",
                "data": {}
            }

    if end_date:
        try:
            user_end = datetime.strptime(end_date, "%Y-%m-%d")
            if user_end < actual_start:
                return {
                    "success": True,
                    "message": f"No data found. end_date {end_date} is before start_date.",
                    "data": {}
                }
            actual_end = min(actual_end, user_end)
        except ValueError:
            return {
                "success": False,
                "message": f"Invalid end_date format: {end_date}",
                "data": {}
            }

    # Now filter the merged DF by the final date range
    df_merged = df_merged[(df_merged["date"] >= actual_start) & (df_merged["date"] <= actual_end)]

    # If the final filtered data is empty, return no data
    if df_merged.empty:
        return {
            "success": True,
            "message": "No data found for the given parameters.",
            "data": {}
        }

    # -------------------------------------------------------------------------
    # PREPARE THE FINAL JSON STRUCTURE
    # -------------------------------------------------------------------------

    # =========== 1) campaignCard ===========
    # campaignName
    #   if one campaign => that campaign's name
    #   if all => "All"
    # range => ex: "20 Mar - 30 Dec"
    # days => total days in the selected range
    if campaign_id:
        # We assume there's only one campaign_id in df_merged, so we can pick the first
        campaign_name = df_merged["campaign_name"].iloc[0]
    else:
        campaign_name = "All"

    # We'll format the date range e.g. "DD MMM - DD MMM (YYYY?)"
    # But the example JSON just shows e.g. "20 Mar - 30 Dec"
    # For simplicity, let's do "DD MMM YYYY - DD MMM YYYY"
    def format_date(dt: datetime):
        return dt.strftime("%d %b %Y")

    date_range_str = f"{format_date(actual_start)} - {format_date(actual_end)}"

    # Days: difference in days
    days_count = (actual_end - actual_start).days + 1  # inclusive

    campaignCard = {
        "campaignName": campaign_name,
        "range": date_range_str,
        "days": days_count
    }

    # =========== 2) performanceMetrics ===========
    # sum of impressions, sum of clicks, sum of views for the selected date range
    total_impressions = int(df_merged["impressions"].sum())
    total_clicks = int(df_merged["clicks"].sum())
    total_views = int(df_merged["views"].sum())

    performanceMetrics = {
        "currentMetrics": {
            "impressions": total_impressions,
            "clicks": total_clicks,
            "views": total_views
        }
    }

    # =========== 3) volumeUnitCostTrend ===========
    # "volumeUnitCostTrend": {
    #       "impressionsCpm": {
    #           "impression": { "YYYY-MM-DD": impressions },
    #           "cpm":        { "YYYY-MM-DD": cpm }
    #       }
    # }
    # We'll group by date and sum impressions, and also gather the average or (?), for cpm.
    # Typically, cpm = cost per thousand impressions => we have a cpm column. We'll just take average or sum?
    # We'll assume we can take the 'mean' of cpm for that date across rows if multiple entries exist in that day.
    df_grouped_date = df_merged.groupby("date").agg(
        {
            "impressions": "sum",
            "cpm": "mean"  # or "mean", depending on your business logic
        }
    ).reset_index()

    # Sort by date ascending
    df_grouped_date = df_grouped_date.sort_values(by="date")

    impression_dict = {}
    cpm_dict = {}

    for _, row in df_grouped_date.iterrows():
        date_str = row["date"].strftime("%Y-%m-%d")
        impression_dict[date_str] = int(row["impressions"])
        cpm_dict[date_str] = float(f"{row['cpm']:.2f}")  # format to 2 decimals

    volumeUnitCostTrend = {
        "impressionsCpm": {
            "impression": impression_dict,
            "cpm": cpm_dict
        }
    }

    # =========== 4) campaignTable ===========
    # The example requires all campaigns, not just the filtered one.
    # So let's do a separate groupby for the entire dataset (no matter the campaign_id filter).
    # But we still want to show them with their overall min date, max date, and some representative
    # effectiveness_score, media_score, creative_score (e.g. average or last known).
    try:
        df_campaigns_full = pd.read_sql("SELECT * FROM tbl_daily_campaigns", con=engine)
        df_scores_full = pd.read_sql("SELECT * FROM tbl_daily_scores", con=engine)
    except Exception as e:
        return {
            "success": False,
            "message": f"Could not load data for campaignTable: {str(e)}",
            "data": {}
        }

    # Merge them
    df_full_merged = pd.merge(df_campaigns_full, df_scores_full, on=["campaign_id", "date"], how="inner")

    # We'll group by campaign_id, get min(date), max(date), average of scores (or last known).
    # For demonstration, let's do average of scores.
    df_summary = df_full_merged.groupby(["campaign_id", "campaign_name"]).agg(
        start_date=("date", "min"),
        end_date=("date", "max"),
        effectiveness=("effectiveness_score", "mean"),
        media=("media_score", "mean"),
        creative=("creative_score", "mean")
    ).reset_index()

    # Sort by campaign_id or maybe by start_date
    df_summary = df_summary.sort_values(by="start_date")

    campaignTable = {
        "start_date": [],
        "end_date": [],
        "adin_id": [],
        "campaign": [],
        "effectiveness": [],
        "media": [],
        "creative": []
    }

    for _, row in df_summary.iterrows():
        campaignTable["start_date"].append(row["start_date"].strftime("%Y-%m-%d"))
        campaignTable["end_date"].append(row["end_date"].strftime("%Y-%m-%d"))
        campaignTable["adin_id"].append(row["campaign_id"])
        campaignTable["campaign"].append(row["campaign_name"])
        # Round the scores, or convert to int if you'd prefer
        campaignTable["effectiveness"].append(int(round(row["effectiveness"], 0)))
        campaignTable["media"].append(int(round(row["media"], 0)))
        campaignTable["creative"].append(int(round(row["creative"], 0)))

    # Combine into final JSON
    final_json = {
        "campaignCard": campaignCard,
        "performanceMetrics": performanceMetrics,
        "volumeUnitCostTrend": volumeUnitCostTrend,
        "campaignTable": campaignTable
    }

    return {
        "success": True,
        "message": "Data retrieved successfully",
        "data": final_json
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
