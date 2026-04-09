import pandas as pd
import os
import asyncio
from datetime import datetime
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..core.database import AsyncSessionLocal
from ..core.orm_models import LeadORM

async def get_all_leads():
    """Fetch all leads from Postgres."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(LeadORM))
        leads = result.scalars().all()
        return leads

def format_lead_data(leads):
    """Convert SQLAlchemy objects to a list of dicts with pretty headers."""
    data = []
    for lead in leads:
        data.append({
            "First Name": lead.first_name,
            "Last Name": lead.last_name,
            "Phone Number": lead.phone_number,
            "State": lead.state,
            "City": lead.city,
            "Address": lead.address,
            "Disease History": lead.disease_history,
            "Source": lead.source,
            "Source URL": lead.source_url,
            "Extracted At": lead.extracted_at.strftime("%Y-%m-%d %H:%M:%S") if lead.extracted_at else ""
        })
    return data

async def export_to_formats(filename_base="health_leads_formatted"):
    """Export leads to both CSV and update the Master XLSX file."""
    print("--- Starting Lead Export ---")
    
    leads = await get_all_leads()
    if not leads:
        print("No leads found in database. Skipping export.")
        return

    df_new = pd.DataFrame(format_lead_data(leads))
    
    # 1. Export as a fresh CSV with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    csv_file = f"exports/leads_export_{timestamp}.csv"
    os.makedirs("exports", exist_ok=True)
    df_new.to_csv(csv_file, index=False)
    print(f"Saved fresh CSV export: {csv_file}")

    # 2. Update Master XLSX
    master_xlsx = f"{filename_base}.xlsx"
    if os.path.exists(master_xlsx):
        try:
            # Load existing, append new, and drop duplicates by source_id or identity (via name/phone)
            # For simplicity here, we'll overwrite the Master with the full Postgres state
            # since Postgres is our 'Source of Truth' now.
            df_new.to_excel(master_xlsx, index=False, engine='openpyxl')
            print(f"Successfully updated Master Excel file: {master_xlsx}")
        except Exception as e:
            print(f"Error updating Excel file: {e}")
    else:
        df_new.to_excel(master_xlsx, index=False, engine='openpyxl')
        print(f"Created new Master Excel file: {master_xlsx}")

    print("--- Export Complete ---")

if __name__ == "__main__":
    asyncio.run(export_to_formats())
