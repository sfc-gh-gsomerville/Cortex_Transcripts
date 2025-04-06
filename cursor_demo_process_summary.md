# Cursor Demo Data Setup Process

## Database and Schema Setup
1. Create `Cursor_Demo` database if it doesn't exist
2. Create `V1` schema in `Cursor_Demo` database if it doesn't exist

## Support Agent Data
3. Create `SUPPORT_AGENTS` table with auto-incrementing ID and agent name
4. Insert 8 sample agents with names

## Customer Data
5. Create `CUSTOMERS` table with auto-incrementing ID and customer name
6. Insert 100 sample customers with names

## Medical Device Data
7. Create `HOME_MEDICAL_DEVICES` table with detailed fields:
   - Device ID, name, category, subcategory
   - Prescription requirements, insurance coverage
   - Maintenance needs, lifespan, common issues, training requirements
8. Insert 50 common medical devices across categories:
   - Diabetes Management
   - Respiratory
   - Mobility Aids
   - Wound Care
   - Urology
   - Pain Management
   - Monitoring Devices
   - Nutrition & Feeding
   - Infusion Therapy
   - Orthopedic

## Support Conversations Setup
9. Create `SUPPORT_CONVERSATIONS` table to store:
   - Conversation metadata (ID, timestamps)
   - Agent and customer IDs (foreign keys)
   - Sentiment and resolution status
   - Device name and common issue
   - Full transcript
10. Insert 5 random support conversations with:
    - Random timestamps within the last 30 days
    - Random agent and customer assignments
    - Random sentiment (positive, negative, neutral)
    - Random resolution status (70% resolved, 30% unresolved)
    - Random device assignment

## Transcript Generation
11. Create `GENERATE_TRANSCRIPTS` stored procedure that:
    - Selects conversations without transcripts
    - For each conversation, builds a prompt for Claude AI
    - Uses Snowflake Cortex to generate realistic conversation transcripts
    - Updates each conversation record with the generated transcript
12. Call the procedure to generate all transcripts
13. Query the results to verify data creation

## Data Extraction for Demo
14. Create `support_conv_initial` table in `Data_Prep` schema to store:
    - Conversation ID and timestamps
    - Agent and customer names
    - Full transcript
15. Insert data from `SUPPORT_CONVERSATIONS` with agent and customer name lookups
16. Query the new table to verify data extraction

## Stage Setup and JSON Export
17. Create internal stage `Call_data_initial` in `Data_Prep` schema
18. Copy data from `support_conv_initial` table to the stage as JSON:
    - Convert each row to a JSON object with conversation_id, timestamps, names, and transcript
    - Export to `support_conv_initial.json` in the stage
    - Use OVERWRITE=TRUE to replace any existing file 