import pandas as pd
import numpy as np
import json # To handle potential JSON strings in 'attendees' column

# --- Configuration ---
RAW_DATA_FILE = 'raw_data.xlsx - Sheet1.csv'
OUTPUT_EXCEL_FILE = 'star_schema_output.xlsx'

# --- 1. Load Raw Data ---
print(f"Loading raw data from '{RAW_DATA_FILE}'...")
try:
    raw_df = pd.read_csv(RAW_DATA_FILE)
    print("Raw data loaded successfully. Shape:", raw_df.shape)
    print("\nRaw data columns:")
    print(raw_df.columns.tolist())
    print("\nRaw data head:")
    print(raw_df.head())
except FileNotFoundError:
    print(f"Error: '{RAW_DATA_FILE}' not found. Please ensure the file is in the correct directory.")
    print("Exiting. Cannot proceed without raw data.")
    exit()

# --- 2. Data Preprocessing and Cleaning ---
# Standardize column names (optional, but good practice for consistency)
raw_df.columns = raw_df.columns.str.lower().str.strip().str.replace(' ', '_')

# Rename 'event_id' to 'comm_id' as per fact_communication table
if 'event_id' in raw_df.columns:
    raw_df.rename(columns={'event_id': 'comm_id'}, inplace=True)

# Ensure 'comm_id' is unique for the fact table (if not, we need to handle duplicates)
if not raw_df['comm_id'].is_unique:
    print("\nWarning: 'comm_id' in raw data is not unique. This might indicate duplicate events or nested data.")
    print("Proceeding by taking the first occurrence for unique comm_id, but review is recommended.")
    raw_df = raw_df.drop_duplicates(subset=['comm_id'], keep='first')

# Convert datetime columns if they exist and are strings
datetime_cols = ['created_at', 'updated_at', 'start_time', 'end_time']
for col in datetime_cols:
    if col in raw_df.columns:
        # Using errors='coerce' will turn unparseable dates into NaT (Not a Time)
        raw_df[col] = pd.to_datetime(raw_df[col], errors='coerce')
        # Fill NaT if necessary, e.g., with a default datetime or by dropping rows
        raw_df[col] = raw_df[col].fillna(pd.NaT) # Keep NaT for now, handle later if needed

print("\nRaw data after initial cleaning and column renaming (head):")
print(raw_df.head())


# --- 3. Create Dimension Tables ---

# --- dim_comm_type ---
print("\nCreating dim_comm_type...")
if 'event_type' in raw_df.columns:
    dim_comm_type = raw_df[['event_type']].drop_duplicates().reset_index(drop=True)
    dim_comm_type['comm_type_id'] = range(1, len(dim_comm_type) + 1) # Surrogate key
    dim_comm_type.rename(columns={'event_type': 'comm_type'}, inplace=True)
    # Create a mapping for fact table
    comm_type_mapping = dim_comm_type.set_index('comm_type')['comm_type_id'].to_dict()
else:
    print("Warning: 'event_type' column not found for dim_comm_type. Creating dummy dim_comm_type.")
    dim_comm_type = pd.DataFrame({
        'comm_type_id': [1, 2],
        'comm_type': ['Meeting', 'Call']
    })
    comm_type_mapping = dim_comm_type.set_index('comm_type')['comm_type_id'].to_dict()

print("dim_comm_type:\n", dim_comm_type.head())


# --- dim_subject ---
print("\nCreating dim_subject...")
if 'event_title' in raw_df.columns:
    dim_subject = raw_df[['event_title']].drop_duplicates().reset_index(drop=True)
    dim_subject['subject_id'] = range(1, len(dim_subject) + 1) # Surrogate key
    dim_subject.rename(columns={'event_title': 'subject'}, inplace=True)
    # Create a mapping for fact table
    subject_mapping = dim_subject.set_index('subject')['subject_id'].to_dict()
else:
    print("Warning: 'event_title' column not found for dim_subject. Creating dummy dim_subject.")
    dim_subject = pd.DataFrame({
        'subject_id': [1, 2],
        'subject': ['Project Review', 'Team Sync']
    })
    subject_mapping = dim_subject.set_index('subject')['subject_id'].to_dict()

print("dim_subject:\n", dim_subject.head())


# --- dim_user & bridge_comm_user (most complex part due to nested users) ---
print("\nCreating dim_user and bridge_comm_user...")
all_users = []
comm_user_relations = []
user_id_counter = 1
user_email_to_id = {} # To store unique users and their surrogate IDs

# Helper function to process user data
def process_user_data(comm_id, user_list_str, role_type):
    if pd.isna(user_list_str) or user_list_str == '[]':
        return []
    try:
        # The 'attendees' column might be a string representation of a list of dictionaries
        users_raw = json.loads(user_list_str)
        if not isinstance(users_raw, list): # Handle cases where it's a single dict or malformed
            users_raw = [users_raw]
    except json.JSONDecodeError:
        # If it's not a valid JSON string (e.g., just an email or plain string)
        # Try to treat it as a single email string
        users_raw = [{'email': user_list_str, 'name': user_list_str}] # Make it a list of dicts
    except TypeError: # If it's already a list/dict object (not string)
        if not isinstance(user_list_str, list):
            users_raw = [user_list_str] # Ensure it's a list for iteration

    processed_users = []
    for user_info in users_raw:
        # Ensure user_info is a dictionary
        if not isinstance(user_info, dict):
            if isinstance(user_info, str): # If it's just a string (email)
                user_info = {'email': user_info, 'name': user_info}
            else: # Skip if it's not a dict or string
                continue

        email = user_info.get('email')
        name = user_info.get('name') or user_info.get('displayName') or email
        location = user_info.get('location')
        display_name = user_info.get('displayName') or name
        phone_number = user_info.get('phoneNumber')

        if not email or pd.isna(email):
            continue

        if email not in user_email_to_id:
            user_email_to_id[email] = user_id_counter
            all_users.append({
                'user_id': user_id_to_id[email],
                'name': name,
                'email': email,
                'location': location,
                'displayName': display_name,
                'phoneNumber': phone_number
            })
            user_id_counter += 1
        
        user_id = user_email_to_id[email]

        relation = {
            'comm_id': comm_id,
            'user_id': user_id,
            'isAttendee': 0,
            'isParticipant': 0,
            'isSpeaker': 0,
            'isOrganiser': 0
        }
        # Set the appropriate role flag
        if role_type == 'attendees':
            relation['isAttendee'] = 1
        elif role_type == 'participants':
            relation['isParticipant'] = 1
        elif role_type == 'speakers':
            relation['isSpeaker'] = 1
        elif role_type == 'organizer': # This is a single organizer, not a list
            relation['isOrganiser'] = 1
        processed_users.append(relation)
    return processed_users

# Iterate through raw_df to populate users and relations
for index, row in raw_df.iterrows():
    comm_id = row['comm_id']

    # Process organizer
    if 'organizer_email' in row and pd.notna(row['organizer_email']):
        # Create a dict that simulates the structure of an attendee for consistency
        organizer_info = {
            'email': row['organizer_email'],
            'name': row.get('organizer_name', row['organizer_email']), # Use name if exists, else email
            'location': row.get('organizer_location'), # Assuming such columns might exist
            'displayName': row.get('organizer_display_name'),
            'phoneNumber': row.get('organizer_phone_number')
        }
        relations = process_user_data(comm_id, json.dumps([organizer_info]), 'organizer')
        comm_user_relations.extend(relations) # Extend with the list of relations

    # Process attendees, participants, speakers
    for col_name, role in [('attendees', 'attendees'), ('participants', 'participants'), ('speakers', 'speakers')]:
        if col_name in row and pd.notna(row[col_name]):
            relations = process_user_data(comm_id, row[col_name], role)
            comm_user_relations.extend(relations)

# Create dim_user DataFrame
dim_user = pd.DataFrame(all_users).drop_duplicates(subset=['user_id']).reset_index(drop=True)
# Ensure user_id is the primary key and sort for consistency
dim_user = dim_user.sort_values('user_id').set_index('user_id').reset_index()

print("dim_user:\n", dim_user.head())

# Create bridge_comm_user DataFrame
# Aggregate relations by (comm_id, user_id) to merge roles
bridge_comm_user = pd.DataFrame(comm_user_relations)
# Group by comm_id and user_id, then sum the boolean flags to combine roles
bridge_comm_user = bridge_comm_user.groupby(['comm_id', 'user_id']).agg({
    'isAttendee': 'max', # max will ensure 1 if any role is 1
    'isParticipant': 'max',
    'isSpeaker': 'max',
    'isOrganiser': 'max'
}).reset_index()

print("bridge_comm_user:\n", bridge_comm_user.head())


# --- dim_calendar ---
print("\nCreating dim_calendar...")
# Assuming 'start_time' is the primary date for calendar dimension
if 'start_time' in raw_df.columns:
    dim_calendar_dates = raw_df['start_time'].dt.normalize().dropna().drop_duplicates().reset_index(drop=True)
    dim_calendar = pd.DataFrame({
        'calendar_date': dim_calendar_dates,
        'year': dim_calendar_dates.dt.year,
        'month': dim_calendar_dates.dt.month,
        'day': dim_calendar_dates.dt.day,
        'day_of_week': dim_calendar_dates.dt.dayofweek,
        'day_name': dim_calendar_dates.dt.day_name(),
        'month_name': dim_calendar_dates.dt.month_name()
    })
    dim_calendar['calendar_id'] = range(1, len(dim_calendar) + 1) # Surrogate key
    # Create a mapping for fact table
    # Mapping date string 'YYYY-MM-DD' to calendar_id
    calendar_mapping = dim_calendar.set_index(dim_calendar['calendar_date'].dt.strftime('%Y-%m-%d'))['calendar_id'].to_dict()
else:
    print("Warning: 'start_time' column not found for dim_calendar. Creating dummy dim_calendar.")
    dim_calendar = pd.DataFrame({
        'calendar_id': [1, 2],
        'calendar_date': pd.to_datetime(['2023-01-01', '2023-01-02']),
        'year': [2023, 2023],
        'month': [1, 1],
        'day': [1, 2],
        'day_of_week': [6, 0], # Sunday, Monday
        'day_name': ['Sunday', 'Monday'],
        'month_name': ['January', 'January']
    })
    calendar_mapping = dim_calendar.set_index(dim_calendar['calendar_date'].dt.strftime('%Y-%m-%d'))['calendar_id'].to_dict()

print("dim_calendar:\n", dim_calendar.head())


# --- dim_audio ---
print("\nCreating dim_audio...")
if 'audio_url' in raw_df.columns:
    dim_audio = raw_df[['audio_url']].dropna().drop_duplicates().reset_index(drop=True)
    dim_audio['audio_id'] = range(1, len(dim_audio) + 1) # Surrogate key
    # Create a mapping for fact table
    audio_mapping = dim_audio.set_index('audio_url')['audio_id'].to_dict()
else:
    print("Warning: 'audio_url' column not found for dim_audio. Creating dummy dim_audio.")
    dim_audio = pd.DataFrame({
        'audio_id': [1, 2],
        'audio_url': ['http://dummy.com/audio1.mp3', 'http://dummy.com/audio2.mp3']
    })
    audio_mapping = dim_audio.set_index('audio_url')['audio_id'].to_dict()

print("dim_audio:\n", dim_audio.head())


# --- dim_video ---
print("\nCreating dim_video...")
if 'video_url' in raw_df.columns:
    dim_video = raw_df[['video_url']].dropna().drop_duplicates().reset_index(drop=True)
    dim_video['video_id'] = range(1, len(dim_video) + 1) # Surrogate key
    # Create a mapping for fact table
    video_mapping = dim_video.set_index('video_url')['video_id'].to_dict()
else:
    print("Warning: 'video_url' column not found for dim_video. Creating dummy dim_video.")
    dim_video = pd.DataFrame({
        'video_id': [1, 2],
        'video_url': ['http://dummy.com/video1.mp4', 'http://dummy.com/video2.mp4']
    })
    video_mapping = dim_video.set_index('video_url')['video_id'].to_dict()

print("dim_video:\n", dim_video.head())


# --- dim_transcript ---
print("\nCreating dim_transcript...")
if 'transcript_url' in raw_df.columns:
    dim_transcript = raw_df[['transcript_url']].dropna().drop_duplicates().reset_index(drop=True)
    dim_transcript['transcript_id'] = range(1, len(dim_transcript) + 1) # Surrogate key
    # Create a mapping for fact table
    transcript_mapping = dim_transcript.set_index('transcript_url')['transcript_id'].to_dict()
else:
    print("Warning: 'transcript_url' column not found for dim_transcript. Creating dummy dim_transcript.")
    dim_transcript = pd.DataFrame({
        'transcript_id': [1, 2],
        'transcript_url': ['http://dummy.com/trans1.txt', 'http://dummy.com/trans2.txt']
    })
    transcript_mapping = dim_transcript.set_index('transcript_url')['transcript_id'].to_dict()

print("dim_transcript:\n", dim_transcript.head())


# --- 4. Create Fact Table (`fact_communication`) ---
print("\nCreating fact_communication...")

fact_communication = raw_df[[
    'comm_id',
    'source_id', # Assuming source_id exists in raw_df as per dictionary
    'event_type', # For mapping to comm_type_id
    'event_title', # For mapping to subject_id
    'start_time', # For mapping to calendar_id and datetime_id
    'audio_url', # For mapping to audio_id
    'video_url', # For mapping to video_id
    'transcript_url', # For mapping to transcript_id
    'created_at', # For ingested_at
    'updated_at', # For processed_at
    'is_processed', # For is_processed
    'raw_title', # For raw_title (assuming 'event_title' is the raw title)
    'duration_seconds' # For raw_duration (assuming 'duration_seconds' is the raw duration)
]].copy()

# Rename columns to match fact_communication table
fact_communication.rename(columns={
    'event_title': 'raw_title', # event_title is also the raw_title
    'duration_seconds': 'raw_duration',
    'created_at': 'ingested_at',
    'updated_at': 'processed_at'
}, inplace=True)

# Map foreign keys
# Using .get() for mappings to handle cases where a value might not be in the mapping dict
fact_communication['comm_type_id'] = fact_communication['event_type'].map(comm_type_mapping).fillna(0).astype(int)
fact_communication['subject_id'] = fact_communication['raw_title'].map(subject_mapping).fillna(0).astype(int)
fact_communication['calendar_id'] = fact_communication['start_time'].dt.strftime('%Y-%m-%d').map(calendar_mapping).fillna(0).astype(int)

# For audio, video, transcript, fill NaNs with 0 (or a special ID if you have one for 'no audio')
fact_communication['audio_id'] = fact_communication['audio_url'].map(audio_mapping).fillna(0).astype(int)
fact_communication['video_id'] = fact_communication['video_url'].map(video_mapping).fillna(0).astype(int)
fact_communication['transcript_id'] = fact_communication['transcript_url'].map(transcript_mapping).fillna(0).astype(int)


# Handle datetime_id: Use start_time as the base. If you need a specific integer date key, generate it.
# For simplicity, using Unix timestamp or just the formatted date string for datetime_id
if 'start_time' in raw_df.columns:
    fact_communication['datetime_id'] = raw_df['start_time'].astype(np.int64) // 10**9 # Unix timestamp
else:
    fact_communication['datetime_id'] = 0 # Dummy value

# Ensure 'is_processed' is an integer (boolean to int)
if 'is_processed' in raw_df.columns:
    fact_communication['is_processed'] = raw_df['is_processed'].astype(int)
else:
    fact_communication['is_processed'] = 0 # Default to 0 if column not found

# Drop the temporary columns used for mapping
fact_communication.drop(columns=['event_type', 'audio_url', 'video_url', 'transcript_url', 'start_time'], inplace=True, errors='ignore')

# Reorder columns to match field_dictionary (optional, but good practice)
fact_comm_columns_order = [
    'comm_id', 'source_id', 'comm_type_id', 'subject_id', 'calendar_id',
    'audio_id', 'video_id', 'transcript_id', 'datetime_id',
    'ingested_at', 'processed_at', 'is_processed', 'raw_title', 'raw_duration'
]
# Filter to only existing columns and then reorder
fact_communication = fact_communication[[col for col in fact_comm_columns_order if col in fact_communication.columns]]

print("fact_communication:\n", fact_communication.head())


# --- 5. Export Tables to Excel ---
print(f"\nExporting dimension and fact tables to '{OUTPUT_EXCEL_FILE}'...")
with pd.ExcelWriter(OUTPUT_EXCEL_FILE, engine='xlsxwriter') as writer:
    dim_comm_type.to_excel(writer, sheet_name='dim_comm_type', index=False)
    dim_subject.to_excel(writer, sheet_name='dim_subject', index=False)
    dim_user.to_excel(writer, sheet_name='dim_user', index=False)
    dim_calendar.to_excel(writer, sheet_name='dim_calendar', index=False)
    dim_audio.to_excel(writer, sheet_name='dim_audio', index=False)
    dim_video.to_excel(writer, sheet_name='dim_video', index=False)
    dim_transcript.to_excel(writer, sheet_name='dim_transcript', index=False)
    fact_communication.to_excel(writer, sheet_name='fact_communication', index=False)
    bridge_comm_user.to_excel(writer, sheet_name='bridge_comm_user', index=False)

print("\nAll tables successfully exported to Excel!")

