from xml.dom import NotFoundErr
from google.oauth2.credentials import Credentials # type: ignore
from google_auth_oauthlib.flow import InstalledAppFlow # type: ignore
from google.auth.transport.requests import  Request # type: ignore
from googleapiclient.discovery import build # type: ignore
import os
import http.server
import socketserver
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import http.client
import json
import base64
import ssl
import sys
import time
import schedule # type: ignore
import paho.mqtt.client as mqtt # type: ignore
from paho.mqtt.client import MQTTMessage, CallbackAPIVersion # type: ignore
from ha_mqtt_discoverable import Settings, DeviceInfo # type: ignore
from ha_mqtt_discoverable.sensors import Sensor, SensorInfo, BinarySensor, BinarySensorInfo, Button, ButtonInfo, Switch, SwitchInfo, Select, SelectInfo, Text, TextInfo, Number, NumberInfo # type: ignore
import re
from bs4 import BeautifulSoup # type: ignore
import io
import requests # type: ignore
from PIL import Image # type: ignore
from googleapiclient.http import MediaIoBaseUpload # type: ignore




class NoStreamError(Exception):
    """Custom exception for when a stream is not found."""
    pass


TZ = ZoneInfo("America/Chicago")

partial = False
mqtt_creds = None
selected_stream_key = "1080p30HLS" 
stream_key_options = ["1080p30HLS", "1080p30RTMP", "AutoHLS"]
selected_privacy_status = "Private" 
privacy_status_options = ["Public", "Private", "Unlisted"]
master_enabled = True
creation_range = 5


if os.path.exists("creds.json"):
    with open('creds.json', 'r') as file:
        creds_data = json.load(file)
        mqtt_creds = creds_data.get('mqtt', {})
        streamKeys = creds_data.get('stream_keys', {})


buttonMQTT = Settings.MQTT(
    host=mqtt_creds.get('broker_ip'),
    port=int(mqtt_creds.get('port')),
    username=mqtt_creds.get('username'),
    password=mqtt_creds.get('password'),
)

device_info = DeviceInfo(
    name="Youtube Stream Creator",
    identifiers="youtube_stream_creator",
)

def debug_switch_callback(client, user_data, message: MQTTMessage):
    payload = message.payload.decode()
    if payload == "ON":
        print("Debug Mode Turned On!")
        debug_mode_switch.on()
    elif payload == "OFF":
        print("Debug Mode Turned Off!")
        debug_mode_switch.off()

debug_mode_switch = Switch(
    Settings(
        mqtt=buttonMQTT, 
        entity=SwitchInfo(
            name="Debug Mode",
            unique_id="debug_mode_switch",
            device=device_info,
        )
    ),
    debug_switch_callback,
)

def stream_key_selector_callback(client, user_data, message: MQTTMessage):
    global selected_stream_key
    payload = message.payload.decode()
    if payload in stream_key_options:
        selected_stream_key = payload
        print(f"Stream key set to: {selected_stream_key}")
        stream_key_selector.select_option(payload)
    else:
        print(f"Received invalid stream key option: {payload}")

stream_key_selector = Select(
    Settings(
        mqtt=buttonMQTT,
        entity=SelectInfo(
            name="Stream Key",
            unique_id="stream_key_selector",
            options=stream_key_options,
            device=device_info,
        )
    ),
    stream_key_selector_callback,
)

def privacy_status_selector_callback(client, user_data, message: MQTTMessage):
    global selected_privacy_status
    payload = message.payload.decode()
    if payload in privacy_status_options:
        selected_privacy_status = payload
        print(f"Privacy status set to: {selected_privacy_status}")
        privacy_status_selector.select_option(payload)
    else:
        print(f"Received invalid privacy status option: {payload}")

privacy_status_selector = Select(
    Settings(
        mqtt=buttonMQTT,
        entity=SelectInfo(
            name="Privacy Status",
            unique_id="privacy_status_selector",
            options=privacy_status_options,
            device=device_info,
        )
    ),
    privacy_status_selector_callback,
)

def master_enable_callback(client, user_data, message: MQTTMessage):
    global master_enabled
    payload = message.payload.decode()
    if payload == "ON":
        master_enabled = True
        print("Master Enable: ON")
        master_enable_switch.on()
    elif payload == "OFF":
        master_enabled = False
        print("Master Enable: OFF")
        master_enable_switch.off()

master_enable_switch = Switch(
    Settings(
        mqtt=buttonMQTT,
        entity=SwitchInfo(
            name="Master Enable",
            unique_id="master_enable_switch",
            device=device_info,
        )
    ),    master_enable_callback,
)

def creation_range_callback(client, user_data, message: MQTTMessage):
    global creation_range
    payload = message.payload.decode()
    try:
        new_range = int(float(payload))
        if 1 <= new_range <= 7:  # Reasonable limits
            creation_range = new_range
            print(f"Creation range set to: {creation_range} days")
            creation_range_input.set_value(new_range)
        else:
            print(f"Invalid creation range: {new_range}. Must be between 1 and 7 days.")
    except ValueError:
        print(f"Invalid creation range format: {payload}")

creation_range_input = Number(
    Settings(
        mqtt=buttonMQTT,
        entity=NumberInfo(
            name="Creation Range (Days)",
            unique_id="creation_range_input",
            icon="mdi:calendar-range",
            min_value=1,
            max_value=7,
            step=1,
            mode="box",
            device=device_info,
        )
    ),
    creation_range_callback,
)

def sync_services_callback(client, user_data, message: MQTTMessage):
    if not master_enabled:
        print("Master switch is disabled. Cannot sync services.")
        return
    
    print(f"Syncing services for the next {creation_range} days...")
    
    # Loop through each day in the range
    for day_offset in range(creation_range + 1):  # +1 to include today
        scanningDate = datetime.now(TZ) + timedelta(days=day_offset)
        date_string = datetime_to_string(scanningDate)
        print(f"Checking for services on {date_string}")
        
        try: 
            plan_id = get_plan_id_by_date(service_type_id, date_string, application_id, secret, True)
            service_start = get_time_by_plan(service_type_id, plan_id, application_id, secret)
            stream_start = service_start - timedelta(minutes=10)  

            # Check if the stream start time has already passed
            if stream_start < datetime.now(TZ):
                print(f"Stream time for {date_string} has already passed ({stream_start.strftime('%I:%M%p')}), skipping.")
                continue    

            service_time_str = service_start.strftime("%I:%M%p").lower()
            stream_time_str = stream_start.strftime("%I:%M%p").lower()

            serviceDate = datetime_to_string(service_start)

            name = get_name_by_plan(service_type_id, plan_id, application_id, secret)

            title = f"Libertyville Covenant Church {name} {serviceDate} - {service_time_str}"
            description = generateDescription(service_type_id, plan_id, application_id, secret, stream_time_str, service_time_str, serviceDate)

            thumbnail = thumbnail_from_url(get_image_by_plan(service_type_id, plan_id, application_id, secret))

            youtube = authenticate_to_youtube()
            

            try:
                if description != get_scheduled_stream_description(title, youtube):
                    print("Copyright has changed, updating YouTube stream.")
                    update_scheduled_stream_description(youtube, title, description, service_start)
                else:
                    print("Copyright has not changed, no update needed.")
                    text_sensors["last_check"].set_state(datetime.now(TZ).strftime("%Y-%m-%d %I:%M %p"))
            except NoStreamError:
                print(f"No existing stream found for {serviceDate}, creating new one.")
                schedule_youtube_live_stream(
                    youtube,
                    title,
                    description,
                    service_start, 
                    streamKeys.get(selected_stream_key), 
                    "PL9xSMDIz2M74Joy1_eLihy_7jYVhCvLkV", #playlist id 
                    29, 
                    thumbnail, 
                    selected_privacy_status.lower()
                )
        except NotFoundErr:
            print(f"No service found for {date_string}")
            continue  # Continue to next day
    
    print("Service sync complete.")
        

sync_services_button = Button(
    Settings(
        mqtt=buttonMQTT,
        entity=ButtonInfo(
            name="Sync Services",
            unique_id="sync_services_button",
            device=device_info,
        )
    ),
    sync_services_callback,
)

MQTTCLIENT = mqtt.Client(
    callback_api_version=CallbackAPIVersion.VERSION2,
    protocol=mqtt.MQTTv311)
MQTTCLIENT.username_pw_set(mqtt_creds.get('username'), mqtt_creds.get('password'))
MQTTCLIENT.connect(mqtt_creds.get('broker_ip'), int(mqtt_creds.get('port')), 60)
MQTTCLIENT.enable_logger()
MQTTCLIENT.reconnect_delay_set(min_delay=1, max_delay=60)
mqtt_settings = Settings.MQTT(client=MQTTCLIENT)

device_info = {"name": "Youtube Stream Creator", "identifiers": "youtube_stream_creator"}


debug_mode_switch.write_config()
stream_key_selector.write_config()
privacy_status_selector.write_config()
master_enable_switch.write_config()
creation_range_input.write_config()
sync_services_button.write_config()
debug_mode_switch.off()
master_enable_switch.on()
stream_key_selector.select_option(selected_stream_key)
privacy_status_selector.select_option(selected_privacy_status)
creation_range_input.set_value(creation_range)


binary_sensors = {
    "Youtube Stream Creator": BinarySensor(
        Settings(
            mqtt=mqtt_settings,
            entity=BinarySensorInfo(
                name="Youtube Stream Creator",
                device_class="connectivity",  
                unique_id="youtube_stream_creator_online",
                device=device_info,
            ),
        )
    ),
}

text_sensors = {
    "last_service_updated": Sensor(
        Settings(
            mqtt=mqtt_settings,
            entity=SensorInfo(
                name="Last Service Updated",
                unique_id="last_service_updated",
                device_class=None,
                unit_of_measurement=None,
                device=device_info,
            ),
        )
    ),
    "last_update_time": Sensor(
        Settings(
            mqtt=mqtt_settings,
            entity=SensorInfo(
                name="Last Update Time",
                unique_id="last_update_time",
                device_class=None,
                unit_of_measurement=None,
                icon="mdi:calendar-clock",
                device=device_info,
            ),
        )
    ),
    "last_check": Sensor(
        Settings(
            mqtt=mqtt_settings,
            entity=SensorInfo(
                name="Last Check",
                unique_id="last_check",
                device_class=None,
                unit_of_measurement=None,
                icon="mdi:calendar-clock",
                device=device_info,
            ),
        )
    ),
}

for sensor in binary_sensors.values():
    sensor.write_config()
for sensor in text_sensors.values():
    sensor.write_config()

# Finished Setting up MQTT, move to main

binary_sensors["Youtube Stream Creator"].on()
lastFound = 575
response = None
data = None
def html_to_string(html):
    if html is None:
        return ""
    soup = BeautifulSoup(html, 'html.parser')
    return soup.get_text().strip().replace("#", "")

def queryPCO(service_type_id,offset,increment,application_id,secret):
    global response, data
    HOST = 'api.planningcenteronline.com'
    URL = f'/services/v2/service_types/{service_type_id}/plans?offset={offset}&per_page={increment}&order=sort_date'
    conn = http.client.HTTPSConnection(HOST, context=ssl._create_unverified_context())
    auth = base64.b64encode(f'{application_id}:{secret}'.encode()).decode()
    headers = {
        'Authorization': f'Basic {auth}',
        'Content-Type': 'application/json'
    }
    #print(f"Querying PCO API")
    conn.request('GET', URL, headers=headers)
    response = conn.getresponse()
    data = response.read().decode()
    conn.close()

def get_plan_id_by_date(service_type_id, date, application_id, secret, findingService):
    global lastFound, response, data
    mathNumber = 1000
    increment = 25
    offset = lastFound
    findingService = False
    queryPCO(service_type_id,offset,increment,application_id,secret)

    while 3==1+2: #crazy math i know
        if response.status == 200:
            plans = json.loads(data)
            for plan in plans['data']:
                plan_date = plan['attributes']['dates']
                if plan_date == date:
                    print(f"Found Service on {date}")
                    lastFound = offset
                    if findingService:
                        print(f"Found ballpark, scanning narrowly")
                        data = "PewPewPew"
                        get_plan_id_by_date(service_type_id, date, application_id, secret, True)
                    return plan['id']
            offset+=increment
            #print(f"{date} not found, offset increasing to {offset}")
            queryPCO(service_type_id,offset,increment,application_id,secret)
        else:
            print(f"Error: Failed to retrieve plans: {response.status}")
            return None

        if offset > lastFound + mathNumber:
            queryPCO(service_type_id,lastFound,increment,application_id,secret)
            raise NotFoundErr(f"No plan found for {date}")

def get_copyright_by_plan(service_type_id, plan_id, application_id, secret):
    URL = f'/services/v2/service_types/{service_type_id}/plans/{plan_id}/items'
    HOST = 'api.planningcenteronline.com'
    conn = http.client.HTTPSConnection(HOST, context=ssl._create_unverified_context())
    auth = base64.b64encode(f'{application_id}:{secret}'.encode()).decode()

    headers = {
        'Authorization': f'Basic {auth}',
        'Content-Type': 'application/json'
    }

    conn.request('GET', URL, headers=headers)
    response = conn.getresponse()
    data = response.read().decode()
    if response.status == 200:
        items = json.loads(data)
        # Filter items by title and get the id
        for item in items['data']:
            if item['attributes']['title'] == 'Copyright Page' or item['attributes']['title'] == 'copyright page' or item['attributes']['title'] == 'Copyright page':
                copyright = item['attributes']['html_details']
                print(f"Found item Copyright Page")
                break
        else:
            conn.close()
            raise NotFoundErr(f"Item 'Copyright Page' not found")
    else:
        print(f"Error: Failed to retrieve data: {response.status}")
        conn.close()
        sys.exit(1)
    conn.close()
    return html_to_string(copyright)


def get_name_by_plan(service_type_id, plan_id, application_id, secret):
    URL = f'/services/v2/service_types/{service_type_id}/plans/{plan_id}/items'
    HOST = 'api.planningcenteronline.com'
    conn = http.client.HTTPSConnection(HOST, context=ssl._create_unverified_context())
    auth = base64.b64encode(f'{application_id}:{secret}'.encode()).decode()

    headers = {
        'Authorization': f'Basic {auth}',
        'Content-Type': 'application/json'
    }

    conn.request('GET', URL, headers=headers)
    response = conn.getresponse()
    data = response.read().decode()
    if response.status == 200:
        items = json.loads(data)
        # Filter items by title and get the id
        for item in items['data']:
            if item['attributes']['title'] == 'Special Service Name' or item['attributes']['title'] == 'Service Name' or item['attributes']['title'] == 'Special Name':
                special_name = item['attributes']['description']
                print(f"Found item Special Service Name")
                break
        else:
            conn.close()
            return "Worship Service"
    else:
        print(f"Error: Failed to retrieve data: {response.status}")
        conn.close()
        sys.exit(1)
    conn.close()
    return special_name




def get_image_by_plan(service_type_id, plan_id, application_id, secret):
    URL = f'/services/v2/service_types/{service_type_id}/plans/{plan_id}?include=series'
    HOST = 'api.planningcenteronline.com'
    conn = http.client.HTTPSConnection(HOST, context=ssl._create_unverified_context())
    auth = base64.b64encode(f'{application_id}:{secret}'.encode()).decode()

    headers = {
        'Authorization': f'Basic {auth}',
        'Content-Type': 'application/json'
    }

    conn.request('GET', URL, headers=headers)
    response = conn.getresponse()
    data = response.read().decode()
    item = json.loads(data)
    return item['included'][0]['attributes']['artwork_original']


def get_time_by_plan(service_type_id, plan_id, application_id, secret):
    URL = f'/services/v2/service_types/{service_type_id}/plans/{plan_id}?include=plan_times'
    HOST = 'api.planningcenteronline.com'
    conn = http.client.HTTPSConnection(HOST, context=ssl._create_unverified_context())
    auth = base64.b64encode(f'{application_id}:{secret}'.encode()).decode()

    headers = {
        'Authorization': f'Basic {auth}',
        'Content-Type': 'application/json'
    }

    conn.request('GET', URL, headers=headers)
    response = conn.getresponse()
    data = response.read().decode()
    item = json.loads(data)
    starts_at = [
    p['attributes']['starts_at']
    for p in item['included']
    if p['attributes']['time_type'] == 'service'
    ][0]
    starts_at = datetime.fromisoformat(starts_at.replace("Z", "+00:00")).astimezone(TZ)
    return starts_at


def thumbnail_from_url(url):
    resp = requests.get(url)
    resp.raise_for_status()

    img = Image.open(io.BytesIO(resp.content))
    if img.mode in ("RGBA", "P"):
        img = img.convert("RGB")

    buf = io.BytesIO()
    quality = 85
    while True:
        buf.seek(0); buf.truncate()
        img.save(buf, format="JPEG", quality=quality)
        if buf.tell() <= 2*1024*1024 or quality <= 25:
            break
        quality -= 10

    buf.seek(0)
    return MediaIoBaseUpload(buf, mimetype="image/jpeg", resumable=False)


def datetime_to_string(dt):
    return dt.strftime("%B %d, %Y").replace(" 0", " ").lstrip("0")

def rightNow():
    return datetime.now(TZ).strftime('%Y-%m-%d %I:%M %p')


def authenticate_to_youtube():
    SCOPES = ["https://www.googleapis.com/auth/youtube.force-ssl"]
    token_path = "token.json"
    creds = None

    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # Use the HOST_IP from environment variables, or default to 'localhost'.
            host_ip = os.environ.get('HOST_IP', 'localhost')
            redirect_uri = f'http://{host_ip}:500'
            print(f"Using redirect URI: {redirect_uri}")
            
            flow = InstalledAppFlow.from_client_config(creds_data, SCOPES)
            flow.redirect_uri = redirect_uri
            auth_url, _ = flow.authorization_url(prompt='consent')

            print("--- MANUAL AUTHENTICATION REQUIRED ---")
            print("Please visit this URL on your host machine to authorize:")
            print(auth_url)
            print("Waiting for authorization...")

            # More robust server to prevent "Connection reset" errors
            class AuthHandler(http.server.BaseHTTPRequestHandler):
                def do_GET(self):
                    query_components = parse_qs(urlparse(self.path).query)
                    if 'code' in query_components:
                        self.server.auth_code = query_components["code"][0]
                    
                    self.send_response(200)
                    self.send_header('Content-type', 'text/html')
                    self.end_headers()
                    self.wfile.write(b"<html><body><h1>Authentication successful!</h1><p>You can close this browser tab.</p></body></html>")
                    self.server.shutdown_requested = True

            server_address = ("0.0.0.0", 500)
            httpd = socketserver.TCPServer(server_address, AuthHandler)
            httpd.shutdown_requested = False
            httpd.auth_code = None

            while not httpd.shutdown_requested:
                httpd.handle_request()
            
            auth_code = httpd.auth_code
            if not auth_code:
                raise Exception("Failed to retrieve authorization code from redirect.")

            print("Authorization code received. Fetching token...")
            flow.fetch_token(code=auth_code)
            creds = flow.credentials
        
        with open(token_path, "w") as token_file:
            token_file.write(creds.to_json())
        print("Token file created successfully.")

    return build('youtube', 'v3', credentials=creds)

def schedule_youtube_live_stream(
    youtube,
    title,
    description,
    start_time,
    stream_key,
    playlist_id,
    video_category_id,
    thumbnail_media_upload,
    privacy_status = "private"
):
    print("--- Starting YouTube Live Stream Scheduling ---")

    print(f"1. Searching for stream with key: '{stream_key}'...")
    stream_list_response = youtube.liveStreams().list(
        part="id,snippet,cdn",
        mine=True
    ).execute()

    target_stream_id = None
    for stream in stream_list_response.get("items", []):
        if stream["cdn"]["ingestionInfo"]["streamName"] == stream_key:
            target_stream_id = stream["id"]
            print(f"   -> Found matching stream with ID: {target_stream_id}")
            break

    if not target_stream_id:
        raise ValueError(f"Could not find a live stream with the key '{stream_key}'. Please check the key in your YouTube Studio settings.")

    print(f"2. Creating broadcast titled: '{title}'...")
    broadcast_request_body = {
        "snippet": {
            "title": title,
            "description": description,
            "scheduledStartTime": start_time.isoformat(),
        },
        "status": {
            "privacyStatus": privacy_status,
            "selfDeclaredMadeForKids": False,
        },
        "contentDetails": {
            "enableAutoStart": True,
            "enableAutoStop": True,
            "enableLiveChat": True
        }
    }
    broadcast_response = youtube.liveBroadcasts().insert(
        part="snippet,status,contentDetails",
        body=broadcast_request_body
    ).execute()

    broadcast_id = broadcast_response['id']
    print(f"   -> Broadcast created with ID: {broadcast_id}")

    print(f"3. Binding broadcast '{broadcast_id}' to stream '{target_stream_id}'...")
    youtube.liveBroadcasts().bind(
        part="id,contentDetails",
        id=broadcast_id,
        streamId=target_stream_id
    ).execute()
    print("   -> Successfully bound.")

    print(f"4. Uploading thumbnail...")
    youtube.thumbnails().set(
        videoId=broadcast_id,
        media_body=thumbnail_media_upload
    ).execute()
    print("   -> Thumbnail set successfully.")

    print(f"5. Setting video category to ID: {video_category_id}...")
    video_update_body = {
        "id": broadcast_id,
        "snippet": {
            "categoryId": video_category_id,

            "title": title,
            "description": description,
        }
    }
    youtube.videos().update(
        part="snippet",
        body=video_update_body
    ).execute()
    print("   -> Category updated.")

    print(f"6. Adding video to playlist ID: {playlist_id}...")
    playlist_item_body = {
        "snippet": {
            "playlistId": playlist_id,
            "resourceId": {
                "kind": "youtube#video",
                "videoId": broadcast_id
            }
        }
    }
    youtube.playlistItems().insert(
        part="snippet",
        body=playlist_item_body    ).execute()
    print("   -> Successfully added to playlist.")
    
    print("--- Scheduling Complete! ---")

    text_sensors["last_update_time"].set_state(datetime.now(TZ).strftime("%Y-%m-%d %I:%M %p"))
    text_sensors["last_service_updated"].set_state(datetime_to_string(start_time.date()))
    return broadcast_response

planning_center_creds = creds_data.get('planning_center', {})
application_id = planning_center_creds.get('application_id')
secret = planning_center_creds.get('secret')
service_type_id = planning_center_creds.get('service_type_id')

if not all([application_id, secret, service_type_id]):
    print("Planning Center credentials are missing in the JSON file.")
    exit(1)
    
    





def generateDescription(service_type_id, plan_id, application_id, secret, stream_time_str, service_time_str, day):
    description =  f"This is the Libertyville Covenant Church Worship Service for {day}."
    description += f"The stream will begin at approximately {stream_time_str} CDT and the service will begin at {service_time_str} CDT."
    description += f"\nThe bulletin can be found here: https://libcov.org/bulletin\n\n"
    description += get_copyright_by_plan(service_type_id, plan_id, application_id, secret)
    description += "\n\n#LibertyvilleCovenantChurch #churchathome #churchonline #churchservice"
    return description


def get_scheduled_stream_description(title, youtube):
    try:
        request = youtube.liveBroadcasts().list(
            part="snippet",
            broadcastStatus="upcoming",
            maxResults=10
        )
        response = request.execute()

        for item in response.get("items", []):
            if item["snippet"]["title"] == title:
                return item["snippet"]["description"]

        raise NoStreamError(f"No stream found with title: '{title}'")

    except Exception as e:
        print(f"{e}")
        return f"Error fetching from YouTube: {e}"

def update_scheduled_stream_description(youtube, title, new_description, start_time):
    try:
        # Search for the upcoming broadcast by title
        request = youtube.liveBroadcasts().list(
            part="snippet,id",
            broadcastStatus="upcoming",
            maxResults=10
        )
        response = request.execute()

        broadcast_id = None
        for item in response.get("items", []):
            if item["snippet"]["title"] == title:
                broadcast_id = item["id"]
                break

        if not broadcast_id:
            raise NoStreamError(f"No upcoming stream found with title: '{title}'")        # Update the broadcast description
        update_body = {
            "id": broadcast_id,
            "snippet": {
                "title": title,  # Keep the same title
                "description": new_description,
                "scheduledStartTime": item["snippet"]["scheduledStartTime"]  # Preserve start time
            }
        }
        
        youtube.liveBroadcasts().update(
            part="snippet",
            body=update_body
        ).execute()
        
        print(f"Successfully updated description for stream: '{title}'")
        text_sensors["last_update_time"].set_state(datetime.now(TZ).strftime("%Y-%m-%d %I:%M %p"))
        text_sensors["last_service_updated"].set_state(datetime_to_string(start_time.date()))
        return True
        
    except NoStreamError:
        raise  # Re-raise the custom exception
    except Exception as e:
        print(f"An error occurred while updating stream description: {e}")
        return False


MQTTCLIENT.loop_start()
authenticate_to_youtube()
print("PCO Youtube Started -- Waiting for commands")
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("Shutting Down")
    binary_sensors["Youtube Stream Creator"].off()
    MQTTCLIENT.loop_stop()
    MQTTCLIENT.disconnect()
